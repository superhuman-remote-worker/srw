"""Batch tool-call approval — announce step.

knowledge-base/knowledge/superpowers/specs/2026-08-01-batch-tool-approval-design.md
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import agent.api.persistent_app as pa
from agent.persistent_graph import PermissionOutcome


PINNED_AGENT_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"


@pytest.fixture(autouse=True)
def _exact_pinned_permission_owner(monkeypatch):
    """Permission sweeps in this unit module run as one exact pinned owner."""

    # These legacy unit sessions have no advertised generation/attach contract.
    # A prior file's modern runtime must not supply authority to their fakes.
    monkeypatch.setattr(pa._session_identity, "_runtime_contract", False)
    monkeypatch.setattr(pa._session_identity, "_status_contract", False)
    monkeypatch.setattr(pa._session_identity, "_session_generation", None)
    monkeypatch.setattr(pa._session_identity, "_attach_token", None)
    monkeypatch.setattr(
        pa,
        "_permission_retirement_authority",
        lambda: ("pinned", PINNED_AGENT_ID),
    )


@pytest.fixture(autouse=True)
def _isolate_announced_rows():
    """The announce step writes into a module-global ledger. Tests that don't
    patch it would otherwise hand their rows to the next file on this xdist
    worker, where `_terminate_session_inner` tries to retire gates that belong
    to a session that never existed."""

    saved = dict(pa._announced_permission_rows)
    pa._announced_permission_rows.clear()
    yield
    pa._announced_permission_rows.clear()
    pa._announced_permission_rows.update(saved)


def _mock_session(permission_mode: str = "supervised"):
    session = MagicMock()
    session.permission_mode = permission_mode
    session.tool_decisions = {}
    session.quiesce_subagents = AsyncMock()
    session.resume_subagents = AsyncMock()
    # None (not an unconfigured MagicMock): a bare MagicMock auto-chains
    # through `async with conn.acquire() as c: await c.fetchval(...)` and
    # returns a truthy AsyncMock rather than raising or returning None,
    # which would make any real DB-backed check silently misread "no row
    # configured" as "found a row". Tests that need a real DB round trip
    # wire postgres_conn explicitly (see _conn_with below).
    session.postgres_conn = None
    return session


CALLS = [
    {"name": "web_search", "args": {"query": "france"}, "id": "tc_0"},
    {"name": "web_search", "args": {"query": "japan"}, "id": "tc_1"},
]


class TestAnnounceBatch:
    @pytest.mark.asyncio
    async def test_inserts_a_row_per_call_and_broadcasts_once(self):
        inserts = []

        async def _insert(tool_call_id, tool_name, tool_args):
            inserts.append(tool_call_id)
            return f"rid-{tool_call_id}"

        bcast = MagicMock()
        with (
            patch.object(pa, "_session", _mock_session()),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_insert_permission_request", _insert),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(CALLS)

        assert inserts == ["tc_0", "tc_1"]
        assert bcast.call_count == 1
        event, payload = bcast.call_args.args
        assert event == "permission.request_batch"
        assert [r["id"] for r in payload["requests"]] == ["tc_0", "tc_1"]
        assert payload["requests"][0]["approval_id"] == "rid-tc_0"
        assert payload["requests"][0]["tool"] == "web_search"
        assert payload["requests"][0]["args"] == {"query": "france"}

    @pytest.mark.asyncio
    async def test_autonomous_mode_announces_nothing(self):
        bcast = MagicMock()
        insert = AsyncMock()
        with (
            patch.object(pa, "_session", _mock_session("autonomous")),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_insert_permission_request", insert),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(CALLS)

        insert.assert_not_awaited()
        bcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_accept_announces_only_shell_calls(self):
        """Under auto_accept only shell tools are gated — the card must not
        imply the auto-approved calls need a decision."""
        mixed = CALLS + [{"name": "run_command", "args": {"cmd": "ls"}, "id": "tc_2"}]
        inserts = []

        async def _insert(tool_call_id, tool_name, tool_args):
            inserts.append(tool_call_id)
            return f"rid-{tool_call_id}"

        bcast = MagicMock()
        with (
            patch.object(pa, "_session", _mock_session("auto_accept")),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_insert_permission_request", _insert),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(mixed)

        assert inserts == ["tc_2"]
        assert [r["id"] for r in bcast.call_args.args[1]["requests"]] == ["tc_2"]

    @pytest.mark.asyncio
    async def test_insert_failure_is_soft_and_skips_that_entry(self):
        async def _insert(tool_call_id, tool_name, tool_args):
            return None if tool_call_id == "tc_0" else "rid-tc_1"

        bcast = MagicMock()
        with (
            patch.object(pa, "_session", _mock_session()),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_insert_permission_request", _insert),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(CALLS)

        # tc_0 has no durable row, so the per-call gate path will handle it.
        assert [r["id"] for r in bcast.call_args.args[1]["requests"]] == ["tc_1"]

    @pytest.mark.asyncio
    async def test_no_session_is_a_noop(self):
        bcast = MagicMock()
        with (
            patch.object(pa, "_session", None),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(CALLS)
        bcast.assert_not_called()


def _conn_with(fetchrow_result):
    conn = AsyncMock()
    conn.fetchrow = AsyncMock(return_value=fetchrow_result)
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    session = _mock_session()
    session.postgres_conn = MagicMock()
    session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)
    return session, conn


class TestClaimsAnnouncedRow:
    @pytest.mark.asyncio
    async def test_claims_pending_row_without_inserting_again(self):
        """No unique constraint on (thread_id, tool_call_id): inserting again
        would orphan a card nobody waits on."""
        session, _ = _conn_with({"id": "rid-announced", "status": "pending"})
        insert = AsyncMock()
        waited = {}

        async def _wait(request_id, *a, **kw):
            waited["id"] = request_id
            return "approved"

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_insert_permission_request", insert),
            patch.object(pa, "_wait_for_permission_resolution", _wait),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_0")

        insert.assert_not_awaited()
        assert waited["id"] == "rid-announced"
        assert outcome is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_claimed_row_does_not_rebroadcast_request(self):
        """The batch frame already told the client — a second
        permission.request would duplicate the entry."""
        session, _ = _conn_with({"id": "rid-announced", "status": "pending"})
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_insert_permission_request", AsyncMock()),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="approved"),
            ),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_permission_check("web_search", {}, "tc_0")

        events = [c.args[0] for c in bcast.call_args_list]
        assert "permission.request" not in events

    @pytest.mark.asyncio
    async def test_no_announced_row_still_inserts_and_broadcasts(self):
        """Single-gate turns keep today's behaviour exactly."""
        session, _ = _conn_with(None)
        insert = AsyncMock(return_value="rid-fresh")
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_insert_permission_request", insert),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="approved"),
            ),
            patch.object(pa, "_broadcast", bcast),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_9")

        insert.assert_awaited_once()
        assert "permission.request" in [c.args[0] for c in bcast.call_args_list]
        assert outcome is PermissionOutcome.APPROVED


class TestSharedShellToolSet:
    """``_gate_needed`` and ``_loop_permission_check`` must consult the SAME
    shell-tool set. Two independently-maintained copies can silently drift:
    a tool added to only one leaves a gated call with no announced row — a
    permanently stuck approval card.

    Patches ``_SHELL_TOOLS`` itself (rather than asserting against its
    current real contents) so the test exercises the actual wiring — that
    both call sites read the one shared constant — instead of a coincidence
    that would hold even if _loop_permission_check still carried its own
    hardcoded copy with today's same three names.
    """

    @pytest.mark.asyncio
    async def test_permission_check_and_gate_needed_agree_for_every_shell_tool(self):
        fake_shell_tools = {"synthetic_shell_tool_for_drift_test"}
        with patch.object(pa, "_SHELL_TOOLS", fake_shell_tools):
            for tool_name in pa._SHELL_TOOLS:
                assert pa._gate_needed("auto_accept", tool_name) is True

                session = _mock_session("auto_accept")
                session.postgres_conn = None  # deterministic DECLINE if gated
                with patch.object(pa, "_session", session):
                    outcome = await pa._loop_permission_check(tool_name, {}, "tc")

                # A shell tool must never be auto-approved here — it should
                # fall through to the gate (and only DECLINE because
                # postgres_conn is None in this test, not because it was
                # waved through as a non-shell tool).
                assert outcome is not PermissionOutcome.APPROVED, (
                    f"{tool_name!r} was auto-approved though _gate_needed "
                    "says it requires a gate — the two paths disagree"
                )


class TestAnnounceSkipsTerminalRows:
    """Phase 5 wake replay: LangGraph restores a tool_call_id whose gate was
    already resolved out-of-band (e.g. a magic-link click) while the agent
    was suspended — that row is 'approved'/'denied' with decided_at set.
    Announcing unconditionally would INSERT a fresh 'pending' row for the
    same tool_call_id; _loop_permission_check's terminal-row short-circuit
    then returns immediately without ever claiming, waiting on, or expiring
    that new row. Nothing else reaps it (there is no expires_at sweeper —
    only an active waiter CAS-expires on timeout), so it re-renders as a
    live approval card on every reattach forever: exactly the orphaned,
    unresolvable card this task exists to prevent.
    """

    @pytest.mark.asyncio
    async def test_skips_tool_call_with_existing_terminal_decision(self):
        calls = [
            {"name": "web_search", "args": {"query": "a"}, "id": "tc_terminal"},
            {"name": "web_search", "args": {"query": "b"}, "id": "tc_fresh"},
        ]

        async def _fetchval(sql, *args):
            # Existence check is WHERE thread_id = $1 AND tool_call_id = $2.
            tool_call_id = args[1]
            return 1 if tool_call_id == "tc_terminal" else None

        conn = AsyncMock()
        conn.fetchval = AsyncMock(side_effect=_fetchval)
        acquire_ctx = MagicMock()
        acquire_ctx.__aenter__ = AsyncMock(return_value=conn)
        acquire_ctx.__aexit__ = AsyncMock(return_value=False)

        session = _mock_session()
        session.postgres_conn = MagicMock()
        session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)

        insert = AsyncMock(return_value="rid-fresh")
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_insert_permission_request", insert),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(calls)

        # No fresh row for the already-decided call — inserting one would
        # orphan a card nobody claims or resolves.
        insert.assert_awaited_once_with("tc_fresh", "web_search", {"query": "b"})
        ids = [r["id"] for r in bcast.call_args.args[1]["requests"]]
        assert "tc_terminal" not in ids
        assert ids == ["tc_fresh"]


class TestCallbackWiring:
    @pytest.mark.asyncio
    async def test_loop_callbacks_carry_the_announce_hook(self):
        """Without this wiring the batch card never appears in production.

        Captures the real PersistentLoopCallbacks the session builds, rather
        than asserting on source text.
        """
        captured = {}

        def _fake_run(**kwargs):
            captured["callbacks"] = kwargs.get("callbacks")

            async def _noop():
                return None

            return _noop()

        with (
            patch.object(pa, "_session", _mock_session()),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_loop_task", None),
            patch.object(pa, "_session_ready", lambda: True),
            patch.object(pa, "run_persistent_loop", _fake_run),
        ):
            pa._ensure_persistent_loop_started("test")

        cb = captured["callbacks"]
        assert cb is not None, "loop was never started"
        assert cb.announce_permission_batch is pa._loop_announce_permission_batch
        assert cb.before_turn_authorization is pa._loop_before_turn_authorization


# =============================================================================
# Announced rows must not outlive the turn that announced them
#
# An announced row is only ever retired by its OWN gate in
# _loop_permission_check, and that gate runs at most once per call. Any turn
# exit before call i — parked on NO_ANSWER, interrupted, errored, or a
# mid-batch mode downgrade — strands rows i..N as 'pending' forever: there is
# no expires_at sweeper, only an active waiter CAS-expires a row. A stranded
# row rides `session.state` on every reattach and re-renders as a live
# approval card; "Approve all" then flips it to 'approved' with NO waiter
# listening, so nothing executes while the user believes they approved the
# tools. That silent divergence is the reason this is Critical.
# =============================================================================


FOUR_CALLS = [
    {"name": "web_search", "args": {"query": "a"}, "id": "tc_0"},
    {"name": "web_search", "args": {"query": "b"}, "id": "tc_1"},
    {"name": "web_search", "args": {"query": "c"}, "id": "tc_2"},
    {"name": "web_search", "args": {"query": "d"}, "id": "tc_3"},
]


class _FakeRowStore:
    """Just enough of ``thread_permission_requests`` to exercise the CAS
    sweep: an id -> status map plus the three statements the sweep path
    touches (INSERT, the CAS expire, and the status re-read)."""

    def __init__(self) -> None:
        self.status: dict[str, str] = {}

    async def fetchval(self, sql: str, *args):
        if "INSERT INTO thread_permission_requests" in sql:
            request_id = f"rid-{args[1]}"  # $2 is tool_call_id
            self.status[request_id] = "pending"
            return request_id
        if "SET status = 'expired'" in sql:
            # CAS: only a row that is still pending may be expired.
            request_id = args[0]
            if self.status.get(request_id) == "pending":
                self.status[request_id] = "expired"
                return request_id
            return None
        if "SELECT 1 FROM thread_permission_requests" in sql:
            return None  # no terminal decision from a prior wake
        if "SELECT status FROM thread_permission_requests" in sql:
            return self.status.get(args[0])
        return None

    async def fetchrow(self, sql: str, *args):
        if "SELECT id, status FROM thread_permission_requests" in sql:
            # The claim SELECT: $2 is tool_call_id.
            request_id = f"rid-{args[1]}"
            status = self.status.get(request_id)
            if status in ("approved", "denied", "pending"):
                return {"id": request_id, "status": status}
            return None
        return None


def _session_with_store(store: _FakeRowStore, mode: str = "supervised"):
    acquire_ctx = MagicMock()
    acquire_ctx.__aenter__ = AsyncMock(return_value=store)
    acquire_ctx.__aexit__ = AsyncMock(return_value=False)
    session = _mock_session(mode)
    session.workspace_sync = None
    session.messages = []
    session.postgres_conn = MagicMock()
    session.postgres_conn.acquire = MagicMock(return_value=acquire_ctx)
    return session


async def _end_the_turn(turn_id: int = 7):
    """Drive the real turn-end callback with everything unrelated stubbed."""
    with (
        patch.object(pa, "_wire_session_aux_archiver", MagicMock()),
        patch.object(pa, "_save_turn_ai_messages", AsyncMock()),
        patch.object(pa, "_auto_title_after_first_turn", AsyncMock()),
        patch.object(pa, "_should_notify_cloud_stage", lambda: False),
    ):
        await pa._loop_on_turn_complete(turn_id)


class TestAnnouncedRowsDoNotOutliveTheTurn:
    @pytest.mark.asyncio
    async def test_turn_parked_on_no_answer_expires_the_unreached_rows(self):
        """Repro, no interrupt required: 4 calls announced, the user closes
        the tab, gate 0 goes untethered and CAS-expires its own row, the turn
        parks. Rows 1-3 were never gated — they must not survive as pending.
        """
        store = _FakeRowStore()
        session = _session_with_store(store)

        async def _wait(request_id, *a, **kw):
            # Untethered: _wait_for_permission_resolution CAS-expires its row.
            store.status[request_id] = "expired"
            return "expired"

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(pa, "_wait_for_permission_resolution", _wait),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            assert list(store.status.values()) == ["pending"] * 4

            outcome = await pa._loop_permission_check("web_search", {}, "tc_0")
            assert outcome is PermissionOutcome.NO_ANSWER

            await _end_the_turn()

        assert store.status == {
            "rid-tc_0": "expired",
            "rid-tc_1": "expired",
            "rid-tc_2": "expired",
            "rid-tc_3": "expired",
        }, "stranded rows would re-render as a phantom card that runs nothing"

    @pytest.mark.asyncio
    async def test_interrupted_turn_expires_every_announced_row(self):
        """Stop (or the composer's bare interrupt) leaves the waited-on row
        'pending' by design — no decision may be fabricated. But the turn is
        over, so nothing will ever claim it or the rows behind it."""
        store = _FakeRowStore()
        session = _session_with_store(store)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="interrupted"),
            ),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            outcome = await pa._loop_permission_check("web_search", {}, "tc_0")
            assert outcome is PermissionOutcome.NO_ANSWER

            await _end_the_turn()

        assert set(store.status.values()) == {"expired"}

    @pytest.mark.asyncio
    async def test_concurrent_approval_is_not_clobbered(self):
        """The sweep is a CAS (`WHERE id = $1 AND status = 'pending'`): a
        decision that landed a microsecond earlier still wins, and only rows
        the sweep actually expired are broadcast as resolved."""
        store = _FakeRowStore()
        session = _session_with_store(store)
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            # The user clicked "Approve all" as the turn was unwinding.
            store.status["rid-tc_1"] = "approved"
            bcast.reset_mock()

            await _end_the_turn()

        assert store.status["rid-tc_1"] == "approved"
        assert store.status["rid-tc_0"] == "expired"
        resolved = [
            c.args[1]["id"]
            for c in bcast.call_args_list
            if c.args[0] == "permission.resolved"
        ]
        assert "tc_1" not in resolved
        assert set(resolved) == {"tc_0", "tc_2", "tc_3"}

    @pytest.mark.asyncio
    async def test_mid_batch_switch_to_autonomous_retires_every_row(self):
        """Trigger (c): the gate for call i returns APPROVED at the top,
        before the claim block, so nothing ever retires rows i..N."""
        store = _FakeRowStore()
        session = _session_with_store(store)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            session.permission_mode = "autonomous"
            outcome = await pa._loop_permission_check("web_search", {}, "tc_1")

        assert outcome is PermissionOutcome.APPROVED
        assert set(store.status.values()) == {"expired"}

    @pytest.mark.asyncio
    async def test_mid_batch_switch_to_auto_accept_keeps_shell_rows(self):
        """Trigger (b) — but only for calls the new mode really auto-approves.
        A shell call still gates under auto_accept, so its announced row must
        survive for the gate that is about to claim it."""
        calls = FOUR_CALLS[:2] + [
            {"name": "run_command", "args": {"cmd": "rm -rf /tmp/x"}, "id": "tc_sh"}
        ]
        store = _FakeRowStore()
        session = _session_with_store(store)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._loop_announce_permission_batch(calls)
            session.permission_mode = "auto_accept"
            await pa._loop_permission_check("web_search", {}, "tc_0")

        assert store.status["rid-tc_0"] == "expired"
        assert store.status["rid-tc_1"] == "expired"
        assert store.status["rid-tc_sh"] == "pending", (
            "the shell gate still needs its announced row to claim"
        )


class TestClaimSelectSoftFailKeepsTheAnnouncedRow:
    """Defect 2, backend half. If the claim SELECT raises, falling through to
    a second INSERT hands the waiter a NEW approval_id while the card still
    shows the announced one — the decision then resolves a row nobody is
    listening on and the turn blocks forever with the card gone. The announce
    owner already knows the row id in memory; use it."""

    @pytest.mark.asyncio
    async def test_db_blip_on_claim_select_reuses_the_announced_row(self):
        store = _FakeRowStore()
        session = _session_with_store(store)
        waited = {}

        async def _wait(request_id, *a, **kw):
            waited["id"] = request_id
            return "approved"

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(pa, "_wait_for_permission_resolution", _wait),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            store.fetchrow = AsyncMock(side_effect=RuntimeError("connection reset"))
            insert = AsyncMock(return_value="rid-SECOND")
            with patch.object(pa, "_insert_permission_request", insert):
                outcome = await pa._loop_permission_check("web_search", {}, "tc_0")

        insert.assert_not_awaited()
        assert waited["id"] == "rid-tc_0"
        assert outcome is PermissionOutcome.APPROVED

    @pytest.mark.asyncio
    async def test_sweep_db_failure_is_soft_and_keeps_the_rows_tracked(self):
        """The sweep runs first inside the turn-end hook — if it raised, the
        turn's message save, title pass and cloud push would all be skipped.
        Soft-fail, and leave the unreached rows on the ledger so the next
        sweep retries them."""
        store = _FakeRowStore()
        session = _session_with_store(store)
        ledger: dict = {}

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_subscribers", {}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            store.fetchval = AsyncMock(side_effect=RuntimeError("pool exhausted"))
            await _end_the_turn()  # must not raise

        assert set(ledger) == {"tc_0", "tc_1", "tc_2", "tc_3"}


# =============================================================================
# Defect A — a sweep must not expire the row a gate is mid-claim on
#
# The claim block is `async with pool.acquire() as conn: await conn.fetchrow()`.
# The context-manager EXIT is a yield point, and `_active_permission_request_id`
# (the sweep's "don't touch this row" guard) is not published until ~60 lines
# later, just before the wait. A fire-and-forget `mode.set` sweep that lands in
# that window CAS-expires the very row this gate is about to wait on; the wait
# then re-SELECTs 'expired' -> NO_ANSWER and the turn parks *immediately after*
# the user switched to autonomous. Publishing the id later cannot fix it: the
# row id is not known until after the awaited SELECT. The reservation must be
# keyed on tool_call_id and taken BEFORE the SELECT.
# =============================================================================


def _session_with_yielding_pool(store: _FakeRowStore, on_acquire_exit: list):
    """Like ``_session_with_store`` but the acquire() context-manager EXIT is a
    real yield point: it awaits (and consumes) the first hook parked in
    ``on_acquire_exit``. That is precisely where Defect A's window opens.

    The hook is popped *before* it is awaited so a sweep running inside it —
    which acquires the same pool — cannot re-enter itself.
    """

    def _acquire():
        ctx = MagicMock()
        ctx.__aenter__ = AsyncMock(return_value=store)

        async def _aexit(*_exc):
            if on_acquire_exit:
                hook = on_acquire_exit.pop(0)
                await hook()
            return False

        ctx.__aexit__ = _aexit
        return ctx

    session = _mock_session()
    session.workspace_sync = None
    session.workspace_manager = None
    session.messages = []
    session.postgres_conn = MagicMock()
    session.postgres_conn.acquire = _acquire
    return session


class TestSweepCannotExpireAGateMidClaim:
    @pytest.mark.asyncio
    async def test_mode_set_sweep_landing_inside_the_claim_window(self):
        """Reproduces the window deliberately: the sweep runs at the claim
        SELECT's context-manager exit, before the gate could possibly know its
        row id. The claimed row must survive and the gate must still resolve
        APPROVED — while the rows nothing is holding are still swept."""
        store = _FakeRowStore()
        on_exit: list = []
        session = _session_with_yielding_pool(store, on_exit)
        swept = {"ran": False}

        async def _sweep_lands_now():
            # Exactly what the WS handler fires from mode.set: a detached
            # task, scoped to the new (looser) mode.
            swept["ran"] = True
            await asyncio.create_task(
                pa._retire_announced_permission_rows(
                    "mode.set autonomous", "autonomous"
                )
            )

        async def _wait(request_id, *a, **kw):
            # Mirrors the real waiter: it re-SELECTs the row after registering
            # its listener, so a sweep that already expired the row is seen.
            status = store.status.get(request_id)
            if status != "pending":
                return status or "expired"
            store.status[request_id] = "approved"
            return "approved"

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(pa, "_wait_for_permission_resolution", _wait),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            assert list(store.status.values()) == ["pending"] * 4
            # Arm the hook only now — the announce's own acquires must not
            # trigger it.
            on_exit.append(_sweep_lands_now)
            outcome = await pa._loop_permission_check("web_search", {}, "tc_0")

        assert swept["ran"], "the race window was never reproduced"
        assert store.status["rid-tc_0"] == "approved", (
            "the sweep expired the row this gate was about to wait on — the "
            "turn parks right after the user unblocked it"
        )
        assert outcome is PermissionOutcome.APPROVED
        # The sweep really did its job on everything nobody was holding.
        assert store.status["rid-tc_1"] == "expired"
        assert store.status["rid-tc_2"] == "expired"
        assert store.status["rid-tc_3"] == "expired"


class TestGateReservationIsAlwaysReleased:
    """A tool_call_id left in ``_gates_in_flight`` is a row no sweep will ever
    retire — the same stranded phantom card, arrived at from the other side.
    Every exit of the gating section must release it, including the two early
    returns that do not fall through to the end of the function."""

    @pytest.mark.asyncio
    async def test_released_on_terminal_prior_decision_return(self):
        """Phase 5 wake path: the claim SELECT finds an approved row and
        returns immediately, long before the wait."""
        session, _ = _conn_with({"id": "rid-prior", "status": "approved"})
        gates: set = set()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_gates_in_flight", gates),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_prior")

        assert outcome is PermissionOutcome.APPROVED
        assert gates == set()

    @pytest.mark.asyncio
    async def test_released_on_insert_failure_return(self):
        """DB refused the row: a real DECLINE, returned before the wait."""
        session, _ = _conn_with(None)
        gates: set = set()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_gates_in_flight", gates),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(
                pa, "_insert_permission_request", AsyncMock(return_value=None)
            ),
        ):
            outcome = await pa._loop_permission_check("web_search", {}, "tc_noinsert")

        assert outcome is PermissionOutcome.DECLINED
        assert gates == set()

    @pytest.mark.asyncio
    async def test_released_on_the_normal_answered_path(self):
        session, _ = _conn_with({"id": "rid-announced", "status": "pending"})
        gates: set = set()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_gates_in_flight", gates),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(
                pa,
                "_wait_for_permission_resolution",
                AsyncMock(return_value="approved"),
            ),
        ):
            await pa._loop_permission_check("web_search", {}, "tc_ok")

        assert gates == set()

    @pytest.mark.asyncio
    async def test_released_when_the_waiting_gate_is_cancelled(self):
        """The loop task is cancelled mid-gate (✕-detach, drain, shutdown).
        The reservation must not survive the cancellation."""
        session, _ = _conn_with({"id": "rid-announced", "status": "pending"})
        gates: set = set()
        entered = asyncio.Event()

        async def _wait(request_id, *a, **kw):
            entered.set()
            await asyncio.sleep(60)
            return "approved"

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_gates_in_flight", gates),
            patch.object(pa, "_announced_permission_rows", {}),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
            patch.object(pa, "_wait_for_permission_resolution", _wait),
        ):
            task = asyncio.create_task(
                pa._loop_permission_check("web_search", {}, "tc_cancelled")
            )
            await entered.wait()
            assert gates == {"tc_cancelled"}
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task

        assert gates == set()


# =============================================================================
# Defect B — a sweep that cannot reach the DB must not drop the ledger
#
# Entries are popped off the ledger up front ("take ownership"), but the
# `_session is None or postgres_conn is None` guard returns *after* the pop.
# Those rows are then gone from memory and were never expired in the DB —
# nothing will ever retire them, and they re-render as phantom approval cards
# on every reattach. The `except` path already restores unreached entries;
# the early return must do the same.
# =============================================================================


class TestSweepWithoutADbKeepsTheLedger:
    @pytest.mark.asyncio
    async def test_no_pool_leaves_every_entry_for_the_next_sweep(self):
        ledger = {
            "tc_0": ("rid-tc_0", "web_search", "tid"),
            "tc_1": ("rid-tc_1", "web_search", "tid"),
        }
        session = _mock_session()
        session.postgres_conn = None
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._retire_announced_permission_rows("pool gone")

        assert set(ledger) == {"tc_0", "tc_1"}, (
            "rows dropped from the ledger without ever being expired in the DB "
            "can never be retired by anyone"
        )
        bcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_session_leaves_every_entry_for_the_next_sweep(self):
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}

        with (
            patch.object(pa, "_session", None),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._retire_announced_permission_rows("session gone")

        assert set(ledger) == {"tc_0"}


class TestSweepHoldsExactRuntimeAuthority:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("authority", "required_fragments"),
        [
            (
                ("stateless", 41),
                ("run_queue AS queue", "queue.lease_token", "FOR SHARE OF queue"),
            ),
            (
                ("pinned", PINNED_AGENT_ID),
                (
                    "threads AS thread",
                    "agents AS agent",
                    "FOR NO KEY UPDATE OF thread",
                    "FOR SHARE OF agent",
                ),
            ),
        ],
    )
    async def test_irreversible_update_and_owner_fence_are_one_statement(
        self, authority, required_fragments
    ):
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}
        session, conn = _conn_with(None)
        conn.fetchval = AsyncMock(return_value=None)

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(
                pa, "_permission_retirement_authority", return_value=authority
            ),
            patch.object(pa, "_broadcast", MagicMock()) as broadcast,
        ):
            await pa._retire_announced_permission_rows("authority test")

        sql, request_id, thread_id, credential = conn.fetchval.await_args.args
        assert "UPDATE thread_permission_requests AS request" in sql
        assert all(fragment in sql for fragment in required_fragments)
        assert (request_id, thread_id, credential) == (
            "rid-tc_0",
            "tid",
            authority[1],
        )
        # A moved owner returns no row. The stale process drops only its local
        # bookkeeping and never fabricates a resolved event.
        assert ledger == {}
        broadcast.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_exact_owner_leaves_pending_row_for_successor(self):
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}
        session, conn = _conn_with(None)
        conn.fetchval = AsyncMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_permission_retirement_authority", return_value=None),
        ):
            await pa._retire_announced_permission_rows("owner already moved")

        conn.fetchval.assert_not_awaited()
        assert set(ledger) == {"tc_0"}


# =============================================================================
# Defect C — the ledger must never outlive, or reach across, a session
#
# C1: _terminate_session_inner clears every sibling global but not these, and
#     it CANCELS the loop task — the graph's `except CancelledError: return`
#     skips on_turn_complete, where the turn-end sweep lives. So a session
#     terminated mid-batch strands its rows; the NEXT session's first announce
#     sweeps them and broadcasts permission.resolved carrying the PREVIOUS
#     thread's tool_call_ids to the NEW thread's clients.
# C2: defence in depth — entries carry their thread id, and a sweep skips any
#     entry that is not the current thread's.
# =============================================================================


async def _terminate(reason: str = "rest_detach", **kwargs):
    """Drive the real terminate path with everything unrelated stubbed."""
    with (
        patch.object(pa, "_update_thread_status", AsyncMock()),
        patch.object(pa._session_termination, "stop_watchdogs", MagicMock()),
        patch.object(pa, "_stop_thread_control_watcher", AsyncMock()),
        patch.object(pa, "_stop_thread_interrupt_watcher", AsyncMock()),
        patch.object(pa._canvas_control, "clear_all", MagicMock()),
        patch.object(pa, "_loop_task", None),
        patch.object(pa, "_event_writer", None),
        patch.object(pa._session_termination, "max_sessions_per_process", 0),
    ):
        await pa._session_termination._terminate_inner(reason, **kwargs)


class TestTerminateDoesNotStrandOrLeakAnnouncedRows:
    @pytest.mark.asyncio
    async def test_terminate_retires_the_rows_and_clears_the_ledger(self):
        store = _FakeRowStore()
        session = _session_with_store(store)
        session.cleanup = AsyncMock()
        session.workspace_manager = None
        ledger: dict = {}

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid-old"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", {"tc_0"}),
            patch.object(pa, "_active_permission_request_id", "rid-tc_0"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            assert set(ledger) == {"tc_0", "tc_1", "tc_2", "tc_3"}

            with (
                patch.object(pa, "_control_owner_agent_id", PINNED_AGENT_ID),
                patch.object(
                    pa, "_close_pinned_control_inbox", AsyncMock(return_value=True)
                ) as close_inbox,
            ):
                await _terminate()

            assert ledger == {}, "the next session inherits this thread's rows"
            assert pa._gates_in_flight == set()
            assert pa._active_permission_request_id is None
            close_inbox.assert_awaited_once_with(agent_id=PINNED_AGENT_ID)

        assert set(store.status.values()) == {"expired"}, (
            "rows left pending on an ended thread re-render as phantom cards"
        )

    @pytest.mark.asyncio
    async def test_next_session_announce_does_not_resolve_the_old_threads_calls(
        self,
    ):
        store = _FakeRowStore()
        old_session = _session_with_store(store)
        old_session.cleanup = AsyncMock()
        old_session.workspace_manager = None
        ledger: dict = {}
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", old_session),
            patch.object(pa._session_identity, "_thread_id", "tid-old"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._loop_announce_permission_batch(FOUR_CALLS)
            await _terminate()

            # A brand-new session on a brand-new thread, same process — and
            # the SAME database: a pool agent serves many threads off one
            # Postgres, so the previous thread's rows are still reachable.
            bcast.reset_mock()
            new_session = _session_with_store(store)
            with (
                patch.object(pa, "_session", new_session),
                patch.object(pa._session_identity, "_thread_id", "tid-new"),
            ):
                await pa._loop_announce_permission_batch(
                    [{"name": "web_search", "args": {}, "id": "tc_new"}]
                )

        leaked = [
            c.args[1]["id"]
            for c in bcast.call_args_list
            if c.args[0] == "permission.resolved"
        ]
        assert leaked == [], (
            f"the new thread's clients were told about {leaked} — tool calls "
            "from a thread they never saw"
        )

    @pytest.mark.asyncio
    async def test_stateless_teardown_without_live_lease_only_clears_local_state(self):
        store = _FakeRowStore()
        store.status["rid-tc_0"] = "pending"
        session = _session_with_store(store)
        session.cleanup = AsyncMock()
        session.workspace_manager = None
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}
        gates = {"tc_0"}
        close_inbox = AsyncMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", gates),
            patch.object(pa, "_active_permission_request_id", "rid-tc_0"),
            patch.object(pa, "_stateless_mode", return_value=True),
            patch.object(pa, "_permission_retirement_authority", return_value=None),
            patch.object(pa, "_close_pinned_control_inbox", close_inbox),
        ):
            await _terminate(mark_thread=False)

        assert store.status["rid-tc_0"] == "pending"
        assert ledger == {}
        assert gates == set()
        close_inbox.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_pinned_rebind_after_admission_close_cannot_expire_successor_row(
        self,
    ):
        store = _FakeRowStore()
        store.status["rid-tc_0"] = "pending"
        session = _session_with_store(store)
        session.cleanup = AsyncMock()
        session.workspace_manager = None
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}
        broadcast = MagicMock()

        # Admission closure succeeded in its earlier transaction, but the
        # binding moves before the retirement statement. PostgreSQL then
        # returns no updated row from the exact owner-fenced CTE.
        store.fetchval = AsyncMock(return_value=None)
        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_control_owner_agent_id", PINNED_AGENT_ID),
            patch.object(
                pa, "_close_pinned_control_inbox", AsyncMock(return_value=True)
            ),
            patch.object(
                pa,
                "_permission_retirement_authority",
                return_value=("pinned", PINNED_AGENT_ID),
            ),
            patch.object(pa, "_broadcast", broadcast),
        ):
            await _terminate(mark_thread=False)

        assert store.status["rid-tc_0"] == "pending"
        assert ledger == {}
        assert not any(
            call.args and call.args[0] == "permission.resolved"
            for call in broadcast.call_args_list
        )


class TestSweepIsThreadScoped:
    @pytest.mark.asyncio
    async def test_entries_from_another_thread_are_never_swept(self):
        """Defence in depth: even if a clear is missed, a sweep may only touch
        rows belonging to the thread it is running on."""
        store = _FakeRowStore()
        store.status["rid-old"] = "pending"
        store.status["rid-mine"] = "pending"
        session = _session_with_store(store)
        ledger = {
            "tc_old": ("rid-old", "web_search", "tid-old"),
            "tc_mine": ("rid-mine", "web_search", "tid-mine"),
        }
        bcast = MagicMock()

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid-mine"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", bcast),
        ):
            await pa._retire_announced_permission_rows("turn 1 ended")

        assert store.status["rid-old"] == "pending"
        assert store.status["rid-mine"] == "expired"
        assert set(ledger) == {"tc_old"}, "the foreign entry must stay untouched"
        resolved = [
            c.args[1]["id"]
            for c in bcast.call_args_list
            if c.args[0] == "permission.resolved"
        ]
        assert resolved == ["tc_mine"]


class TestGatesInFlightExclusion:
    @pytest.mark.asyncio
    async def test_a_reserved_tool_call_is_skipped_by_the_sweep(self):
        store = _FakeRowStore()
        store.status["rid-tc_0"] = "pending"
        store.status["rid-tc_1"] = "pending"
        session = _session_with_store(store)
        ledger = {
            "tc_0": ("rid-tc_0", "web_search", "tid"),
            "tc_1": ("rid-tc_1", "web_search", "tid"),
        }

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", {"tc_0"}),
            patch.object(pa, "_active_permission_request_id", None),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._retire_announced_permission_rows("turn ended")

        assert store.status["rid-tc_0"] == "pending"
        assert store.status["rid-tc_1"] == "expired"
        assert set(ledger) == {"tc_0"}

    @pytest.mark.asyncio
    async def test_active_request_id_exclusion_still_holds(self):
        """Belt-and-braces: the older guard is kept, not replaced."""
        store = _FakeRowStore()
        store.status["rid-tc_0"] = "pending"
        session = _session_with_store(store)
        ledger = {"tc_0": ("rid-tc_0", "web_search", "tid")}

        with (
            patch.object(pa, "_session", session),
            patch.object(pa._session_identity, "_thread_id", "tid"),
            patch.object(pa, "_announced_permission_rows", ledger),
            patch.object(pa, "_gates_in_flight", set()),
            patch.object(pa, "_active_permission_request_id", "rid-tc_0"),
            patch.object(pa, "_subscribers", {"c1": MagicMock()}),
            patch.object(pa, "_broadcast", MagicMock()),
        ):
            await pa._retire_announced_permission_rows("turn ended")

        assert store.status["rid-tc_0"] == "pending"
        assert set(ledger) == {"tc_0"}
