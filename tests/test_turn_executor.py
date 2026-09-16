"""Mock-based tests for the M3 stateless turn executor (turn_executor.py).

The run_queue substrate itself is covered by the real-Postgres suite
(tests/test_run_queue.py); everything here fakes the queue functions and the
persistent_app machinery and asserts the DRIVER's behavior: the claim →
bundle → attach → inject → complete flow, skip-if-answered, the error/release
paths, the lease-lost polite abort, the strip-restored-pending helper, the
scrub-on-claim acceptance (§5.6), soft affinity, and the fenced persist in
postgres_db.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, call, patch
from uuid import uuid4

import pytest

import agent.api.persistent_app as pa
import agent.api.turn_executor as te
from agent.api.lease_context import (
    LeaseHandle,
    LeaseLostError,
    current_lease,
    get_current_lease,
)
from agent.api.orchestrator_client import ClaimBundleError
from shared.run_queue import ClaimedUnit


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_claim(
    unit_id=None,
    token: int = 3,
    input_seq: Optional[int] = 10,
    consumed_seq: Optional[int] = None,
    control_input_seq: int = 0,
    control_consumed_seq: int = 0,
    attempts: int = 1,
) -> ClaimedUnit:
    return ClaimedUnit(
        unit_id=unit_id or uuid4(),
        unit_kind="session_turn",
        fair_key=None,
        lease_token=token,
        input_seq=input_seq,
        consumed_seq=consumed_seq,
        attempts_since_completion=attempts,
        leased_until=datetime.now(timezone.utc),
        control_input_seq=control_input_seq,
        control_consumed_seq=control_consumed_seq,
    )


class FakeDB:
    """Stands in for the agent's PostgresDB pool wrapper."""

    def __init__(self, pending_rows: Optional[List[Dict[str, Any]]] = None):
        self.pending_rows = list(pending_rows or [])
        self.fetch_calls: List[tuple] = []
        self.refreshed_consumed_seq: Optional[int] = None
        # Transcript leg of skip-if-answered: the seq the transcript proves
        # answered, or None (the ordinary claim path).
        self.transcript_answered_seq: Optional[int] = None
        # Step 4a shutdown classification: do all persisted tool calls of the
        # pending turn have their ToolMessage? (None = query fails → park)
        self.tool_effects_durable: Optional[bool] = None

    async def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        next_turn = int(getattr(pa._session, "turn_count", 0) or 0) + 1
        return [
            {**row, "turn_number": row.get("turn_number", next_turn)}
            for row in self.pending_rows
        ]

    async def fetchval(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        if sql == te._EXACT_CONSUMED_SEQ_AFTER_ATTACH_SQL:
            return (
                self.refreshed_consumed_seq
                if self.refreshed_consumed_seq is not None
                else int(args[3])
            )
        if sql == te._PENDING_EVENT_EXISTS_SQL:
            return any(row.get("delivery_id") for row in self.pending_rows)
        if sql == te._ANSWERED_BY_TRANSCRIPT_SQL:
            return self.transcript_answered_seq
        if sql == te._TOOL_EFFECTS_DURABLE_SQL:
            if self.tool_effects_durable is None:
                raise RuntimeError("durability probe unavailable")
            return self.tool_effects_durable
        return None


class FakeSession:
    def __init__(
        self,
        shell_owner_tokens: Optional[List[int]] = None,
        *,
        stateless_warm_reuse_safe: bool = True,
    ):
        self.messages: List[Any] = []
        self.postgres_conn = None
        self._shell_owner_tokens = shell_owner_tokens
        self.shell_owner_tokens: List[int] = []
        self.stateless_warm_reuse_safe = stateless_warm_reuse_safe
        self.turn_count = 0
        self.tool_context = SimpleNamespace(_stateless_subagent_recovery_active=False)

    def set_shell_owner_token(self, token: int) -> None:
        self.shell_owner_tokens.append(token)
        if self._shell_owner_tokens is not None:
            self._shell_owner_tokens.append(token)


class Harness:
    """Wires fake run_queue functions + fake persistent_app machinery."""

    def __init__(self, monkeypatch, pending_rows=None):
        self.calls: Dict[str, List[Dict[str, Any]]] = {
            "claim": [],
            "complete": [],
            "heartbeat": [],
            "park": [],
            "release": [],
            "attach_failure": [],
            "journal": [],
            "bundle": [],
            "attach": [],
            "terminate": [],
            "control_start": [],
            "control_stop": [],
            "interrupt_start": [],
            "interrupt_open": [],
            "interrupt_close": [],
            "interrupt_stop": [],
            "interrupt_drain": [],
            "interrupt_stale": [],
            "shell_owner_token": [],
        }
        self.consumed: List[Dict[str, Any]] = []
        self.interrupt_order: List[str] = []
        self.db = FakeDB(pending_rows)
        self.loop_behavior = "complete"  # complete | hang | die | cleanup
        self.interrupt_observations: List[Dict[str, Any]] = []
        self.stale_result = (0, None)
        self.restored_turn_count = 0
        self.heartbeat_result: Any = datetime.now(timezone.utc)
        self.bundle_error: Optional[Exception] = None
        self.attach_error: Optional[Exception] = None
        self.stateless_warm_reuse_safe = True
        self.session_postgres_conn = None
        self.sessions: List[FakeSession] = []
        self._fake_loop_tasks: List[asyncio.Task] = []
        self.disposition_order: List[str] = []
        self.park_reasons: List[Any] = []
        self.attach_failure_state = "queued"

        pa._agent = SimpleNamespace(postgres_conn=self.db)
        pa._session = None
        pa._thread_id = None
        pa._loop_user_queue = None
        pa._loop_task = None
        pa._turn_start_external_hook = None
        pa._turn_complete_external_hook = None
        pa._turn_tool_execution_external_hook = None
        pa._interrupt_watcher_task = None
        pa._interrupt_watcher_stop = None
        pa._interrupt_owner_lease_token = None
        pa._interrupt_owner_turn_id = None
        pa._tool_inflight = False
        pa._turn_tool_execution_identity = None
        pa._loop_interrupt_flag = None
        pa._loop_interrupt_target_turn_id = None
        pa._hard_interrupt_event = asyncio.Event()
        pa._turn_event_open = False
        pa._pending_cloud_push_task = None
        pa._pending_cloud_push_staged = None
        pa._pending_cloud_push_claim = None
        pa._pending_cloud_push_sync = None
        pa._background_cloud_pushes = {}

        harness = self

        async def fake_complete(db, *, unit_id, lease_token, consumed_seq):
            harness.calls["complete"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "consumed_seq": consumed_seq,
                }
            )
            return "done"

        async def fake_release(
            db, *, unit_id, lease_token, backoff_seconds=0.0, error=False
        ):
            harness.disposition_order.append("release")
            harness.calls["release"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "backoff_seconds": backoff_seconds,
                    "error": error,
                }
            )
            return "queued"

        async def fake_park(db, *, unit_id, lease_token, reason=None):
            harness.disposition_order.append("park")
            harness.calls["park"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                }
            )
            harness.park_reasons.append(reason)
            return "parked"

        async def fake_record_attach_failure(
            db, *, unit_id, lease_token, error, signature, backoff_seconds, **_kw
        ):
            # The attach path's release (stateless_turn_resilience.md step 2):
            # counted + backed off in one CAS instead of a plain error release.
            harness.disposition_order.append("release")
            harness.calls["attach_failure"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "error": error,
                    "signature": signature,
                    "backoff_seconds": backoff_seconds,
                }
            )
            return {
                "state": harness.attach_failure_state,
                "attempts_since_completion": 1,
                "attach_failures": 1,
                "park_reason": (
                    "attach_failed"
                    if harness.attach_failure_state == "parked"
                    else None
                ),
                "run_after": None,
            }

        async def fake_journal(db, *, thread_id, kind, payload, **_kw):
            harness.calls["journal"].append(
                {"thread_id": thread_id, "kind": kind, "payload": payload}
            )
            return (0, len(harness.calls["journal"]))

        async def fake_heartbeat(db, *, unit_id, lease_token, lease_ttl_seconds=60.0):
            harness.calls["heartbeat"].append(
                {"unit_id": unit_id, "lease_token": lease_token}
            )
            return harness.heartbeat_result

        monkeypatch.setattr(te, "complete_unit", fake_complete)
        monkeypatch.setattr(te, "park_unit", fake_park)
        monkeypatch.setattr(te, "release_unit", fake_release)
        monkeypatch.setattr(te, "record_attach_failure", fake_record_attach_failure)
        monkeypatch.setattr(te, "append_system_frame", fake_journal)
        monkeypatch.setattr(te, "heartbeat_unit", fake_heartbeat)

        async def fake_open_interrupt(db, *, unit_id, lease_token, turn_id):
            harness.interrupt_order.append("open")
            harness.calls["interrupt_open"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "turn_id": turn_id,
                }
            )
            return True

        async def fake_close_interrupt(
            db, *, unit_id, lease_token, turn_id, completed_input_seq=None
        ):
            harness.interrupt_order.append("close")
            harness.calls["interrupt_close"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "turn_id": turn_id,
                    "completed_input_seq": completed_input_seq,
                }
            )
            return True

        monkeypatch.setattr(te, "open_interrupt_admission", fake_open_interrupt)
        monkeypatch.setattr(te, "close_interrupt_admission", fake_close_interrupt)

        # --- fake orchestrator client -------------------------------------
        class FakeClient:
            async def get_claim_bundle(self, unit_id, lease_token):
                harness.calls["bundle"].append(
                    {"unit_id": unit_id, "lease_token": lease_token}
                )
                if harness.bundle_error is not None:
                    raise harness.bundle_error
                return {
                    "unit_id": unit_id,
                    "thread_id": unit_id,
                    "unit_kind": "session_turn",
                    "execution_lane": "stateless",
                    "watermarks": {"input_seq": None, "consumed_seq": None},
                    "attach": harness.attach_for(unit_id),
                }

        pa._orchestrator_client = FakeClient()
        self._attach_overrides: Dict[str, Dict[str, Any]] = {}

        # --- fake persistent_app machinery --------------------------------
        async def fake_attach(**kwargs):
            harness.calls["attach"].append(kwargs)
            if harness.attach_error is not None:
                raise harness.attach_error
            pa._session = FakeSession(
                harness.calls["shell_owner_token"],
                stateless_warm_reuse_safe=harness.stateless_warm_reuse_safe,
            )
            pa._session.postgres_conn = harness.session_postgres_conn
            pa._session.turn_count = harness.restored_turn_count
            pa._turn_tool_execution_identity = None
            pa._turn_tool_execution_external_hook = None
            harness.sessions.append(pa._session)
            pa._thread_id = kwargs.get("thread_id")
            pa._loop_user_queue = asyncio.Queue()

        async def fake_terminate(
            reason,
            *,
            mark_thread=True,
            preserve_shell=None,
            preserve_workspace_daemons=False,
        ):
            harness.disposition_order.append("terminate")
            harness.calls["terminate"].append(
                {
                    "reason": reason,
                    "mark_thread": mark_thread,
                    "preserve_shell": preserve_shell,
                    "preserve_workspace_daemons": preserve_workspace_daemons,
                }
            )
            task = pa._loop_task
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await task
            pa._loop_task = None
            pa._session = None
            pa._thread_id = None
            pa._loop_user_queue = None
            pa._turn_tool_execution_identity = None
            pa._turn_tool_execution_external_hook = None

        def fake_ensure(source, client_id=None):
            if pa._loop_user_queue is None:
                return False
            if pa._loop_task is None or pa._loop_task.done():
                pa._loop_task = asyncio.create_task(harness._fake_loop())
                harness._fake_loop_tasks.append(pa._loop_task)
            return True

        async def fake_start_control_watcher(*, lease_token=None, agent_id=None):
            harness.calls["control_start"].append(
                {"lease_token": lease_token, "agent_id": agent_id}
            )
            return 0

        async def fake_stop_control_watcher():
            harness.calls["control_stop"].append({})

        async def fake_start_interrupt_watcher(*, lease_token, target_turn_id):
            harness.interrupt_order.append("start")
            harness.calls["interrupt_start"].append(
                {"lease_token": lease_token, "target_turn_id": target_turn_id}
            )
            pa._interrupt_owner_lease_token = lease_token
            pa._interrupt_owner_turn_id = target_turn_id
            return 0

        async def fake_stop_interrupt_watcher():
            harness.interrupt_order.append("stop")
            harness.calls["interrupt_stop"].append({})
            pa._interrupt_owner_lease_token = None
            pa._interrupt_owner_turn_id = None

        async def fake_drain_interrupts(*, lease_token, target_turn_id):
            harness.interrupt_order.append("drain")
            harness.calls["interrupt_drain"].append(
                {"lease_token": lease_token, "target_turn_id": target_turn_id}
            )
            return 0

        async def fake_reconcile_stale_interrupts(*, lease_token):
            harness.interrupt_order.append("stale")
            harness.calls["interrupt_stale"].append({"lease_token": lease_token})
            return harness.stale_result

        monkeypatch.setattr(pa, "_attach_session", fake_attach)
        monkeypatch.setattr(pa, "_terminate_session", fake_terminate)
        monkeypatch.setattr(pa, "_ensure_persistent_loop_started", fake_ensure)
        monkeypatch.setattr(
            pa, "_start_thread_control_watcher", fake_start_control_watcher
        )
        monkeypatch.setattr(
            pa, "_stop_thread_control_watcher", fake_stop_control_watcher
        )
        monkeypatch.setattr(
            pa, "_start_thread_interrupt_watcher", fake_start_interrupt_watcher
        )
        monkeypatch.setattr(
            pa, "_stop_thread_interrupt_watcher", fake_stop_interrupt_watcher
        )
        monkeypatch.setattr(pa, "_drain_thread_interrupts", fake_drain_interrupts)
        monkeypatch.setattr(
            pa,
            "_reconcile_stale_thread_interrupts",
            fake_reconcile_stale_interrupts,
        )

        self.executor = te.StatelessTurnExecutor(
            pod_name="test-pod", abort_grace_seconds=0.05
        )

    def attach_for(self, unit_id: str) -> Dict[str, Any]:
        return self._attach_overrides.get(
            unit_id,
            {
                "thread_id": unit_id,
                "config_override": None,
                "resolved_config": {"agent": {"llm": {"model": "m"}}},
                "project_ids": [],
                "datasources": None,
                "config_name": "session_base",
                # Production claim bundles always carry this explicit pair.
                # Keep the executor fake honest even for no-workspace tests.
                "workspace_generation": None,
                "workspace_runtime_incarnation": None,
            },
        )

    def set_attach(self, unit_id: str, attach: Dict[str, Any]) -> None:
        self._attach_overrides[str(unit_id)] = attach

    def mark_tool_effect(self, turn_id: int) -> None:
        identity = (
            str(pa._thread_id),
            int(self.executor._lease.lease_token),
            int(turn_id),
        )
        pa._turn_tool_execution_identity = identity
        hook = pa._turn_tool_execution_external_hook
        if hook is not None:
            hook(identity)

    @staticmethod
    def has_tool_effect(claim: ClaimedUnit, turn_id: int = 1) -> bool:
        return pa._turn_tool_execution_started_for_claim(
            thread_id=str(claim.unit_id),
            lease_token=int(claim.lease_token),
            turn_id=turn_id,
        )

    async def _fake_loop(self):
        while True:
            item = await pa._loop_user_queue.get()
            self.consumed.append(item)
            turn_id = int(pa._session.turn_count) + 1
            pa._session.turn_count = turn_id
            pa._turn_tool_execution_identity = None
            pa._turn_event_open = True
            start_hook = pa._turn_start_external_hook
            if start_hook is not None:
                await start_hook(turn_id)
            if self.loop_behavior == "interrupt_checkpoint":
                while pa._loop_interrupt_flag is None:
                    await asyncio.sleep(0)
                self.interrupt_order.append("signal")
                self.interrupt_observations.append(
                    {
                        "turn_id": turn_id,
                        "mode": pa._loop_interrupt_flag,
                        "target_turn_id": pa._loop_interrupt_target_turn_id,
                        "hard_event_set": pa._hard_interrupt_event.is_set(),
                    }
                )
                self.interrupt_observations[-1]["consumed_mode"] = (
                    pa._loop_check_interrupt()
                )
                pa._turn_event_open = False
                hook = pa._turn_complete_external_hook
                if hook is not None:
                    hook(turn_id)
            elif self.loop_behavior == "turn_complete_cleanup":
                # Model a real tool having crossed its external-effect seam,
                # then use the production transcript/settlement callbacks.
                self.mark_tool_effect(turn_id)
                await pa._loop_on_turn_complete(turn_id)
                await pa._loop_on_turn_settled(turn_id)
            elif self.loop_behavior == "turn_error_cleanup":
                # A tool-bearing turn may fail in a later provider step. The
                # durable error path is still turn-owned cleanup and must keep
                # the effect latch until physical settlement.
                self.mark_tool_effect(turn_id)
                await pa._loop_on_error("provider failed after tool", turn_id)
                await pa._loop_on_turn_settled(turn_id)
            elif self.loop_behavior == "settled_effect":
                # Minimal post-tool turn that has fired the physical settled
                # hook but whose run_queue completion CAS still belongs to the
                # executor.
                self.mark_tool_effect(turn_id)
                pa._turn_event_open = False
                await pa._loop_on_turn_settled(turn_id)
            elif self.loop_behavior == "hang":
                await asyncio.sleep(3600)
            elif self.loop_behavior == "die":
                pa._turn_event_open = False
                raise RuntimeError("scripted turn failure")
            elif self.loop_behavior == "die_after_effect":
                self.mark_tool_effect(turn_id)
                pa._turn_event_open = False
                raise RuntimeError("scripted post-effect turn failure")
            else:
                pa._turn_event_open = False
                hook = pa._turn_complete_external_hook
                if hook is not None:
                    hook(turn_id)

    async def cleanup(self):
        for task in self._fake_loop_tasks:
            if not task.done():
                task.cancel()
            # Await even already-done tasks so a scripted failure's exception
            # is retrieved (no "Task exception was never retrieved" noise).
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task


_PA_SAVED_ATTRS = (
    "_session",
    "_thread_id",
    "_loop_user_queue",
    "_loop_task",
    "_turn_start_external_hook",
    "_turn_complete_external_hook",
    "_turn_tool_execution_external_hook",
    "_interrupt_watcher_task",
    "_interrupt_watcher_stop",
    "_interrupt_owner_lease_token",
    "_interrupt_owner_turn_id",
    "_agent",
    "_orchestrator_client",
    "_tool_inflight",
    "_turn_tool_execution_identity",
    "_loop_interrupt_flag",
    "_loop_interrupt_target_turn_id",
    "_hard_interrupt_event",
    "_turn_event_open",
    "_pending_cloud_push_task",
    "_pending_cloud_push_staged",
    "_pending_cloud_push_claim",
    "_pending_cloud_push_sync",
    "_background_cloud_pushes",
)


@pytest.fixture
def harness(monkeypatch):
    saved = {name: getattr(pa, name) for name in _PA_SAVED_ATTRS}
    h = Harness(monkeypatch)
    try:
        yield h
    finally:
        for name, value in saved.items():
            setattr(pa, name, value)


async def _finish(h: Harness):
    await h.cleanup()


# ---------------------------------------------------------------------------
# 1. Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    @pytest.mark.asyncio
    async def test_claim_bundle_attach_inject_complete(self, harness):
        unit = uuid4()
        row_id = str(uuid4())
        harness.db.pending_rows = [{"id": row_id, "seq": 5, "content": "hello"}]
        claim = make_claim(unit_id=unit, token=7, input_seq=5, consumed_seq=None)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        # Bundle fetched with the claim's identity.
        assert harness.calls["bundle"] == [{"unit_id": str(unit), "lease_token": 7}]
        # Attach fed the bundle's attach object unchanged.
        assert len(harness.calls["attach"]) == 1
        assert harness.calls["attach"][0]["thread_id"] == str(unit)
        assert harness.calls["attach"][0]["config_name"] == "session_base"
        # The injected item carried the DB row id + content (no re-persist).
        assert harness.consumed == [{"content": "hello", "id": row_id}]
        # Completion carried the injected message's seq.
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 7, "consumed_seq": 5}
        ]
        assert not harness.calls["release"]
        assert pa._turn_tool_execution_external_hook is None
        # Affinity hint updated for the next poll.
        assert harness.executor._prefer_unit_id == unit
        # Lease handle points at this claim (fenced writers read it live).
        assert get_current_lease() is None  # var only set inside run()
        assert harness.executor._lease.unit_id == str(unit)
        assert harness.executor._lease.lease_token == 7
        assert harness.calls["shell_owner_token"] == [7]
        assert harness.interrupt_order == [
            "stale",
            "start",
            "open",
            "drain",
            "close",
            "stop",
            "drain",
            "stop",  # idempotent claim-finally belt
        ]

    @pytest.mark.asyncio
    async def test_event_input_keeps_role_and_stable_delivery_identity(self, harness):
        unit = uuid4()
        row_id = str(uuid4())
        delivery_id = str(uuid4())
        harness.db.pending_rows = [
            {
                "id": row_id,
                "seq": 5,
                "content": "[wake] inspect the job",
                "turn_number": 1,
                "role": "event",
                "delivery_id": delivery_id,
            }
        ]
        claim_delivery = AsyncMock(
            return_value={
                "message_id": row_id,
                "seq": 5,
                "claim_generation": 9,
            }
        )
        harness.db.claim_stateless_input_delivery = claim_delivery

        await harness.executor._serve_claim(
            # A pre-0185 wake can sit below the old human-only watermark. The
            # ledger identity, not seq>consumed alone, keeps it executable.
            make_claim(unit_id=unit, token=7, input_seq=5, consumed_seq=5)
        )
        await _finish(harness)

        claim_delivery.assert_awaited_once_with(
            thread_id=str(unit),
            delivery_id=delivery_id,
            lease_token=7,
            executor_id="test-pod",
            pod_uid=harness.executor._pod_uid,
        )
        assert harness.consumed == [
            {
                "content": "[wake] inspect the job",
                "id": row_id,
                "role": "event",
                "delivery_id": delivery_id,
                "claim_generation": 9,
            }
        ]
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 7, "consumed_seq": 5}
        ]

    @pytest.mark.asyncio
    async def test_subagent_recovery_reuses_abandoned_turn_and_watermark(self, harness):
        unit = uuid4()
        row_id = str(uuid4())
        delivery_id = str(uuid4())
        harness.db.pending_rows = [
            {
                "id": row_id,
                "seq": 9,
                "content": "[subagent recovery] use durable evidence",
                "turn_number": 3,
                "role": "event",
                "delivery_id": delivery_id,
                "supersedes_input_seq": 5,
            }
        ]
        harness.restored_turn_count = 4
        harness.db.claim_stateless_input_delivery = AsyncMock(
            return_value={
                "message_id": row_id,
                "seq": 9,
                "claim_generation": 4,
            }
        )

        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=12, input_seq=9, consumed_seq=5)
        )
        await _finish(harness)

        assert harness.sessions[0].turn_count == 3
        assert (
            harness.sessions[0].tool_context._stateless_subagent_recovery_active
            is False
        )
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 12, "consumed_seq": 5}
        ]

    @pytest.mark.asyncio
    async def test_recovered_interrupted_input_is_never_injected(self, harness):
        unit = uuid4()
        stopped_id = str(uuid4())
        newer_id = str(uuid4())
        claim = make_claim(unit_id=unit, token=10, input_seq=9, consumed_seq=1)
        harness.stale_result = (1, 5)
        fetch_pending = AsyncMock(
            return_value=[
                {
                    "id": newer_id,
                    "seq": 9,
                    "content": "newer",
                    "turn_number": 1,
                }
            ]
        )

        with patch.object(harness.executor, "_fetch_pending_rows", fetch_pending):
            await harness.executor._serve_claim(claim)
        await _finish(harness)

        fetch_pending.assert_awaited_once_with(str(unit), 5)
        assert harness.consumed == [{"content": "newer", "id": newer_id}]
        assert all(item["id"] != stopped_id for item in harness.consumed)
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 10, "consumed_seq": 9}
        ]

    @pytest.mark.asyncio
    async def test_attach_recovery_refreshes_consumed_watermark_before_selection(
        self, harness
    ):
        unit = uuid4()
        later_id = str(uuid4())
        # Foreground recovery during fresh attach proves input 5 already has a
        # final response and advances the DB watermark without an event.
        harness.db.refreshed_consumed_seq = 5
        fetch_pending = AsyncMock(
            return_value=[
                {
                    "id": later_id,
                    "seq": 9,
                    "content": "later input",
                    "turn_number": 2,
                }
            ]
        )

        with patch.object(harness.executor, "_fetch_pending_rows", fetch_pending):
            await harness.executor._serve_claim(
                make_claim(unit_id=unit, token=11, input_seq=9, consumed_seq=4)
            )
        await _finish(harness)

        fetch_pending.assert_awaited_once_with(str(unit), 5)
        assert harness.consumed == [{"content": "later input", "id": later_id}]
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 11, "consumed_seq": 9}
        ]

    @pytest.mark.asyncio
    async def test_fresh_restore_rewinds_pending_turn_before_admission(self, harness):
        """Admission targets the durable row even if persist never runs."""

        unit = uuid4()
        row_id = str(uuid4())
        harness.restored_turn_count = 5
        harness.db.pending_rows = [
            {
                "id": row_id,
                "seq": 17,
                "content": "stop-safe",
                "turn_number": 5,
            }
        ]

        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=7, input_seq=17, consumed_seq=16)
        )
        await _finish(harness)

        assert harness.calls["interrupt_open"] == [
            {"unit_id": unit, "lease_token": 7, "turn_id": 5}
        ]
        assert harness.calls["interrupt_start"] == [
            {"lease_token": 7, "target_turn_id": 5}
        ]
        # The harness loop deliberately never calls persist_message: this is
        # the admission-before-persist crash window itself.
        assert harness.sessions[0].turn_count == 5

    @pytest.mark.asyncio
    async def test_failed_post_open_drain_closes_stops_and_final_drains(self, harness):
        unit = uuid4()
        claim = make_claim(unit_id=unit, token=12, input_seq=1)
        harness.executor._lease.update(unit, 12)
        drain = AsyncMock(side_effect=[RuntimeError("first drain failed"), 0])

        with patch.object(pa, "_drain_thread_interrupts", drain):
            with pytest.raises(RuntimeError, match="first drain failed"):
                await harness.executor._arm_interrupt_window(
                    pa,
                    claim,
                    target_turn_id=3,
                )

        assert harness.interrupt_order == [
            "start",
            "open",
            "close",
            "stop",
        ]
        assert drain.await_args_list == [
            call(lease_token=12, target_turn_id=3),
            call(lease_token=12, target_turn_id=3),
        ]
        assert not harness.executor._lease.lost.is_set()

    @pytest.mark.asyncio
    async def test_failed_final_drain_forces_no_release(self, harness):
        unit = uuid4()
        claim = make_claim(unit_id=unit, token=12, input_seq=1)
        harness.executor._lease.update(unit, 12)
        drain = AsyncMock(side_effect=RuntimeError("database unavailable"))

        with patch.object(pa, "_drain_thread_interrupts", drain):
            with pytest.raises(RuntimeError, match="database unavailable"):
                await harness.executor._arm_interrupt_window(
                    pa,
                    claim,
                    target_turn_id=3,
                )

        assert harness.executor._lease.lost.is_set()
        await harness.executor._release(claim, reason="arm_failed")
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_no_pending_input_completes_with_fallback(self, harness):
        unit = uuid4()
        harness.db.pending_rows = []
        claim = make_claim(unit_id=unit, token=2, input_seq=9, consumed_seq=4)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        # COALESCE(input_seq, consumed_seq, 0) → input_seq.
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 2, "consumed_seq": 9}
        ]
        assert not harness.calls["release"]


# ---------------------------------------------------------------------------
# 2. Skip-if-answered
# ---------------------------------------------------------------------------


class TestSkipIfAnswered:
    @pytest.mark.asyncio
    async def test_consumed_at_or_past_input_completes_immediately(self, harness):
        unit = uuid4()
        claim = make_claim(unit_id=unit, token=4, input_seq=7, consumed_seq=7)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 4, "consumed_seq": 7}
        ]
        # No LLM path: no bundle fetch, no attach, no injection.
        assert not harness.calls["bundle"]
        assert not harness.calls["attach"]
        assert not harness.consumed
        # There is no attached session to keep warm on this no-LLM path.
        assert harness.executor._prefer_unit_id is None

    @pytest.mark.asyncio
    async def test_transcript_final_answer_completes_without_reanswering(self, harness):
        """consumed_seq < input_seq, but the transcript already holds the
        final answer for the pending input (a predecessor died in its
        turn-complete hook — the compaction crash): advance the watermark to
        that input and complete. No bundle, no attach, no LLM."""
        unit = uuid4()
        harness.db.transcript_answered_seq = 72204
        claim = make_claim(unit_id=unit, token=4, input_seq=72204, consumed_seq=72200)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 4, "consumed_seq": 72204}
        ]
        assert not harness.calls["bundle"]
        assert not harness.calls["attach"]
        assert not harness.consumed

    @pytest.mark.asyncio
    async def test_transcript_unanswered_keeps_the_ordinary_claim_path(self, harness):
        unit = uuid4()
        harness.db.transcript_answered_seq = None
        claim = make_claim(unit_id=unit, token=4, input_seq=72204, consumed_seq=72200)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["bundle"]
        assert harness.calls["attach"]

    @pytest.mark.asyncio
    async def test_transcript_answer_with_pending_event_keeps_the_claim(self, harness):
        """A pending event delivery must still run: the transcript leg never
        completes a unit that has one waiting."""
        unit = uuid4()
        harness.db.transcript_answered_seq = 72204
        harness.db.pending_rows = [
            {
                "id": "evt-1",
                "seq": 72205,
                "content": "notice",
                "role": "event",
                "delivery_id": "d-1",
            }
        ]
        claim = make_claim(unit_id=unit, token=4, input_seq=72205, consumed_seq=72200)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["bundle"]
        assert harness.calls["attach"]

    @pytest.mark.asyncio
    async def test_pending_control_bypasses_skip_and_claims_without_human_input(
        self, harness
    ):
        unit = uuid4()
        harness.db.pending_rows = []
        claim = make_claim(
            unit_id=unit,
            token=5,
            input_seq=7,
            consumed_seq=7,
            control_input_seq=2,
            control_consumed_seq=1,
        )

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["bundle"]
        assert harness.calls["attach"]
        assert harness.calls["control_start"] == [{"lease_token": 5, "agent_id": None}]
        assert harness.calls["complete"] == [
            {"unit_id": unit, "lease_token": 5, "consumed_seq": 7}
        ]


# ---------------------------------------------------------------------------
# 3. Bundle failures
# ---------------------------------------------------------------------------


class TestBundleFailures:
    @pytest.mark.asyncio
    async def test_bundle_403_releases_with_error_and_continues(self, harness):
        harness.bundle_error = ClaimBundleError(403, "token mismatch")
        claim = make_claim(token=3)

        await harness.executor._serve_claim(claim)  # must not raise
        await _finish(harness)

        assert len(harness.calls["release"]) == 1
        assert harness.calls["release"][0]["error"] is True
        assert not harness.calls["attach"]
        assert not harness.calls["complete"]

    @pytest.mark.asyncio
    async def test_bundle_404_drops_without_release(self, harness):
        harness.bundle_error = ClaimBundleError(404, "gone")
        claim = make_claim()

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert not harness.calls["release"]
        assert not harness.calls["attach"]
        assert not harness.calls["complete"]

    @pytest.mark.asyncio
    async def test_bundle_network_error_releases(self, harness):
        harness.bundle_error = ConnectionError("orchestrator down")
        claim = make_claim()

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert len(harness.calls["release"]) == 1
        assert harness.calls["release"][0]["error"] is True

    @pytest.mark.asyncio
    async def test_bundle_error_retires_warm_loop_before_release(self, harness):
        harness.bundle_error = ConnectionError("orchestrator down")
        pa._session = FakeSession(stateless_warm_reuse_safe=True)
        pa._thread_id = "previous-warm-thread"
        pa._loop_user_queue = asyncio.Queue()
        pa._loop_task = asyncio.create_task(asyncio.sleep(3600))
        harness._fake_loop_tasks.append(pa._loop_task)
        claim = make_claim()

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert pa._loop_task is None
        assert harness.disposition_order[-2:] == ["terminate", "release"]
        assert harness.calls["terminate"][-1]["reason"] == "release_bundle_error"


# ---------------------------------------------------------------------------
# 4. Turn error → release + loop continues
# ---------------------------------------------------------------------------


class TestTurnError:
    @pytest.mark.asyncio
    async def test_warm_session_detaches_before_queue_release(self, harness):
        unit = uuid4()
        claim = make_claim(unit_id=unit, token=31, input_seq=1)
        pa._session = FakeSession(stateless_warm_reuse_safe=True)
        order = []
        detach_entered = asyncio.Event()
        detach_release = asyncio.Event()

        async def detach(reason):
            order.append(("detach-start", reason))
            detach_entered.set()
            await detach_release.wait()
            pa._session = None
            order.append(("detach-done", reason))

        async def release(*_args, **_kwargs):
            order.append(("release", None))
            return "queued"

        with (
            patch.object(
                harness.executor, "_detach_cached_session", side_effect=detach
            ),
            patch.object(te, "release_unit", side_effect=release),
        ):
            releasing = asyncio.create_task(
                harness.executor._release(claim, reason="turn_error")
            )
            await asyncio.wait_for(detach_entered.wait(), timeout=2)
            assert order == [("detach-start", "release_turn_error")]
            detach_release.set()
            await asyncio.wait_for(releasing, timeout=2)

        assert order == [
            ("detach-start", "release_turn_error"),
            ("detach-done", "release_turn_error"),
            ("release", None),
        ]

    @pytest.mark.asyncio
    async def test_failed_terminate_keeps_claimant_attached_and_blocks_transition(
        self, harness
    ):
        backend = MagicMock()
        session = SimpleNamespace(
            stateless_warm_reuse_safe=False,
            cleanup=AsyncMock(),
            retire_shell_owner=MagicMock(),
            _unwrapped_backend=MagicMock(return_value=backend),
        )
        pa._session = session
        pa._thread_id = "physical-thread"

        with patch.object(
            pa,
            "_terminate_session",
            new=AsyncMock(side_effect=RuntimeError("journal drain failed")),
        ):
            with pytest.raises(RuntimeError, match="journal drain failed"):
                await harness.executor._detach_cached_session("turn_error")

        session.cleanup.assert_not_awaited()
        session.retire_shell_owner.assert_not_called()
        backend.retire.assert_not_called()
        assert pa._session is session
        assert pa._thread_id == "physical-thread"

    @pytest.mark.asyncio
    async def test_loop_death_releases_with_error(self, harness):
        harness.loop_behavior = "die"
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 3, "content": "x"}]
        claim = make_claim(unit_id=unit, token=6, input_seq=3)

        await harness.executor._serve_claim(claim)  # must not raise
        await _finish(harness)

        assert len(harness.calls["release"]) == 1
        assert harness.calls["release"][0] == {
            "unit_id": unit,
            "lease_token": 6,
            "backoff_seconds": 0.0,
            "error": True,
        }
        assert not harness.calls["park"]
        assert not harness.calls["complete"]
        # Broken session was detached, never marking the thread.
        assert harness.calls["terminate"]
        assert all(t["mark_thread"] is False for t in harness.calls["terminate"])
        assert all(t["preserve_shell"] is True for t in harness.calls["terminate"])

    @pytest.mark.asyncio
    async def test_loop_death_after_tool_effect_quiesces_and_parks(self, harness):
        harness.loop_behavior = "die_after_effect"
        unit = uuid4()
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 3, "content": "tool already ran"}
        ]
        claim = make_claim(unit_id=unit, token=7, input_seq=3)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert harness.calls["park"] == [
            {
                "unit_id": unit,
                "lease_token": 7,
            }
        ]
        assert not harness.calls["release"]
        assert not harness.calls["complete"]
        assert harness.calls["terminate"]
        assert harness.calls["terminate"][-1]["reason"] == (
            "park_loop_died_after_tool_effect"
        )
        assert not harness.has_tool_effect(claim)

    @pytest.mark.asyncio
    async def test_post_effect_loop_death_close_failure_still_parks(self, harness):
        harness.loop_behavior = "die_after_effect"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 3, "content": "tool then close failure"}
        ]
        claim = make_claim(token=8, input_seq=3)
        original_close = harness.executor._close_interrupt_window
        close_attempts = 0

        async def _fail_first_close(*args, **kwargs):
            nonlocal close_attempts
            close_attempts += 1
            if close_attempts == 1:
                raise RuntimeError("interrupt close unavailable")
            return await original_close(*args, **kwargs)

        with patch.object(
            harness.executor,
            "_close_interrupt_window",
            side_effect=_fail_first_close,
        ):
            await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert close_attempts == 2  # inner failure + claim-finally cleanup belt
        assert harness.calls["park"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 8,
            }
        ]
        assert not harness.calls["release"]
        assert not harness.calls["complete"]


class TestShutdownCancellation:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("behavior", ["interrupt_checkpoint", "hang"])
    async def test_timeout_before_tool_effect_abandons_owned_input_for_retry(
        self, harness, behavior
    ):
        harness.loop_behavior = behavior
        row = {"id": str(uuid4()), "seq": 4, "content": "unanswered"}
        harness.db.pending_rows = [row]
        claim = make_claim(token=40, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving

        deadline = asyncio.get_running_loop().time() + 2
        while not harness.calls["interrupt_open"]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.005)

        await harness.executor.stop(timeout=0)
        await _finish(harness)

        assert harness.interrupt_observations == (
            [
                {
                    "turn_id": 1,
                    "mode": "hard",
                    "target_turn_id": 1,
                    "hard_event_set": True,
                    "consumed_mode": "hard",
                }
            ]
            if behavior == "interrupt_checkpoint"
            else []
        )
        if behavior == "interrupt_checkpoint":
            assert harness.interrupt_order.index(
                "signal"
            ) < harness.interrupt_order.index("close")
        assert harness.executor._lease.lost.is_set()
        assert not harness.calls["complete"]
        assert harness.calls["release"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 40,
                "backoff_seconds": 0.0,
                "error": True,
            }
        ]
        assert not harness.calls["park"]
        assert harness.executor._shutdown_retry_claim is None
        assert harness.disposition_order[-2:] == ["terminate", "release"]
        assert harness.db.pending_rows == [row]

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("tool_inflight", "boundary"),
        [(True, "mid_tool"), (False, "post_tool")],
    )
    async def test_timeout_after_tool_effect_settles_partial_turn_once(
        self,
        harness,
        tool_inflight,
        boundary,
    ):
        harness.loop_behavior = "interrupt_checkpoint"
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": boundary}]
        claim = make_claim(token=41, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving

        deadline = asyncio.get_running_loop().time() + 2
        while not harness.calls["interrupt_open"]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.005)
        harness.mark_tool_effect(1)
        pa._tool_inflight = tool_inflight

        await harness.executor.stop(timeout=0)
        await _finish(harness)

        assert harness.interrupt_observations == [
            {
                "turn_id": 1,
                "mode": "graceful",
                "target_turn_id": 1,
                "hard_event_set": False,
                "consumed_mode": "graceful",
            }
        ]
        assert not harness.executor._lease.lost.is_set()
        assert harness.calls["complete"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 41,
                "consumed_seq": 4,
            }
        ]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_turn_closing_during_timeout_abort_cannot_complete_unanswered_input(
        self, harness
    ):
        harness.loop_behavior = "hang"
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": "closing"}]
        claim = make_claim(token=42, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving

        deadline = asyncio.get_running_loop().time() + 2
        while not harness.calls["interrupt_open"]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.005)

        original_signal = pa._signal_interrupt_for_turn

        def _close_then_signal(turn_id, **kwargs):
            pa._turn_event_open = False
            return original_signal(turn_id, **kwargs)

        with patch.object(pa, "_signal_interrupt_for_turn", _close_then_signal):
            await harness.executor.stop(timeout=0)
        await _finish(harness)

        assert harness.executor._lease.lost.is_set()
        assert pa._loop_interrupt_flag is None
        assert not harness.calls["complete"]
        # A forced task cancellation may exact-release only after fake
        # termination has quiesced the physical owner; either disposition is
        # retryable and neither advances consumed_seq.
        assert len(harness.calls["release"]) <= 1

    @pytest.mark.asyncio
    async def test_tool_latch_survives_turn_complete_cleanup_until_settlement(
        self, harness
    ):
        harness.loop_behavior = "turn_complete_cleanup"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "tool already ran"}
        ]
        claim = make_claim(token=43, input_seq=4)
        cleanup_entered = asyncio.Event()
        cleanup_release = asyncio.Event()
        abort_called = asyncio.Event()

        async def _blocked_turn_complete_body(*_args, **_kwargs):
            cleanup_entered.set()
            await cleanup_release.wait()

        original_abort = harness.executor._abort_turn_politely

        def _observe_abort(*args, **kwargs):
            result = original_abort(*args, **kwargs)
            abort_called.set()
            return result

        with (
            patch.object(
                pa,
                "_loop_on_turn_complete_body",
                side_effect=_blocked_turn_complete_body,
            ),
            patch.object(
                harness.executor,
                "_abort_turn_politely",
                side_effect=_observe_abort,
            ),
        ):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            harness.executor._task = serving
            await asyncio.wait_for(cleanup_entered.wait(), timeout=2)
            assert pa._turn_event_open is False
            assert harness.has_tool_effect(claim)

            stopping = asyncio.create_task(harness.executor.stop(timeout=0))
            await asyncio.wait_for(abort_called.wait(), timeout=2)
            assert not harness.executor._lease.lost.is_set()
            assert harness.has_tool_effect(claim)
            cleanup_release.set()
            await asyncio.wait_for(stopping, timeout=2)

        await _finish(harness)
        assert not harness.has_tool_effect(claim)
        assert harness.calls["complete"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 43,
                "consumed_seq": 4,
            }
        ]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_tool_latch_survives_error_cleanup_until_settlement(self, harness):
        harness.loop_behavior = "turn_error_cleanup"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "tool then error"}
        ]
        claim = make_claim(token=44, input_seq=4)
        error_entered = asyncio.Event()
        error_release = asyncio.Event()
        abort_called = asyncio.Event()

        async def _save_error(**_kwargs):
            error_entered.set()
            await error_release.wait()

        harness.session_postgres_conn = SimpleNamespace(save_thread_message=_save_error)

        original_abort = harness.executor._abort_turn_politely

        def _observe_abort(*args, **kwargs):
            result = original_abort(*args, **kwargs)
            abort_called.set()
            return result

        with (
            patch.object(pa, "_broadcast"),
            patch.object(
                harness.executor,
                "_abort_turn_politely",
                side_effect=_observe_abort,
            ),
        ):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            harness.executor._task = serving
            await asyncio.wait_for(error_entered.wait(), timeout=2)
            assert pa._turn_event_open is False
            assert harness.has_tool_effect(claim)

            stopping = asyncio.create_task(harness.executor.stop(timeout=0))
            await asyncio.wait_for(abort_called.wait(), timeout=2)
            assert not harness.executor._lease.lost.is_set()
            assert harness.has_tool_effect(claim)
            error_release.set()
            await asyncio.wait_for(stopping, timeout=2)

        await _finish(harness)
        assert not harness.has_tool_effect(claim)
        assert harness.calls["complete"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 44,
                "consumed_seq": 4,
            }
        ]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_tool_identity_survives_settled_hook_until_completion_cas(
        self, harness
    ):
        harness.loop_behavior = "settled_effect"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "settled but not complete"}
        ]
        claim = make_claim(token=45, input_seq=4)
        complete_entered = asyncio.Event()
        complete_release = asyncio.Event()
        abort_called = asyncio.Event()

        async def _blocked_complete(db, *, unit_id, lease_token, consumed_seq):
            complete_entered.set()
            await complete_release.wait()
            harness.calls["complete"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "consumed_seq": consumed_seq,
                }
            )
            return "done"

        original_abort = harness.executor._abort_turn_politely

        def _observe_abort(*args, **kwargs):
            result = original_abort(*args, **kwargs)
            abort_called.set()
            return result

        with (
            patch.object(te, "complete_unit", _blocked_complete),
            patch.object(
                harness.executor,
                "_abort_turn_politely",
                side_effect=_observe_abort,
            ),
        ):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            harness.executor._task = serving
            await asyncio.wait_for(complete_entered.wait(), timeout=2)
            # The settled hook has fired and its interrupt watcher is gone,
            # but Postgres has not accepted the consumed watermark yet.
            assert pa._interrupt_owner_turn_id is None
            assert harness.has_tool_effect(claim)

            stopping = asyncio.create_task(harness.executor.stop(timeout=0))
            await asyncio.wait_for(abort_called.wait(), timeout=2)
            assert not harness.executor._lease.lost.is_set()
            assert harness.has_tool_effect(claim)
            complete_release.set()
            await asyncio.wait_for(stopping, timeout=2)

        await _finish(harness)
        assert not harness.has_tool_effect(claim)
        assert harness.calls["complete"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 45,
                "consumed_seq": 4,
            }
        ]
        assert not harness.calls["park"]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_executor_identity_survives_non_warm_detach_until_completion_cas(
        self, harness
    ):
        harness.loop_behavior = "settled_effect"
        harness.stateless_warm_reuse_safe = False
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "physical tool settled"}
        ]
        claim = make_claim(token=451, input_seq=4)
        complete_entered = asyncio.Event()
        complete_release = asyncio.Event()
        abort_called = asyncio.Event()

        async def _blocked_complete(db, *, unit_id, lease_token, consumed_seq):
            complete_entered.set()
            await complete_release.wait()
            harness.calls["complete"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "consumed_seq": consumed_seq,
                }
            )
            return "done"

        original_abort = harness.executor._abort_turn_politely

        def _observe_abort(*args, **kwargs):
            result = original_abort(*args, **kwargs)
            abort_called.set()
            return result

        with (
            patch.object(te, "complete_unit", _blocked_complete),
            patch.object(
                harness.executor,
                "_abort_turn_politely",
                side_effect=_observe_abort,
            ),
        ):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            harness.executor._task = serving
            await asyncio.wait_for(complete_entered.wait(), timeout=2)

            # Physical teardown deliberately erased every PA session global,
            # but the executor's exact claimant identity remains monotonic
            # until Postgres accepts the queue disposition.
            assert pa._session is None
            assert not harness.has_tool_effect(claim)
            assert harness.executor._claim_crossed_tool_effect(pa, claim=claim)

            stopping = asyncio.create_task(harness.executor.stop(timeout=0))
            await asyncio.wait_for(abort_called.wait(), timeout=2)
            assert not harness.executor._lease.lost.is_set()
            assert harness.executor._claim_crossed_tool_effect(pa, claim=claim)
            complete_release.set()
            await asyncio.wait_for(stopping, timeout=2)

        await _finish(harness)
        assert harness.executor._tool_effect_identity is None
        assert harness.calls["complete"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 451,
                "consumed_seq": 4,
            }
        ]
        assert not harness.calls["park"]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_non_warm_post_effect_cancel_releases_when_completion_hangs(
        self, harness, monkeypatch
    ):
        """Step 4a branch (1): the turn settled (answer durable, consumed_seq
        checkpointed by the interrupt close), so a forced shutdown re-attempts
        the completion CAS — bounded — and, when the DB hangs, releases under
        the checkpoint instead of parking an already-answered turn. The
        successor finds nothing left to consume (skip-if-answered)."""
        harness.loop_behavior = "settled_effect"
        harness.stateless_warm_reuse_safe = False
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "physical tool ambiguous"}
        ]
        claim = make_claim(token=452, input_seq=4)
        complete_entered = asyncio.Event()
        complete_calls = 0

        async def _uncooperative_complete(db, *, unit_id, lease_token, consumed_seq):
            nonlocal complete_calls
            del db, unit_id, lease_token, consumed_seq
            complete_calls += 1
            complete_entered.set()
            await asyncio.sleep(3600)

        monkeypatch.setattr(te, "SHUTDOWN_COMPLETE_TIMEOUT_SECONDS", 0.2)
        with patch.object(te, "complete_unit", _uncooperative_complete):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            harness.executor._task = serving
            await asyncio.wait_for(complete_entered.wait(), timeout=2)

            assert pa._session is None
            assert not harness.has_tool_effect(claim)
            assert harness.executor._claim_crossed_tool_effect(pa, claim=claim)

            # Completion stays blocked beyond abort grace. Forced cancellation
            # re-attempts it once, bounded, then releases: the checkpoint
            # already protects the answer, so a replay is impossible.
            await harness.executor.stop(timeout=0)

        await _finish(harness)
        assert complete_calls == 2  # serve-path attempt + one bounded shutdown retry
        assert harness.calls["release"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 452,
                "backoff_seconds": 0.0,
                "error": True,
            }
        ]
        assert not harness.calls["park"]
        assert not harness.calls["complete"]
        assert harness.executor._tool_effect_identity is None
        assert harness.disposition_order[-2:] == ["terminate", "release"]

    @pytest.mark.asyncio
    async def test_uncooperative_post_effect_shutdown_parks_without_retry(
        self, harness
    ):
        harness.loop_behavior = "hang"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "tool may have written"}
        ]
        claim = make_claim(token=46, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving

        deadline = asyncio.get_running_loop().time() + 2
        while not harness.calls["interrupt_open"]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.005)
        harness.mark_tool_effect(1)

        await harness.executor.stop(timeout=0)
        await _finish(harness)

        assert harness.calls["park"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 46,
            }
        ]
        assert not harness.calls["complete"]
        assert not harness.calls["release"]
        assert not harness.has_tool_effect(claim)
        assert harness.disposition_order[-2:] == ["terminate", "park"]

    @pytest.mark.asyncio
    async def test_settled_post_effect_completion_failure_releases_under_checkpoint(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "settled_effect"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "tool completed"}
        ]
        claim = make_claim(token=47, input_seq=4)
        completion = AsyncMock(side_effect=RuntimeError("database unavailable"))
        monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(te, "complete_unit", completion)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        completion.assert_awaited_once_with(
            harness.db,
            unit_id=claim.unit_id,
            lease_token=47,
            consumed_seq=4,
        )
        assert not harness.calls["park"]
        assert harness.calls["release"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 47,
                "backoff_seconds": 0.0,
                "error": True,
            }
        ]
        assert not harness.has_tool_effect(claim)
        assert harness.disposition_order[-2:] == ["terminate", "release"]

    @pytest.mark.asyncio
    async def test_pre_effect_completion_failure_quiesces_and_stops_claiming(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "complete"
        harness.stateless_warm_reuse_safe = True
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "answer once"}
        ]
        claim = make_claim(token=471, input_seq=4)
        completion = AsyncMock(side_effect=RuntimeError("database unavailable"))
        claim_calls = 0

        async def _claim_once(*_args, **_kwargs):
            nonlocal claim_calls
            claim_calls += 1
            if claim_calls > 1:
                raise AssertionError("executor claimed a successor")
            return claim

        push_task = asyncio.create_task(asyncio.sleep(0))
        await push_task
        pa._pending_cloud_push_task = push_task
        harness.executor._worker_enabled = False
        monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(te, "complete_unit", completion)
        monkeypatch.setattr(te, "claim_unit", _claim_once)

        await asyncio.wait_for(harness.executor.run(), timeout=2)

        completion.assert_awaited_once_with(
            harness.db,
            unit_id=claim.unit_id,
            lease_token=471,
            consumed_seq=4,
        )
        assert harness.calls["interrupt_close"][-1]["completed_input_seq"] == 4
        assert claim_calls == 1
        assert harness.executor._stop.is_set()
        # The fake close records the successful consumed checkpoint above;
        # this list stays empty because no complete queue transition landed.
        assert not harness.calls["complete"]
        assert harness.calls["release"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 471,
                "backoff_seconds": 0.0,
                "error": True,
            }
        ]
        assert not harness.calls["park"]
        assert harness.calls["terminate"][-1]["reason"] == (
            "release_completion_cas_failed"
        )
        assert all(task.done() for task in harness._fake_loop_tasks)
        assert pa._pending_cloud_push_task is None
        assert pa._session is None
        assert harness.executor._prefer_unit_id is None
        assert harness.executor._warm_since is None
        assert harness.executor._tool_effect_identity is None
        assert not harness.has_tool_effect(claim)
        await _finish(harness)

    @pytest.mark.asyncio
    async def test_post_effect_turn_close_failure_parks_before_completion(
        self, harness
    ):
        harness.loop_behavior = "settled_effect"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "tool then close failure"}
        ]
        claim = make_claim(token=48, input_seq=4)
        original_close = harness.executor._close_interrupt_window
        close_attempts = 0

        async def _fail_first_close(*args, **kwargs):
            nonlocal close_attempts
            close_attempts += 1
            if close_attempts == 1:
                raise RuntimeError("interrupt close unavailable")
            return await original_close(*args, **kwargs)

        with patch.object(
            harness.executor,
            "_close_interrupt_window",
            side_effect=_fail_first_close,
        ):
            await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert close_attempts == 2
        assert harness.calls["park"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 48,
            }
        ]
        assert not harness.calls["complete"]
        assert not harness.calls["release"]

    @pytest.mark.asyncio
    async def test_run_stops_without_serve_crash_release_when_post_effect_park_fails(
        self, harness, monkeypatch
    ):
        claim = make_claim(token=48, input_seq=4)
        park = AsyncMock(side_effect=RuntimeError("database unavailable"))
        release_belt = AsyncMock()
        claims = 0

        async def _claim_once(*_args, **_kwargs):
            nonlocal claims
            claims += 1
            assert claims == 1
            return claim

        async def _serve_with_failed_park(served_claim):
            assert served_claim is claim
            await harness.executor._park_post_effect_claim(
                pa,
                served_claim,
                reason="test_exhaustion",
            )

        harness.executor._worker_enabled = False
        monkeypatch.setattr(te, "COMPLETE_RETRY_ATTEMPTS", 1)
        monkeypatch.setattr(te, "claim_unit", _claim_once)
        monkeypatch.setattr(te, "park_unit", park)
        monkeypatch.setattr(harness.executor, "_serve_claim", _serve_with_failed_park)
        monkeypatch.setattr(harness.executor, "_release", release_belt)

        run_task = asyncio.create_task(harness.executor.run())
        await asyncio.wait_for(run_task, timeout=2)

        park.assert_awaited_once_with(
            harness.db,
            unit_id=claim.unit_id,
            lease_token=48,
            reason="test_exhaustion",
        )
        release_belt.assert_not_awaited()
        assert harness.executor._stop.is_set()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("tool_effect", [False, True])
    async def test_run_generic_crash_releases_only_before_tool_effect(
        self, harness, monkeypatch, tool_effect
    ):
        claim = make_claim(token=49, input_seq=4)
        release_belt = AsyncMock()
        claims = 0

        async def _claim_once(*_args, **_kwargs):
            nonlocal claims
            claims += 1
            assert claims == 1
            return claim

        async def _unexpected_crash(served_claim):
            assert served_claim is claim
            harness.executor._lease.update(claim.unit_id, claim.lease_token)
            pa._thread_id = str(claim.unit_id)
            pa._session = FakeSession()
            pa._loop_user_queue = asyncio.Queue()
            if tool_effect:
                pa._turn_tool_execution_identity = (
                    str(claim.unit_id),
                    claim.lease_token,
                    1,
                )
            harness.executor.request_stop()
            raise RuntimeError("unexpected serve failure")

        harness.executor._worker_enabled = False
        monkeypatch.setattr(te, "claim_unit", _claim_once)
        monkeypatch.setattr(harness.executor, "_serve_claim", _unexpected_crash)
        monkeypatch.setattr(harness.executor, "_release", release_belt)

        await asyncio.wait_for(
            asyncio.create_task(harness.executor.run()),
            timeout=2,
        )

        if tool_effect:
            release_belt.assert_not_awaited()
            assert harness.calls["park"] == [
                {
                    "unit_id": claim.unit_id,
                    "lease_token": 49,
                }
            ]
            assert harness.calls["terminate"][-1]["reason"] == (
                "park_serve_crash_after_tool_effect"
            )
        else:
            release_belt.assert_awaited_once_with(claim, reason="serve_crash")
            assert not harness.calls["park"]

    def test_shutdown_abort_target_requires_thread_and_lease_owner(self, harness):
        unit = uuid4()
        harness.executor._lease.update(unit, 9)
        pa._thread_id = str(unit)
        pa._interrupt_owner_lease_token = 9
        pa._interrupt_owner_turn_id = 3

        assert harness.executor._owned_abort_target(pa) == 3
        pa._interrupt_owner_lease_token = 10
        assert harness.executor._owned_abort_target(pa) is None
        pa._interrupt_owner_lease_token = 9
        pa._thread_id = str(uuid4())
        assert harness.executor._owned_abort_target(pa) is None

    def test_stale_tool_effect_handoff_fails_closed_and_new_claim_resets(self, harness):
        first = make_claim(token=20)
        second = make_claim(token=21)
        harness.executor._activate_lease(second.unit_id, second.lease_token)

        with pytest.raises(RuntimeError, match="does not match"):
            harness.executor._record_claim_tool_effect(
                first,
                (str(first.unit_id), first.lease_token, 1),
            )

        assert harness.executor._lease.lost.is_set()
        assert harness.executor._tool_effect_identity is None

        harness.executor._activate_lease(first.unit_id, first.lease_token)
        assert not harness.executor._lease.lost.is_set()
        harness.executor._record_claim_tool_effect(
            first,
            (str(first.unit_id), first.lease_token, 1),
        )
        assert harness.executor._tool_effect_identity == (
            str(first.unit_id),
            first.lease_token,
            1,
        )

        harness.executor._activate_lease(second.unit_id, second.lease_token)
        assert harness.executor._tool_effect_identity is None

    @pytest.mark.asyncio
    async def test_cancelled_stalled_bundle_quiesces_then_exact_releases(self, harness):
        entered = asyncio.Event()

        async def _blocked_bundle(_unit_id, _token):
            entered.set()
            await asyncio.sleep(3600)

        claim = make_claim(token=41)
        pa._session = FakeSession(stateless_warm_reuse_safe=True)
        pa._thread_id = "prior-warm-thread"
        pa._loop_user_queue = asyncio.Queue()
        pa._loop_task = asyncio.create_task(asyncio.sleep(3600))
        harness._fake_loop_tasks.append(pa._loop_task)

        with patch.object(harness.executor, "_fetch_bundle", _blocked_bundle):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            await asyncio.wait_for(entered.wait(), timeout=2)
            serving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await serving

        assert harness.disposition_order[-2:] == ["terminate", "release"]
        assert harness.calls["terminate"][-1]["reason"] == ("shutdown_cancelled_claim")
        assert harness.calls["release"][-1]["lease_token"] == 41

    @pytest.mark.asyncio
    async def test_cancelled_stalled_turn_detaches_before_queue_release(self, harness):
        harness.loop_behavior = "hang"
        harness.stateless_warm_reuse_safe = False
        row_id = str(uuid4())
        claim = make_claim(token=42, input_seq=4)
        harness.db.pending_rows = [{"id": row_id, "seq": 4, "content": "blocked"}]

        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        deadline = asyncio.get_running_loop().time() + 2
        while not harness.calls["interrupt_open"]:
            assert asyncio.get_running_loop().time() < deadline
            await asyncio.sleep(0.01)
        serving.cancel()
        with pytest.raises(asyncio.CancelledError):
            await serving
        await _finish(harness)

        assert harness.calls["terminate"]
        assert harness.calls["terminate"][-1]["mark_thread"] is False
        assert harness.calls["release"] == [
            {
                "unit_id": claim.unit_id,
                "lease_token": 42,
                "backoff_seconds": 0.0,
                "error": True,
            }
        ]
        assert not harness.calls["complete"]

    @pytest.mark.asyncio
    async def test_cancelled_claim_acks_exact_loss_when_end_won_release(self, harness):
        entered = asyncio.Event()

        async def _blocked_bundle(_unit_id, _token):
            entered.set()
            await asyncio.sleep(3600)

        claim = make_claim(token=43)
        ack = AsyncMock(return_value=True)
        with (
            patch.object(harness.executor, "_fetch_bundle", _blocked_bundle),
            patch.object(te, "release_unit", new=AsyncMock(return_value=None)),
            patch.object(harness.executor, "_ack_terminal_claim_loss", ack),
        ):
            serving = asyncio.create_task(harness.executor._serve_claim(claim))
            await asyncio.wait_for(entered.wait(), timeout=2)
            serving.cancel()
            with pytest.raises(asyncio.CancelledError):
                await serving

        ack.assert_awaited_once_with(claim)

    @pytest.mark.asyncio
    async def test_attach_failure_releases_and_clears(self, harness):
        harness.attach_error = RuntimeError("workspace exploded")
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "x"}]
        claim = make_claim(unit_id=unit, input_seq=1)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        # Step 2: the attach path releases through record_attach_failure
        # (counted, signed, backed off), never the plain error release.
        assert not harness.calls["release"]
        assert len(harness.calls["attach_failure"]) == 1
        failure = harness.calls["attach_failure"][0]
        assert (
            failure["unit_id"] == unit and failure["lease_token"] == claim.lease_token
        )
        assert failure["signature"].startswith("RuntimeError:workspace exploded")
        assert failure["error"] == "workspace exploded"
        assert failure["backoff_seconds"] == te.attach_failure_backoff_seconds(
            claim.attempts_since_completion
        )
        assert not harness.calls["journal"]  # re-queued: nothing to tell the user
        assert pa._thread_id is None
        assert not harness.calls["complete"]

    @pytest.mark.asyncio
    async def test_attach_failure_park_journals_turn_parked(self, harness):
        harness.attach_error = RuntimeError(
            "subagent session-terminalize failed (HTTP 400)"
        )
        harness.attach_failure_state = "parked"
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "x"}]
        claim = make_claim(unit_id=unit, input_seq=1)

        await harness.executor._serve_claim(claim)
        await _finish(harness)

        assert len(harness.calls["attach_failure"]) == 1
        assert harness.calls["journal"] == [
            {
                "thread_id": str(unit),
                "kind": "turn.parked",
                "payload": {
                    "reason": "attach_failed",
                    "attempts": 1,
                    "retryable": True,
                    "parked_by": "test-pod",
                    "error": "subagent session-terminalize failed (HTTP 400)",
                },
            }
        ]
        assert pa._thread_id is None


# ---------------------------------------------------------------------------
# 5. Heartbeat lease-lost → polite abort, NO release
# ---------------------------------------------------------------------------


class TestLeaseLost:
    @pytest.mark.asyncio
    async def test_loss_during_bundle_cannot_be_erased_by_handle_update(
        self, harness, monkeypatch
    ):
        monkeypatch.setattr(te, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
        harness.heartbeat_result = None
        prior_unit = uuid4()
        harness.executor._lease.update(prior_unit, 4)
        prior_lost_event = harness.executor._lease.lost
        entered = asyncio.Event()
        release = asyncio.Event()

        async def _blocked_bundle(_unit_id, _token):
            entered.set()
            await release.wait()
            return {
                "attach": harness.attach_for(str(_unit_id)),
                "watermarks": {},
            }

        claim = make_claim(unit_id=uuid4(), token=9, input_seq=2)
        with patch.object(
            harness.executor, "_fetch_bundle", side_effect=_blocked_bundle
        ):
            serve = asyncio.create_task(harness.executor._serve_claim(claim))
            await asyncio.wait_for(entered.wait(), timeout=2)
            deadline = asyncio.get_running_loop().time() + 2
            while not harness.calls["heartbeat"]:
                assert asyncio.get_running_loop().time() < deadline
                await asyncio.sleep(0.01)
            release.set()
            await asyncio.wait_for(serve, timeout=2)

        assert harness.executor._lease.unit_id == str(prior_unit)
        assert harness.executor._lease.lease_token == 4
        assert harness.executor._lease.lost is prior_lost_event
        assert not prior_lost_event.is_set()
        assert not harness.calls["attach"]
        assert not harness.calls["release"]
        assert not harness.calls["complete"]

    @pytest.mark.asyncio
    async def test_heartbeat_loss_aborts_without_release(self, harness, monkeypatch):
        monkeypatch.setattr(te, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
        harness.heartbeat_result = None  # renewal finds no leased row
        harness.loop_behavior = "hang"  # the turn never completes on its own
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "hi"}]
        claim = make_claim(unit_id=unit, token=9, input_seq=2)

        await asyncio.wait_for(harness.executor._serve_claim(claim), timeout=5.0)
        await _finish(harness)

        # Polite abort: interrupt signalled, session discarded, and neither
        # complete_unit nor release_unit was called — the lease is gone.
        assert not harness.calls["release"]
        assert not harness.calls["complete"]
        assert pa._loop_interrupt_flag in ("hard", "graceful")
        assert harness.calls["terminate"]
        assert harness.calls["terminate"][-1]["mark_thread"] is False
        assert harness.calls["terminate"][-1]["preserve_shell"] is True
        # Affinity dropped: the discarded session must not be preferred.
        assert harness.executor._prefer_unit_id is None
        assert harness.executor._attached_fingerprint is None

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("tool_inflight", "expected_mode", "hard_event_set"),
        [
            (True, "graceful", False),
            (False, "hard", True),
        ],
    )
    async def test_heartbeat_loss_scopes_abort_to_exact_active_turn(
        self,
        harness,
        monkeypatch,
        tool_inflight,
        expected_mode,
        hard_event_set,
    ):
        """Lease loss reaches the real turn checkpoint with scoped semantics.

        The graceful case models a tool completing and polling the interrupt;
        the hard case models the pre-provider/blocked-await edge and proves the
        hard wake is armed as well as the cooperative checkpoint.
        """

        monkeypatch.setattr(te, "HEARTBEAT_INTERVAL_SECONDS", 0.01)
        harness.heartbeat_result = None
        harness.loop_behavior = "interrupt_checkpoint"
        pa._tool_inflight = tool_inflight
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "hi"}]
        claim = make_claim(unit_id=unit, token=10, input_seq=2)

        await asyncio.wait_for(harness.executor._serve_claim(claim), timeout=5.0)
        await _finish(harness)

        assert harness.interrupt_observations == [
            {
                "turn_id": 1,
                "mode": expected_mode,
                "target_turn_id": 1,
                "hard_event_set": hard_event_set,
                "consumed_mode": expected_mode,
            }
        ]
        assert not harness.calls["release"]
        assert not harness.calls["complete"]
        assert harness.calls["terminate"][-1]["mark_thread"] is False

    def test_abort_refuses_missing_or_stale_turn_identity(self, harness):
        pa._session = FakeSession()
        pa._session.turn_count = 3
        pa._turn_event_open = True

        assert harness.executor._abort_turn_politely(pa, target_turn_id=None) is False
        assert harness.executor._abort_turn_politely(pa, target_turn_id=2) is False
        assert pa._loop_interrupt_flag is None
        assert pa._loop_interrupt_target_turn_id is None
        assert not pa._hard_interrupt_event.is_set()


# ---------------------------------------------------------------------------
# 6. strip_restored_pending_humans
# ---------------------------------------------------------------------------


class TestStripRestoredPending:
    def _msgs(self):
        from langchain_core.messages import AIMessage, HumanMessage

        return [
            HumanMessage(content="q1", id="a"),
            AIMessage(content="a1", id="b"),
            HumanMessage(content="pending-1", id="fresh-uuid-1"),
            HumanMessage(content="pending-2", id="fresh-uuid-2"),
        ]

    def test_removes_trailing_pending_by_content(self):
        msgs = self._msgs()
        pending = [
            {"id": "row-1", "seq": 10, "content": "pending-1"},
            {"id": "row-2", "seq": 11, "content": "pending-2"},
        ]
        removed = te.strip_restored_pending_humans(msgs, pending)
        assert removed == 2
        assert [m.content for m in msgs] == ["q1", "a1"]

    def test_removes_by_id_when_restore_preserves_ids(self):
        from langchain_core.messages import HumanMessage

        msgs = [HumanMessage(content="anything", id="row-9")]
        pending = [{"id": "row-9", "seq": 4, "content": "different text"}]
        removed = te.strip_restored_pending_humans(msgs, pending)
        assert removed == 1
        assert msgs == []

    def test_non_trailing_human_untouched(self):
        from langchain_core.messages import AIMessage, HumanMessage

        msgs = [
            HumanMessage(content="pending-1", id="x"),
            AIMessage(content="answer", id="y"),
        ]
        pending = [{"id": "row-1", "seq": 10, "content": "pending-1"}]
        removed = te.strip_restored_pending_humans(msgs, pending)
        assert removed == 0
        assert len(msgs) == 2

    def test_pending_event_excluded_from_restore_does_not_block_human_strip(self):
        from langchain_core.messages import HumanMessage

        # Unadmitted ledger rows are excluded from passive restore. The event
        # still appears in the executor's merged pending query after the human;
        # it must not become an unmatched tail sentinel that leaves the human
        # duplicated in memory when the executor injects it.
        msgs = [HumanMessage(content="pending-1", id="p1")]
        pending = [
            {"id": "row-1", "seq": 10, "content": "pending-1", "role": "human"},
            {
                "id": "event-row",
                "seq": 11,
                "content": "[wake] job finished",
                "role": "event",
                "delivery_id": "delivery",
            },
        ]
        removed = te.strip_restored_pending_humans(msgs, pending)
        assert removed == 1
        assert msgs == []

    def test_empty_inputs_are_noops(self):
        assert te.strip_restored_pending_humans([], [{"id": "a"}]) == 0
        from langchain_core.messages import HumanMessage

        msgs = [HumanMessage(content="x", id="a")]
        assert te.strip_restored_pending_humans(msgs, []) == 0
        assert len(msgs) == 1


# ---------------------------------------------------------------------------
# 7. Scrub-on-claim acceptance (§5.6 — deliverable D)
# ---------------------------------------------------------------------------


class TestScrubOnClaim:
    def test_tenant_a_then_no_env_keys_leaves_no_residue(self, monkeypatch):
        from shared.runtime.services import embedding_service as emb

        # Tenant A attach: env keys land, singleton would be rebuilt lazily.
        pa._apply_session_embedding_env(
            {
                "EMBEDDING_PROVIDER": "openai",
                "EMBEDDING_MODEL": "tenant-a-model",
                "EMBEDDING_BASE_URL": "https://a.example",
                "EMBEDDING_API_KEY": "sk-tenant-a",
                "RERANK_MODEL": "tenant-a-rerank",
                "RERANK_BASE_URL": "https://a-rerank.example",
                "RERANK_API_KEY": "sk-tenant-a-rerank",
                "KB_EMBEDDING_MODEL": "kb-a",
                "KB_EMBEDDING_API_KEY": "sk-kb-a",
            }
        )
        assert os.environ.get("EMBEDDING_API_KEY") == "sk-tenant-a"
        assert os.environ.get("RERANK_API_KEY") == "sk-tenant-a-rerank"
        assert os.environ.get("KB_EMBEDDING_MODEL") == "kb-a"
        emb._embedding_service = object()  # simulate a built singleton

        # Tenant B attach with env_keys ABSENT: no A values anywhere,
        # singleton None.
        pa._apply_session_embedding_env(None)
        for key in pa.MEMORY_EMBEDDING_ENV_KEYS:
            assert key not in os.environ, key
        for key in emb.KB_EMBEDDING_ENV_KEYS:
            assert key not in os.environ, key
        assert emb._embedding_service is None
        assert emb._kb_embedding_service is None

        # Cleanup safety: nothing to restore — the helper popped everything.

    def test_partial_override_replaces_not_merges(self, monkeypatch):
        from shared.runtime.services import embedding_service as emb

        pa._apply_session_embedding_env(
            {"EMBEDDING_MODEL": "a-model", "EMBEDDING_API_KEY": "sk-a"}
        )
        # Tenant B supplies only a model: A's key must NOT survive.
        pa._apply_session_embedding_env({"EMBEDDING_MODEL": "b-model"})
        assert os.environ.get("EMBEDDING_MODEL") == "b-model"
        assert "EMBEDDING_API_KEY" not in os.environ
        assert emb._embedding_service is None
        pa._apply_session_embedding_env(None)  # cleanup

    def test_executor_scrub_clears_dual_inboxes(self, harness):
        import agent.api.dual_app as dual_app

        dual_app._guidance_inbox["job-1"] = [{"id": "g1"}]
        dual_app._reply_inbox["job-1"] = [{"id": "r1"}]
        harness.executor._scrub_process_residue()
        assert dual_app._guidance_inbox == {}
        assert dual_app._reply_inbox == {}


# ---------------------------------------------------------------------------
# 8. Affinity
# ---------------------------------------------------------------------------


class TestAffinity:
    @pytest.mark.parametrize(
        ("supports_shell", "expected"),
        ((False, True), (True, False)),
    )
    def test_persistent_session_reuse_capability_follows_backend(
        self, supports_shell, expected
    ):
        from agent.api.persistent_session import PersistentSession

        session = object.__new__(PersistentSession)
        session.workspace_manager = SimpleNamespace(
            backend=SimpleNamespace(supports_shell=supports_shell)
        )

        assert session.stateless_warm_reuse_safe is expected

    def test_fingerprint_ignores_only_inbox_owned_interactive_scalars(self):
        base = {
            "thread_id": "t1",
            "resolved_config": {
                "resolved_at": "first",
                "agent": {
                    "interactive": {
                        "permission_mode": "supervised",
                        "narration_mode": "auto",
                        "idle_timeout_minutes": 30,
                    },
                    "llm": {"model": "m"},
                },
            },
        }
        changed_controls = {
            "thread_id": "t1",
            "resolved_config": {
                "resolved_at": "second",
                "agent": {
                    "interactive": {
                        "permission_mode": "autonomous",
                        "narration_mode": "verbose",
                        "idle_timeout_minutes": 30,
                    },
                    "llm": {"model": "m"},
                },
            },
        }
        changed_runtime_config = {
            **changed_controls,
            "resolved_config": {
                **changed_controls["resolved_config"],
                "agent": {
                    **changed_controls["resolved_config"]["agent"],
                    "interactive": {
                        **changed_controls["resolved_config"]["agent"]["interactive"],
                        "idle_timeout_minutes": 60,
                    },
                },
            },
        }

        assert te.attach_fingerprint(base) == te.attach_fingerprint(changed_controls)
        assert te.attach_fingerprint(base) != te.attach_fingerprint(
            changed_runtime_config
        )

    def test_fingerprint_ignores_fallback_control_scalars(self):
        first = {
            "thread_id": "t1",
            "config_override": {
                "interactive": {
                    "permission_mode": "supervised",
                    "narration_mode": "auto",
                }
            },
        }
        second = {
            "thread_id": "t1",
            "config_override": {
                "interactive": {
                    "permission_mode": "auto_accept",
                    "narration_mode": "silent",
                }
            },
        }
        assert te.attach_fingerprint(first) == te.attach_fingerprint(second)

    def test_fingerprint_changes_with_conversation_revision_and_event_epoch(self):
        base = {
            "thread_id": "t1",
            "conversation_revision": 0,
            "events_epoch": 7,
            "resolved_config": {"agent": {"llm": {"model": "m"}}},
        }
        rewound = {**base, "conversation_revision": 1, "events_epoch": 8}
        epoch_only = {**base, "events_epoch": 8}

        assert te.attach_fingerprint(base) != te.attach_fingerprint(rewound)
        assert te.attach_fingerprint(base) != te.attach_fingerprint(epoch_only)

    def test_fingerprint_ignores_rotating_runtime_actor_credentials_only(self):
        first = {
            "thread_id": "t1",
            "runtime_actor": {
                "caller_kind": "human",
                "user_id": "u1",
                "project_id": "p1",
                "project_role": "member",
                "thread_id": "t1",
                "officer_incarnation": None,
                "access_credential": "access-a",
                "refresh_credential": "refresh-a",
                "access_expires_at": "2026-09-02T18:00:00Z",
                "refresh_expires_at": "2026-09-03T18:00:00Z",
            },
        }
        rotated = {
            "thread_id": "t1",
            "runtime_actor": {
                **first["runtime_actor"],
                "access_credential": "access-b",
                "refresh_credential": "refresh-b",
                "access_expires_at": "2026-09-02T18:05:00Z",
                "refresh_expires_at": "2026-09-03T18:05:00Z",
            },
        }
        changed_identity = {
            "thread_id": "t1",
            "runtime_actor": {
                **rotated["runtime_actor"],
                "project_role": "admin",
            },
        }

        assert te.attach_fingerprint(first) == te.attach_fingerprint(rotated)
        assert te.attach_fingerprint(first) != te.attach_fingerprint(changed_identity)

    @pytest.mark.asyncio
    async def test_lite_same_thread_same_fingerprint_skips_reattach(self, harness):
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "one"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )
        assert len(harness.calls["attach"]) == 1

        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "two"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2)
        )
        await _finish(harness)

        # Reused: no second attach, no detach in between.
        assert len(harness.calls["attach"]) == 1
        assert not harness.calls["terminate"]
        # The lease handle was repointed at the NEW claim's token.
        assert harness.executor._lease.lease_token == 2
        assert len(harness.calls["complete"]) == 2
        assert harness.calls["shell_owner_token"] == [1, 2]
        assert harness.sessions[0].shell_owner_tokens == [1, 2]
        assert [call["turn_id"] for call in harness.calls["interrupt_open"]] == [
            1,
            2,
        ]

    @pytest.mark.asyncio
    async def test_warm_attach_consumes_event_once_under_new_lease(self, harness):
        unit = uuid4()
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 1, "content": "human", "turn_number": 1}
        ]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )

        row_id = str(uuid4())
        delivery_id = str(uuid4())
        harness.db.pending_rows = [
            {
                "id": row_id,
                "seq": 2,
                "content": "event",
                "turn_number": 2,
                "role": "event",
                "delivery_id": delivery_id,
            }
        ]
        claim_delivery = AsyncMock(
            return_value={
                "message_id": row_id,
                "seq": 2,
                "claim_generation": 4,
            }
        )
        harness.db.claim_stateless_input_delivery = claim_delivery
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2, consumed_seq=1)
        )
        await _finish(harness)

        assert len(harness.calls["attach"]) == 1
        assert [item.get("role", "human") for item in harness.consumed] == [
            "human",
            "event",
        ]
        claim_delivery.assert_awaited_once()
        assert harness.calls["complete"][-1]["consumed_seq"] == 2

    @pytest.mark.asyncio
    async def test_warm_reuse_refuses_divergent_durable_turn_identity(self, harness):
        unit = uuid4()
        harness.db.pending_rows = [
            {
                "id": str(uuid4()),
                "seq": 1,
                "content": "one",
                "turn_number": 1,
            }
        ]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )
        assert pa._session is not None
        pa._session.turn_count = 7
        harness.db.pending_rows = [
            {
                "id": str(uuid4()),
                "seq": 2,
                "content": "must-not-run",
                "turn_number": 2,
            }
        ]

        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2, consumed_seq=1)
        )
        await _finish(harness)

        assert len(harness.consumed) == 1
        assert len(harness.calls["interrupt_open"]) == 1
        assert harness.calls["release"][-1]["lease_token"] == 2

    @pytest.mark.asyncio
    async def test_shell_backend_reattaches_before_next_lease_token(self, harness):
        unit = uuid4()
        harness.stateless_warm_reuse_safe = False
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "one"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )
        first_session = harness.sessions[0]
        # Physical ownership is retired while claim 1 is still exclusive,
        # before the queue completion transition.
        assert pa._session is None

        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "two"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2)
        )
        await _finish(harness)

        assert len(harness.calls["attach"]) == 2
        assert harness.calls["terminate"] == [
            {
                "reason": "turn_complete",
                "mark_thread": False,
                "preserve_shell": True,
                "preserve_workspace_daemons": True,
            },
            {
                "reason": "turn_complete",
                "mark_thread": False,
                "preserve_shell": True,
                "preserve_workspace_daemons": True,
            },
        ]
        assert first_session is harness.sessions[0]
        assert pa._session is None
        assert first_session is not harness.sessions[1]
        assert harness.sessions[0].shell_owner_tokens == [1]
        assert harness.sessions[1].shell_owner_tokens == [2]

    @pytest.mark.asyncio
    async def test_control_scalar_bundle_change_reuses_warm_session(self, harness):
        unit = uuid4()
        base = harness.attach_for(str(unit))
        first = {
            **base,
            "resolved_config": {
                **base["resolved_config"],
                "agent": {
                    **base["resolved_config"]["agent"],
                    "interactive": {
                        "permission_mode": "supervised",
                        "narration_mode": "auto",
                    },
                },
            },
        }
        harness.set_attach(str(unit), first)
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "one"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )

        second = {
            **first,
            "resolved_config": {
                **first["resolved_config"],
                "agent": {
                    **first["resolved_config"]["agent"],
                    "interactive": {
                        "permission_mode": "autonomous",
                        "narration_mode": "verbose",
                    },
                },
            },
        }
        harness.set_attach(str(unit), second)
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "two"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2)
        )
        await _finish(harness)

        assert len(harness.calls["attach"]) == 1
        assert not harness.calls["terminate"]

    @pytest.mark.asyncio
    async def test_different_thread_detaches_then_attaches(self, harness):
        unit_a, unit_b = uuid4(), uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "a"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit_a, token=1, input_seq=1)
        )
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "b"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit_b, token=1, input_seq=1)
        )
        await _finish(harness)

        assert len(harness.calls["attach"]) == 2
        assert harness.calls["attach"][1]["thread_id"] == str(unit_b)
        # Old session detached first, thread never marked.
        assert len(harness.calls["terminate"]) == 1
        assert harness.calls["terminate"][0]["mark_thread"] is False
        assert harness.calls["terminate"][0]["preserve_shell"] is True

    @pytest.mark.asyncio
    async def test_same_thread_changed_config_reattaches(self, harness):
        unit = uuid4()
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 1, "content": "a"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=1, input_seq=1)
        )
        harness.set_attach(
            str(unit),
            {
                "thread_id": str(unit),
                "config_override": {"llm": {"model": "different"}},
                "resolved_config": None,
                "project_ids": [],
                "datasources": None,
                "config_name": "session_base",
            },
        )
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 2, "content": "b"}]
        await harness.executor._serve_claim(
            make_claim(unit_id=unit, token=2, input_seq=2)
        )
        await _finish(harness)

        assert len(harness.calls["attach"]) == 2
        assert len(harness.calls["terminate"]) == 1


# ---------------------------------------------------------------------------
# 9. Fenced persistence (postgres_db + lease context)
# ---------------------------------------------------------------------------


class _FenceConn:
    """Fake asyncpg connection: fence query returns None (rejected)."""

    def __init__(self, fence_row=None):
        self.fence_row = fence_row
        self.sql_log: List[str] = []

    def transaction(self):
        @contextlib.asynccontextmanager
        async def _tx():
            yield

        return _tx()

    async def fetchrow(self, sql, *args):
        self.sql_log.append(sql)
        if "FROM run_queue" in sql:
            return self.fence_row
        return {"id": args[0] if args else "x", "seq": 42}

    async def fetchval(self, sql, *args):
        self.sql_log.append(sql)
        return args[0] if args else 1

    async def execute(self, sql, *args):
        self.sql_log.append(sql)
        return "UPDATE 1"

    async def executemany(self, sql, args):
        self.sql_log.append(sql)


def _db_with_conn(conn):
    from agent.database.postgres_db import PostgresDB

    db = PostgresDB(connection_string="postgresql://t:t@localhost:1/t")

    @contextlib.asynccontextmanager
    async def _acquire():
        yield conn

    db.acquire = _acquire  # type: ignore[method-assign]
    return db


class TestFencedPersistence:
    @pytest.mark.asyncio
    async def test_provider_delivery_callback_uses_exact_stateless_owner(
        self, monkeypatch
    ):
        transition = AsyncMock(return_value=True)
        db = SimpleNamespace(transition_stateless_input_delivery=transition)
        thread_id = str(uuid4())
        monkeypatch.setattr(pa, "_session", SimpleNamespace(postgres_conn=db))
        monkeypatch.setattr(pa, "_thread_id", thread_id)
        handle = LeaseHandle()
        handle.update(
            thread_id,
            17,
            executor_id="executor-a",
            pod_uid="pod-a",
        )
        token = current_lease.set(handle)
        try:
            assert await pa._transition_claimed_input(
                "0d8a40c3-8f0f-4f2b-acab-8a07660ecf5d",
                3,
                "admitted",
                turn_number=8,
            )
        finally:
            current_lease.reset(token)

        transition.assert_awaited_once_with(
            thread_id=thread_id,
            delivery_id="0d8a40c3-8f0f-4f2b-acab-8a07660ecf5d",
            lease_token=17,
            executor_id="executor-a",
            pod_uid="pod-a",
            claim_generation=3,
            transition="admitted",
            turn_number=8,
            reason=None,
        )

    @pytest.mark.asyncio
    async def test_fence_rejection_raises_and_marks_lost(self):
        conn = _FenceConn(fence_row=None)
        db = _db_with_conn(conn)
        handle = LeaseHandle()
        thread_id = str(uuid4())
        handle.update(thread_id, 5)
        token = current_lease.set(handle)
        try:
            with pytest.raises(LeaseLostError):
                await db.save_thread_message(
                    thread_id=thread_id, role="human", content="x"
                )
            assert handle.lost.is_set()
            # The fence ran and nothing else did inside the transaction.
            assert any("FROM run_queue" in s for s in conn.sql_log)
            assert not any("INSERT INTO thread_messages" in s for s in conn.sql_log)
        finally:
            current_lease.reset(token)

    @pytest.mark.asyncio
    async def test_fence_pass_writes_row(self):
        conn = _FenceConn(fence_row={"?column?": 1})
        db = _db_with_conn(conn)
        handle = LeaseHandle()
        thread_id = str(uuid4())
        handle.update(thread_id, 5)
        token = current_lease.set(handle)
        try:
            result = await db.save_thread_message(
                thread_id=thread_id, role="human", content="x"
            )
            assert result["seq"] == 42
            assert any("FROM run_queue" in s for s in conn.sql_log)
            assert any("INSERT INTO thread_messages" in s for s in conn.sql_log)
        finally:
            current_lease.reset(token)

    @pytest.mark.asyncio
    async def test_no_lease_context_keeps_pinned_behavior(self):
        conn = _FenceConn(fence_row=None)  # would reject IF consulted
        db = _db_with_conn(conn)
        assert get_current_lease() is None
        result = await db.save_thread_message(
            thread_id=str(uuid4()), role="human", content="x"
        )
        assert result["seq"] == 42
        assert not any("FROM run_queue" in s for s in conn.sql_log)

    @pytest.mark.asyncio
    async def test_batch_reconcile_fenced(self):
        conn = _FenceConn(fence_row=None)
        db = _db_with_conn(conn)
        handle = LeaseHandle()
        handle.update(str(uuid4()), 8)
        token = current_lease.set(handle)
        try:
            with pytest.raises(LeaseLostError):
                await db.save_thread_messages(
                    str(uuid4()),
                    [{"id": None, "role": "ai", "content": "x", "turn_number": 1}],
                )
        finally:
            current_lease.reset(token)


# ---------------------------------------------------------------------------
# 10. Journal writer lease fence (persistent_app._OrderedPersistentEventWriter)
# ---------------------------------------------------------------------------


class TestWriterLeaseFence:
    def _writer(self, lease):
        recorded = []

        class _Txn:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return None

        class _Conn:
            def transaction(self):
                return _Txn()

            async def fetchval(self, sql, *args):
                recorded.append((sql, args))
                return 1

        class _Acquire:
            async def __aenter__(self):
                return _Conn()

            async def __aexit__(self, exc_type, exc, tb):
                return None

        pool = SimpleNamespace(acquire=lambda: _Acquire())
        writer = pa._OrderedPersistentEventWriter(
            postgres_conn=pool,
            thread_id=lease.unit_id if lease is not None else "thread-1",
            epoch=0,
            on_terminal_failure=lambda events, reason: None,
            lease=lease,
        )
        return writer, recorded

    @pytest.mark.asyncio
    async def test_lease_flush_carries_fence_params_read_at_flush_time(self):
        handle = LeaseHandle()
        unit = str(uuid4())
        handle.update(unit, 3)
        writer, recorded = self._writer(handle)
        event = pa._QueuedPersistentEvent(epoch=0, seq=1, kind="token", payload={})

        await writer._write_batch([event])
        assert "FROM threads" in recorded[0][0]
        assert "FROM run_queue" in recorded[1][0]
        sql, args = recorded[2]
        assert "run_queue" in sql and "FOR SHARE" in sql
        assert args[3] == unit and args[4] == 3

        # Affinity re-claim: same writer, handle repointed → new token flows.
        handle.update(unit, 4)
        await writer._write_batch([event])
        assert recorded[5][1][4] == 4

    @pytest.mark.asyncio
    async def test_pinned_writer_keeps_four_arg_shape(self):
        writer, recorded = self._writer(None)
        event = pa._QueuedPersistentEvent(epoch=0, seq=1, kind="token", payload={})
        await writer._write_batch([event])
        sql, args = recorded[0]
        assert "run_queue" not in sql
        assert len(args) == 3  # thread_id, rows_json, epoch


# ---------------------------------------------------------------------------
# Commit-then-effects (stateless_turn_resilience.md step 4a)
# ---------------------------------------------------------------------------


class TestCommitThenEffects:
    def test_answered_by_transcript_accepts_a_turn_completed_frame(self):
        # Carry-over from step 3: a final assistant message that carried a
        # tool call has no zero-tool-call row; the turn.completed frame for
        # that turn is the settled boundary.
        assert "turn.completed" in te._ANSWERED_BY_TRANSCRIPT_SQL
        assert "frame.payload ->> 'turn_id' = input.turn_number::text" in (
            te._ANSWERED_BY_TRANSCRIPT_SQL
        )

    @pytest.mark.asyncio
    async def test_hand_off_precedes_complete_and_the_push_is_not_awaited(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "complete"
        harness.stateless_warm_reuse_safe = True
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": "hi"}]
        claim = make_claim(token=91, input_seq=4)
        push_task = asyncio.create_task(asyncio.sleep(3600))
        staged = asyncio.Event()
        staged.set()
        pa._pending_cloud_push_task = push_task
        pa._pending_cloud_push_staged = staged
        order: List[str] = []

        async def fake_hand_off(db, *, thread_id, lease_token, pod_name, pod_uid):
            # The consumed watermark is already checkpointed when the push is
            # handed its own fence — the answer is durable before any
            # off-slot writer exists.
            assert harness.calls["interrupt_close"][-1]["completed_input_seq"] == 4
            assert thread_id == str(claim.unit_id) and lease_token == 91
            order.append("handoff")
            # what persistent_app does: the task leaves the pending slot
            pa._pending_cloud_push_task = None
            pa._pending_cloud_push_staged = None
            return True

        original_complete = te.complete_unit

        async def ordered_complete(db, **kwargs):
            order.append("complete")
            return await original_complete(db, **kwargs)

        monkeypatch.setattr(pa, "_hand_off_cloud_push", fake_hand_off, raising=False)
        monkeypatch.setattr(te, "complete_unit", ordered_complete)
        try:
            await asyncio.wait_for(harness.executor._serve_claim(claim), timeout=2)
            assert order == ["handoff", "complete"]
            # the slot is released while the transmit is still running
            assert not push_task.done()
            assert harness.calls["complete"][-1]["consumed_seq"] == 4
            assert not harness.calls["park"] and not harness.calls["release"]
        finally:
            push_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await push_task
        await _finish(harness)

    @pytest.mark.asyncio
    async def test_failed_hand_off_falls_back_to_waiting_the_push_out(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "complete"
        harness.stateless_warm_reuse_safe = True
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": "hi"}]
        claim = make_claim(token=92, input_seq=4)
        push_done = asyncio.Event()

        async def _push():
            await push_done.wait()

        push_task = asyncio.create_task(_push())
        staged = asyncio.Event()
        staged.set()
        pa._pending_cloud_push_task = push_task
        pa._pending_cloud_push_staged = staged

        async def broken_hand_off(db, **kwargs):
            raise RuntimeError("database unavailable")

        monkeypatch.setattr(pa, "_hand_off_cloud_push", broken_hand_off, raising=False)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        await asyncio.sleep(0.1)
        # completion must NOT have happened: the push is still under the lease
        assert not harness.calls["complete"]
        assert not serving.done()
        push_done.set()
        await asyncio.wait_for(serving, timeout=2)
        assert harness.calls["complete"][-1]["consumed_seq"] == 4
        await _finish(harness)

    @pytest.mark.asyncio
    async def test_cancel_after_settle_completes_instead_of_parking(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "settled_effect"
        harness.db.pending_rows = [
            {"id": str(uuid4()), "seq": 4, "content": "settled, then killed"}
        ]
        claim = make_claim(token=93, input_seq=4)
        entered = asyncio.Event()
        calls = 0

        async def complete_blocks_once(db, *, unit_id, lease_token, consumed_seq):
            nonlocal calls
            calls += 1
            if calls == 1:
                entered.set()
                await asyncio.sleep(3600)  # cancelled by shutdown
            harness.calls["complete"].append(
                {
                    "unit_id": unit_id,
                    "lease_token": lease_token,
                    "consumed_seq": consumed_seq,
                }
            )
            return "done"

        monkeypatch.setattr(te, "complete_unit", complete_blocks_once)
        harness.executor._abort_grace_seconds = 0.05
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving
        await asyncio.wait_for(entered.wait(), timeout=2)
        assert harness.has_tool_effect(claim)
        await asyncio.wait_for(harness.executor.stop(timeout=0), timeout=5)
        await _finish(harness)
        # Branch (1): settled → the idempotent completion CAS, never a park.
        assert harness.calls["complete"] == [
            {"unit_id": claim.unit_id, "lease_token": 93, "consumed_seq": None}
        ]
        assert not harness.calls["park"]
        assert not harness.calls["release"]
        assert not harness.has_tool_effect(claim)

    @pytest.mark.asyncio
    async def test_cancel_mid_turn_with_durable_tool_results_releases(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "hang"
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": "mid"}]
        harness.db.tool_effects_durable = True
        claim = make_claim(token=94, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving
        await asyncio.sleep(0.1)
        harness.executor._tool_effect_identity = (str(claim.unit_id), 94, 1)
        harness.executor._abort_grace_seconds = 0.05
        await asyncio.wait_for(harness.executor.stop(timeout=0), timeout=5)
        await _finish(harness)
        # Branch (3): every persisted tool call has its ToolMessage → release.
        assert harness.calls["release"]
        assert not harness.calls["park"]

    @pytest.mark.asyncio
    async def test_cancel_mid_turn_with_an_inflight_tool_still_parks(
        self, harness, monkeypatch
    ):
        harness.loop_behavior = "hang"
        harness.db.pending_rows = [{"id": str(uuid4()), "seq": 4, "content": "mid"}]
        harness.db.tool_effects_durable = False
        claim = make_claim(token=95, input_seq=4)
        serving = asyncio.create_task(harness.executor._serve_claim(claim))
        harness.executor._task = serving
        await asyncio.sleep(0.1)
        harness.executor._tool_effect_identity = (str(claim.unit_id), 95, 1)
        harness.executor._abort_grace_seconds = 0.05
        await asyncio.wait_for(harness.executor.stop(timeout=0), timeout=5)
        await _finish(harness)
        # Branch (4): a persisted call with no result row is the only park left.
        assert harness.calls["park"]
        assert harness.park_reasons[-1] == "shutdown_cancelled"
        assert not harness.calls["release"]
