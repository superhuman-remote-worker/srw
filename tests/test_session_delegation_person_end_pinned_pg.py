"""A person's End of a pinned session that delegated, on Postgres.

Design: knowledge-base/knowledge/features/parallel_subagents.md §5 (End and
Force-End), D4, and §14.2 (P6, defect 1).

A person's End authorizes the pinned retirement in the orchestrator before
the runtime hears of it: the token it installs revokes the parent authority
every child write needs, for good. The runtime's lifecycle watchdog then runs
the termination, whose Begin leaves the child runtime to the retirement. Up
to a poll later; meanwhile each child write is refused, and a refused call
reads the lifecycle at once. These lives must still retire, and no call may
leave a result in the turn the termination cancels:

- one that recovered its predecessor's children at attach (one batch, or one
  child alone) and has nothing live: its quiescence closes locally;
- one in a live delegation batch, whether its children still run when the
  termination comes or end in the window before it: they are left to the
  retirement, which ends each running row ``cancelled:parent_retired`` (D4),
  and Resume's settle answers every call once;
- one with a background child running.

An End still in preflight refuses child writes as well, but may abort: then
nothing is held and the session goes on.

The dying runtime is the real one (``tests.test_session_delegation_shutdown_
pinned_pg``): the loop turn, the subagent runtime, the session ledger over the
in-process orchestrator and Postgres. Its children prove their parent's
authority on Postgres, as the pinned runtime does, and the termination owner
is the pinned runtime's own.
"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from types import SimpleNamespace
from typing import Any, List
from uuid import UUID

import httpx
import pytest
from langchain_core.messages import ToolMessage

import agent.api.persistent_app as pa
from agent.api.orchestrator_client import OrchestratorClient
from agent.api.persistent_session import PersistentSession
from agent.subagents.runtime import SubagentCall
from shared.session_subagent_batch import (
    batch_continuation_text,
    not_started_result_text,
)
from tests._fanout_gate import set_session_fanout
from tests.test_session_delegation_shutdown_pinned_pg import (  # noqa: F401
    REPORT,
    _child_rows,
    _dying_turn,
    _pinned_turn,
    _resume,
    _until_rows,
    fence,
)
from tests.test_session_subagent_batch_recovery_pg import (
    _pinned_agent,
    _pinned_source_state,
)
from tests.test_session_subagent_batch_settle_pg import (
    _continuations,
    _fresh_pool,
    _metrics,
    _seed,
    _tool_rows,
)
from tests.test_subagent_thread_migration import (  # noqa: F401  (scratch_pg_dsn fixture)
    _orchestrator_db,
    scratch_pg_dsn,
)

PERSON_END = "cancelled:parent_retired"
RETIRING = "parent session retiring as ended"


@pytest.fixture(scope="module")
def pg_dsn(scratch_pg_dsn: str) -> str:  # noqa: F811 (pytest fixture param)
    """The migration test's scratch Postgres (testcontainers), reused as-is."""
    return scratch_pg_dsn


@pytest.fixture(autouse=True)
def _offline_token_counts(monkeypatch):
    """Real children count tokens; never fetch tokenizer assets here."""
    import agent.persistent_graph as persistent_graph
    from agent.core import context
    from shared.runtime.core import chunk_planner

    monkeypatch.setattr(context, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(chunk_planner, "TIKTOKEN_AVAILABLE", False)
    monkeypatch.setattr(persistent_graph, "_DELEGATION_INTERRUPT_POLL_S", 0.01)


def _prove_authority_on_postgres(runtime, agent_db, authority: dict) -> None:
    """The pinned host's effect and settlement proofs: the exact life on the
    thread row, with no retirement token (``_loop_runtime_authority_current``)."""

    async def current() -> bool:
        return await agent_db.verify_pinned_runtime_effect_authority(
            thread_id=str(runtime.host.thread_id),
            agent_id=str(authority["agent_id"]),
            pod_uid=str(authority["pod_uid"]),
            session_runtime_generation=str(authority["session_runtime_generation"]),
            runtime_attach_token=str(authority["runtime_attach_token"]),
        )

    runtime.host.effect_authority_fn = current
    runtime.host.settlement_authority_fn = current


async def _persons_end(orchestrator, seed) -> dict[str, Any]:
    """DELETE /api/persistent/threads/{id}: the owner's Begin, then the
    authorization that installs the retirement token."""

    begun = await orchestrator.begin_pinned_thread_retirement(
        str(seed.session), permanent=False, settle_status="ended"
    )
    assert begun["state"] == "pending", begun
    assert await orchestrator.authorize_pinned_thread_retirement(
        str(seed.session),
        token=str(begun["token"]),
        generation=str(begun["generation"]),
        settle_status="ended",
    )
    return begun


async def _settle_end(orchestrator, seed, begun: dict[str, Any]) -> None:
    """The rest of End once the runtime quiesced: its local-quiescence
    receipt and the settlement."""

    authority = seed.authority
    receipt = await orchestrator.acknowledge_pinned_thread_local_quiescence(
        str(seed.session),
        expected_runtime_generation=str(begun["generation"]),
        expected_retirement_token=str(begun["token"]),
        expected_agent_id=str(authority["agent_id"]),
        expected_attach_token=str(authority["runtime_attach_token"]),
        expected_settle_status="ended",
        # A sandbox session with no bound workspace: the agent runtime only.
        expected_quiescence_protocol="agent_runtime_zero_v1",
        expected_workspace_generation=None,
        expected_workspace_runtime_incarnation=None,
    )
    assert receipt is not None
    assert await orchestrator.settle_pinned_thread_retirement(
        str(seed.session),
        token=str(begun["token"]),
        generation=str(begun["generation"]),
        final_status="ended",
    )


async def _stop_turn(turn: asyncio.Task, release: asyncio.Event) -> None:
    """Never leave the dying turn running behind a failed assertion."""

    release.set()
    if not turn.done():
        turn.cancel()
    await asyncio.gather(turn, return_exceptions=True)


async def _thread_status(pool, seed) -> tuple:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, agent_id, runtime_retirement_token FROM threads "
            "WHERE id=$1",
            seed.session,
        )
    return row["status"], row["agent_id"], row["runtime_retirement_token"]


class _Session(SimpleNamespace):
    """The pinned session the termination quiesces: PersistentSession's own
    child-runtime boundaries over the dying turn's tool context."""

    quiesce_subagents = PersistentSession.quiesce_subagents
    leave_subagents_to_retirement = getattr(
        PersistentSession, "leave_subagents_to_retirement", None
    )


async def _lifecycle_client(stack, authority: dict) -> OrchestratorClient:
    """The runtime's own orchestrator client, which reads the lifecycle with
    the exact life's identity (the in-process orchestrator over Postgres)."""

    from orchestrator import main

    client = OrchestratorClient(
        "http://orchestrator.test", "127.0.0.1", 8002, "dying", "session_base"
    )
    client.agent_id = str(authority["agent_id"])
    client.session_runtime_generation = str(authority["session_runtime_generation"])
    client.session_runtime_attach_token = str(authority["runtime_attach_token"])
    client._client = await stack.enter_async_context(
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app),
            headers={"X-Internal-Key": "batch-recovery-internal-key"},
        )
    )
    return client


def _attach(monkeypatch, seed, runtime, client=None) -> Any:
    """The pinned runtime's termination owner over this exact life, nothing
    mirrored yet, and its host reading the lifecycle as production wires it
    (``retirement_authorized`` -> ``retirement_authorized_now``)."""

    authority = seed.authority
    identity = pa._session_identity
    monkeypatch.setattr(identity, "_thread_id", str(seed.session))
    monkeypatch.setattr(
        identity, "_session_generation", str(authority["session_runtime_generation"])
    )
    monkeypatch.setattr(
        identity, "_attach_token", str(authority["runtime_attach_token"])
    )
    monkeypatch.setattr(identity, "_runtime_contract", True)
    monkeypatch.setattr(pa, "_orchestrator_client", client)
    monkeypatch.setattr(
        pa,
        "_session",
        _Session(
            tool_context=runtime.parent_context,
            auxiliary_llm=None,
            memory_service=None,
            _subagent_runtime_quiesced=False,
        ),
    )
    owner = pa._session_termination
    for name in (
        "retirement_admission_identity",
        "retirement_admission_disposition",
        "retirement_admission_token",
        "retirement_admission_permanent",
        "lifecycle_poll_wake",
    ):
        monkeypatch.setattr(owner, name, None, raising=False)
    runtime.host.retirement_authorized_fn = owner.retirement_authorized_now
    return owner


def _watchdog_saw_the_end(monkeypatch, seed, begun: dict[str, Any], runtime) -> Any:
    """The runtime's lifecycle watchdog observed the authorized retirement of
    its exact life (``thread_status_watchdog``) and hands it to the pinned
    runtime's termination owner."""

    owner = _attach(monkeypatch, seed, runtime)
    owner.retirement_admission_identity = pa._session_identity.retirement_identity()
    owner.retirement_admission_disposition = "ended"
    owner.retirement_admission_token = str(begun["token"])
    owner.retirement_admission_permanent = False
    return owner


async def _begin(owner, seed) -> bool:
    """The termination's Begin (``_terminate_inner``), leaving the children."""

    return await asyncio.wait_for(
        owner.begin_retirement(
            pinned_agent_id=str(seed.authority["agent_id"]),
            retirement_disposition="ended",
            retirement_permanent=False,
            reopen_controls_if_uncommitted=False,
        ),
        30,
    )


async def _until_held(runtime, n: int) -> None:
    deadline = asyncio.get_running_loop().time() + 20
    while len(runtime._successor_waiters) < n:
        assert asyncio.get_running_loop().time() < deadline, len(
            runtime._successor_waiters
        )
        await asyncio.sleep(0.02)


async def _resume_settles_retired_batch(
    pool, orchestrator, seed, tmp_path, stack, monkeypatch, finished, running, queued
) -> None:
    """Resume: one settle answers every call once (D4 at Resume)."""

    seed.authority = await _resume(pool, orchestrator, seed)
    async with pool.acquire() as conn:
        seed.ai_seq = await conn.fetchval(
            "SELECT seq FROM thread_messages WHERE id=$1", seed.ai_id
        )
    successor = await _pinned_agent(
        pool, orchestrator, seed, tmp_path / "successor", stack, monkeypatch
    )
    await successor.runtime().recover_orphans()

    assert successor.settle_requests() == 1
    results = {row["tool_call_id"]: row for row in await _tool_rows(pool, seed)}
    assert list(results) == seed.call_ids
    assert {c: _metrics(results[c])["class"] for c in seed.call_ids} == {
        finished: "completed",
        running[0]: "retired",
        running[1]: "retired",
        queued: "not_started",
    }
    assert REPORT in results[finished]["content"]
    for call_id in running:
        assert "CANCELLED - session stopped" in results[call_id]["content"]
    assert results[queued]["content"] == not_started_result_text()
    (continuation,) = await _continuations(pool, seed)
    assert continuation["content"] == batch_continuation_text(
        calls=4, interrupted=0, not_started=1, declined=0, retired=2
    )
    assert await _pinned_source_state(pool, seed) == "settled"


# ---------------------------------------------------------------------------
# A life that recovered its predecessor's children
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "states",
    [["completed", "running"], ["running"]],
    ids=["batch", "one-child"],
)
async def test_a_life_that_recovered_children_quiesces_at_a_persons_end(
    pg_dsn: str, tmp_path, monkeypatch, states
) -> None:
    """The attach recovered a crashed turn's children (as one batch, or the
    one child on its own path), so every handle this life holds is a
    recovered one whose end is durable. A person's End then revokes the
    parent authority: quiescence has nothing to settle and closes locally,
    and the retirement completes."""

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _seed(pool, orchestrator, states, lane="pinned")
            agent = await _pinned_agent(
                pool, orchestrator, seed, tmp_path, stack, monkeypatch
            )
            runtime = agent.runtime()
            _prove_authority_on_postgres(runtime, agent.agent_db, seed.authority)

            recovered = await runtime.recover_orphans()
            assert len(recovered) == len(states)
            assert agent.settle_requests() == (1 if len(states) > 1 else 0)
            assert runtime.handles
            assert await runtime.host.settlement_authority() is True

            begun = await _persons_end(orchestrator, seed)
            assert await runtime.host.settlement_authority() is False

            # Begin's quiescence: nothing is left to settle, so it closes
            # locally without parent authority.
            await asyncio.wait_for(runtime.quiesce(RETIRING), 10)

            await _settle_end(orchestrator, seed, begun)
            assert await _thread_status(pool, seed) == ("ended", None, None)
            rows = await _child_rows(pool, seed)
            assert {row["status"] for row in rows.values()} == {"ended"}
            assert await _pinned_source_state(pool, seed) == "settled"
    finally:
        await pool.close()


# ---------------------------------------------------------------------------
# A live batch
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_a_persons_end_leaves_a_live_batch_to_the_retirement(
    pg_dsn: str,
    tmp_path,
    monkeypatch,
    fence,  # noqa: F811
) -> None:
    """Four calls, cap 2: one child finished, two run, one call is queued
    when a person ends the session. The termination's Begin completes at
    once: the two running children are stopped, the queued call never
    starts, nothing is written, and no call returns into the parent's turn,
    which the termination then cancels. The retirement ends the two running
    rows ``cancelled:parent_retired`` (D4), and Resume's one settle answers
    every call: the report, CANCELLED twice, NOT STARTED once."""

    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 4)
            started: List[str] = []
            release = asyncio.Event()  # never set: the running children wait
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=2,
                started=started,
                release=release,
            )
            stack.push_async_callback(_stop_turn, dying.turn, release)
            runtime = dying.runtime
            _prove_authority_on_postgres(runtime, dying.agent.agent_db, seed.authority)

            rows = await _until_rows(
                pool, seed, ["completed", "running", "running"], started, 3
            )
            (finished,) = [
                c for c, r in rows.items() if r["subagent_status"] == "completed"
            ]
            running = [c for c in seed.call_ids if c in rows and c != finished]
            (queued,) = [c for c in seed.call_ids if c not in rows]

            # --- the person's End reaches the orchestrator first ---
            begun = await _persons_end(orchestrator, seed)
            owner = _watchdog_saw_the_end(monkeypatch, seed, begun, runtime)

            # --- the termination's Begin leaves the children to it ---
            assert await _begin(owner, seed) is True
            # No child runs any more; no call has returned; nothing changed.
            assert pa._session._subagent_runtime_quiesced is True
            assert not runtime._active
            assert not dying.turn.done()
            assert len(started) == 3  # the queued call never reached a provider
            assert [m for m in dying.persisted if isinstance(m, ToolMessage)] == []
            assert await _tool_rows(pool, seed) == []
            after = await _child_rows(pool, seed)
            assert set(after) == {finished, *running}
            assert after[finished]["subagent_outcome"] == "completed"
            for call_id in running:
                assert after[call_id] == rows[call_id]  # still running, unwritten

            # --- the termination cancels the turn: it writes nothing ---
            dying.turn.cancel()
            await asyncio.gather(dying.turn, return_exceptions=True)
            assert dying.turn.cancelled()
            assert [m for m in dying.persisted if isinstance(m, ToolMessage)] == []
            assert await _tool_rows(pool, seed) == []

            # --- the retirement ends the running children (D4) ---
            await _settle_end(orchestrator, seed, begun)
            assert await _thread_status(pool, seed) == ("ended", None, None)
            ended = await _child_rows(pool, seed)
            assert ended[finished]["subagent_outcome"] == "completed"
            for call_id in running:
                assert ended[call_id]["subagent_outcome"] == PERSON_END

            await _resume_settles_retired_batch(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                finished,
                running,
                queued,
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_children_that_end_before_the_watchdog_leave_no_result(
    pg_dsn: str,
    tmp_path,
    monkeypatch,
    fence,  # noqa: F811
) -> None:
    """The same batch, but the running children take short steps: right
    after the End they reach their next boundary, long before the lifecycle
    watchdog's poll. Each one's end is refused, as is the queued call's row,
    and each refused call reads the lifecycle at once: the retirement is
    authorized for this life, so the token is mirrored and the call is held,
    not answered with the refusal. Nothing reaches the parent; the
    termination then leaves the runtime to the retirement and Resume settles
    the turn as for any End."""

    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 4)
            started: List[str] = []
            release = asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=2,
                started=started,
                release=release,
            )
            stack.push_async_callback(_stop_turn, dying.turn, release)
            runtime = dying.runtime
            _prove_authority_on_postgres(runtime, dying.agent.agent_db, seed.authority)
            client = await _lifecycle_client(stack, seed.authority)
            owner = _attach(monkeypatch, seed, runtime, client)

            rows = await _until_rows(
                pool, seed, ["completed", "running", "running"], started, 3
            )
            (finished,) = [
                c for c, r in rows.items() if r["subagent_status"] == "completed"
            ]
            running = [c for c in seed.call_ids if c in rows and c != finished]
            (queued,) = [c for c in seed.call_ids if c not in rows]

            # --- the End; the children take their next step at once ---
            begun = await _persons_end(orchestrator, seed)
            release.set()
            await _until_held(runtime, 3)

            # Read on the refusal, not at the next poll: mirrored now.
            assert owner.retirement_admission_token == str(begun["token"])
            assert not dying.turn.done()
            assert [m for m in dying.persisted if isinstance(m, ToolMessage)] == []
            assert await _tool_rows(pool, seed) == []
            after = await _child_rows(pool, seed)
            assert set(after) == {finished, *running}
            for call_id in running:
                assert after[call_id]["subagent_status"] == "running"

            # --- the woken watchdog's termination ---
            assert await _begin(owner, seed) is True
            assert not runtime._active
            dying.turn.cancel()
            await asyncio.gather(dying.turn, return_exceptions=True)
            assert dying.turn.cancelled()
            assert [m for m in dying.persisted if isinstance(m, ToolMessage)] == []
            assert await _tool_rows(pool, seed) == []

            await _settle_end(orchestrator, seed, begun)
            ended = await _child_rows(pool, seed)
            for call_id in running:
                assert ended[call_id]["subagent_outcome"] == PERSON_END
            await _resume_settles_retired_batch(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                finished,
                running,
                queued,
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_an_end_still_in_preflight_holds_nothing(
    pg_dsn: str,
    tmp_path,
    monkeypatch,
    fence,  # noqa: F811
) -> None:
    """An owner End in preflight installs its token unauthorized, and child
    writes are refused just the same; but it may abort. Each refused call
    reads the lifecycle, finds no authorized retirement and fails as before:
    nothing is held or mirrored and the turn ends. The End aborts, and the
    same runtime delegates again."""

    set_session_fanout(monkeypatch)
    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 2)
            started: List[str] = ["(no first child)"]  # every child waits
            release = asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=2,
                started=started,
                release=release,
            )
            stack.push_async_callback(_stop_turn, dying.turn, release)
            runtime = dying.runtime
            _prove_authority_on_postgres(runtime, dying.agent.agent_db, seed.authority)
            client = await _lifecycle_client(stack, seed.authority)
            owner = _attach(monkeypatch, seed, runtime, client)
            await _until_rows(pool, seed, ["running", "running"], started, 3)

            preflight = await orchestrator.begin_pinned_thread_retirement(
                str(seed.session), permanent=False, settle_status="ended"
            )
            assert preflight["state"] == "pending"
            assert preflight["authorized_at"] is None
            assert await runtime.host.settlement_authority() is False
            release.set()

            # Both calls fail as they did before; the turn goes on and ends.
            await asyncio.wait_for(asyncio.shield(dying.turn), 30)
            assert runtime._successor_waiters == set()
            assert runtime._left_to_retirement is False
            assert owner.retirement_admission_identity is None
            assert owner.retirement_admission_token is None
            results = await _tool_rows(pool, seed)
            assert [row["tool_call_id"] for row in results] == seed.call_ids

            # --- the End aborts: the same life goes on ---
            assert await orchestrator.abort_pinned_thread_retirement(
                str(seed.session),
                token=str(preflight["token"]),
                generation=str(preflight["generation"]),
            )
            assert await runtime.host.settlement_authority() is True
            assert runtime._accepting is True
            assert await _thread_status(pool, seed) == (
                "active",
                UUID(seed.authority["agent_id"]),
                None,
            )
    finally:
        await pool.close()


@pytest.mark.asyncio
async def test_a_persons_end_leaves_a_background_child_to_the_retirement(
    pg_dsn: str,
    tmp_path,
    monkeypatch,
    fence,  # noqa: F811
) -> None:
    """A background child runs when a person ends the session. The leave
    cancels it with no write and no delivery to the parent, a delegate call
    after it returns nothing, and the retirement ends its row."""

    pool = await _fresh_pool(pg_dsn)
    try:
        async with AsyncExitStack() as stack:
            orchestrator = _orchestrator_db(pool)
            seed = await _pinned_turn(pool, orchestrator, 1)
            started: List[str] = ["(no first child)"]  # the child waits
            release = asyncio.Event()
            dying = await _dying_turn(
                pool,
                orchestrator,
                seed,
                tmp_path,
                stack,
                monkeypatch,
                cap=2,
                started=started,
                release=release,
                background=True,
            )
            stack.push_async_callback(_stop_turn, dying.turn, release)
            runtime = dying.runtime
            _prove_authority_on_postgres(runtime, dying.agent.agent_db, seed.authority)

            # The receipt answered the call and the turn ended; the child runs.
            await _until_rows(pool, seed, ["running"], started, 2)
            await asyncio.wait_for(asyncio.shield(dying.turn), 30)
            (receipt,) = [m for m in dying.persisted if isinstance(m, ToolMessage)]
            assert "queued" in str(receipt.content)
            before = await _child_rows(pool, seed)

            begun = await _persons_end(orchestrator, seed)
            owner = _watchdog_saw_the_end(monkeypatch, seed, begun, runtime)
            assert await _begin(owner, seed) is True

            assert not runtime._active
            assert all(task.done() for task in runtime._background_tasks.values())
            assert await _child_rows(pool, seed) == before  # nothing written
            assert runtime.drain_local_deliveries() == []
            late = asyncio.create_task(
                runtime.run_background(
                    SubagentCall(
                        tool_call_id="call_late",
                        subagent_type="explorer",
                        prompt="Too late.",
                        run_in_background=True,
                    )
                )
            )
            await asyncio.sleep(0.1)
            assert not late.done()  # held: no "quiescing" error for the turn
            late.cancel()
            await asyncio.gather(late, return_exceptions=True)

            await _settle_end(orchestrator, seed, begun)
            assert await _thread_status(pool, seed) == ("ended", None, None)
            (row,) = (await _child_rows(pool, seed)).values()
            assert row["subagent_outcome"] == PERSON_END
    finally:
        await pool.close()
