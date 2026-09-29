"""Session fan-out: configuration, cap, gate and tool description (WP3a).

Design: knowledge-base/knowledge/features/parallel_subagents.md §6.4, §12,
§13 D2/D5. What this file pins:

* the session cap resolves code default (6) → parent-family matrix value →
  explicit expert/session value, clamped to 1..20; the per-turn maximum
  defaults to 20 (clamped to 1..64);
* the settings matrix routes a family's ``session_max_concurrent`` to its own
  delegation slot and re-derives it on every pass (a model switch replaces it);
* ``session_fanout_allowed`` = gate on for the lane AND the orchestrator's
  batch-settle capability AND a session parent (pinned needs its own key);
* the ``delegate_agent`` description tells each parent the truth: the WP0
  text when fan-out is not allowed, the effective cap / one-child-per-response
  / writer rule / recovery markers when it is, workers unchanged;
* the cap the description states is the cap the runtime enforces, also after
  a live config change.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import itertools
import logging
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import shared.runtime.core.loader as loader
from agent.subagents.limiter import LiveConcurrencyLimit
from agent.tools.delegation.delegate_agent import (
    INTERRUPTED_MARKER,
    NOT_STARTED_MARKER,
    build_description,
)
from agent.tools.delegation.fanout import (
    delegation_max_calls_per_turn,
    delegation_max_concurrent,
    parent_parallel_tool_calls,
    session_fanout_allowed,
)
from shared.runtime.core.delegation_settings import (
    FAMILY_SESSION_MAX_CONCURRENT_KEY,
    SESSION_MAX_CALLS_PER_TURN_DEFAULT,
    SESSION_MAX_CONCURRENT_DEFAULT,
    session_fanout_configured,
    session_max_calls_per_turn,
    session_max_concurrent,
)
from shared.runtime.core.loader import (
    DelegationConfig,
    _apply_settings_matrix,
    load_agent_config_from_dict,
)
from shared.runtime.core.model_registry import family_of
from shared.runtime.core.session_config_patch import patch_frozen_session
from shared.session_subagent_batch import (
    SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY,
    not_started_result_text,
)
from tests.test_delegate_agent_tool import (
    EchoModel,
    brief_args,
    install,
    invoke,
    make_parent,
    the_tool,
)

#: The WP0 sentence a session is told when it may not fan out.
_WP0_SINGLE = "issue exactly ONE delegate_agent call per response"
_FANOUT = "send one delegate_agent call per brief in a single response"
_WRITER_RULE = "at most one child with write tools may work there at a time"
_NO_PARALLEL = "One subagent at a time: you send one tool call per response"


def session_parent(
    tmp_path,
    *,
    lane: str = "stateless",
    gate: bool = True,
    pinned_gate: bool = False,
    contract: bool = True,
    parallel_tool_calls: bool = True,
    **delegation,
):
    """A session-marked parent whose fan-out inputs are all set explicitly."""
    ctx, root = make_parent(tmp_path, max_concurrent=delegation.pop("max", 2))
    ctx._subagent_parent_kind = "session"
    ctx._subagent_execution_lane = lane
    ctx._session_subagent_batch_settle_contract = contract
    ctx.config["delegation"].update(
        {"session_fanout": gate, "session_fanout_pinned": pinned_gate, **delegation}
    )
    ctx.config["parallel_tool_calls"] = parallel_tool_calls
    return ctx, root


# ---------------------------------------------------------------------------
# Cap and per-turn maximum resolution
# ---------------------------------------------------------------------------


class TestCapResolution:
    def test_code_default(self):
        assert SESSION_MAX_CONCURRENT_DEFAULT == 6
        assert session_max_concurrent({}) == 6
        assert session_max_concurrent(None) == 6
        assert session_max_concurrent({"max_concurrent": 2}) == 6  # worker cap

    def test_family_value_beats_the_default(self):
        assert session_max_concurrent({FAMILY_SESSION_MAX_CONCURRENT_KEY: 10}) == 10

    @pytest.mark.parametrize("explicit", [2, 12])
    def test_explicit_value_beats_the_family(self, explicit):
        block = {
            "session_max_concurrent": explicit,
            FAMILY_SESSION_MAX_CONCURRENT_KEY: 8,
        }
        assert session_max_concurrent(block) == explicit

    @pytest.mark.parametrize(
        ("raw", "expected"), [(0, 1), (-5, 1), (1, 1), (20, 20), (21, 20), (999, 20)]
    )
    def test_clamped_at_both_ends(self, raw, expected):
        assert session_max_concurrent({"session_max_concurrent": raw}) == expected
        assert (
            session_max_concurrent({FAMILY_SESSION_MAX_CONCURRENT_KEY: raw}) == expected
        )

    @pytest.mark.parametrize("bad", ["many", True, 2.5j, [3]])
    def test_an_unusable_explicit_value_falls_through(self, bad):
        block = {"session_max_concurrent": bad, FAMILY_SESSION_MAX_CONCURRENT_KEY: 9}
        assert session_max_concurrent(block) == 9
        assert session_max_concurrent({"session_max_concurrent": bad}) == 6

    @pytest.mark.parametrize(
        ("raw", "expected"),
        [(None, 20), (5, 5), (0, 1), (500, 64), ("x", 20), (False, 20)],
    )
    def test_per_turn_maximum(self, raw, expected):
        assert SESSION_MAX_CALLS_PER_TURN_DEFAULT == 20
        block = {} if raw is None else {"session_max_calls_per_turn": raw}
        assert session_max_calls_per_turn(block) == expected

    def test_the_parser_keeps_the_explicit_and_family_slots_apart(self, caplog):
        with caplog.at_level(logging.WARNING, logger="shared.runtime.core.loader"):
            cfg = load_agent_config_from_dict(
                {
                    "agent_id": "a",
                    "display_name": "A",
                    "delegation": {
                        "session_max_concurrent": 50,
                        FAMILY_SESSION_MAX_CONCURRENT_KEY: 7,
                        "session_max_calls_per_turn": 0,
                        "session_fanout": "yes",  # only a literal true opens it
                        "session_fanout_pinned": True,
                    },
                }
            )
        assert cfg.delegation == DelegationConfig(
            session_fanout=False,
            session_fanout_pinned=True,
            session_max_concurrent=20,
            family_session_max_concurrent=7,
            session_max_calls_per_turn=1,
        )
        messages = " ".join(r.message for r in caplog.records)
        assert "delegation.session_max_concurrent=50 is outside 1..20" in messages
        assert "delegation.session_max_calls_per_turn=0 is outside 1..64" in messages

    def test_defaults_when_nothing_is_authored(self):
        cfg = load_agent_config_from_dict({"agent_id": "a", "display_name": "A"})
        assert cfg.delegation.session_fanout is False
        assert cfg.delegation.session_fanout_pinned is False
        assert cfg.delegation.session_max_concurrent is None
        assert cfg.delegation.session_max_calls_per_turn == 20


# ---------------------------------------------------------------------------
# The settings-matrix route (keyed on the PARENT's family)
# ---------------------------------------------------------------------------

_CLAUDE = "claude-opus-4-5"
_GPT = "gpt-5.5"


@pytest.fixture
def family_caps(monkeypatch):
    """A settings matrix whose Claude family sets a session cap of 10."""
    claude, gpt = family_of(_CLAUDE), family_of(_GPT)
    assert claude != gpt
    matrix = {
        "default": {"temperature": 0.0, "model_max_context_tokens": 128000},
        claude: {"session_max_concurrent": 10},
        gpt: {"temperature": 1.0},
    }
    monkeypatch.setattr(loader, "_load_settings_matrix", lambda *a, **k: matrix)
    monkeypatch.setattr(loader, "_settings_override_for", lambda family: {})
    return matrix


class TestMatrixRoute:
    def test_the_bundled_matrix_sets_no_family_cap_yet(self):
        loader._model_config_matrix_cache.clear()
        for family, settings in loader._load_settings_matrix().items():
            assert "session_max_concurrent" not in settings, family
        data = {"llm": {"model": _CLAUDE}}
        _apply_settings_matrix(data, set())
        assert FAMILY_SESSION_MAX_CONCURRENT_KEY not in (data.get("delegation") or {})
        cfg = load_agent_config_from_dict(
            {"agent_id": "a", "display_name": "A", **data}
        )
        assert session_max_concurrent(dataclasses.asdict(cfg.delegation)) == 6

    def test_a_family_value_lands_in_the_delegation_slot_not_llm(self, family_caps):
        data = {"llm": {"model": _CLAUDE}}
        _apply_settings_matrix(data, set())
        assert data["delegation"] == {FAMILY_SESSION_MAX_CONCURRENT_KEY: 10}
        assert "session_max_concurrent" not in data["llm"]
        assert session_max_concurrent(data["delegation"]) == 10

    def test_an_explicit_value_survives_the_matrix(self, family_caps):
        data = {
            "llm": {"model": _CLAUDE},
            "delegation": {"enabled": True, "session_max_concurrent": 3},
        }
        _apply_settings_matrix(data, set())
        assert data["delegation"]["session_max_concurrent"] == 3
        assert data["delegation"][FAMILY_SESSION_MAX_CONCURRENT_KEY] == 10
        assert session_max_concurrent(data["delegation"]) == 3

    def test_a_model_switch_replaces_the_family_value(self, family_caps):
        data = {"llm": {"model": _CLAUDE}, "delegation": {"enabled": True}}
        _apply_settings_matrix(data, set())
        assert session_max_concurrent(data["delegation"]) == 10
        data["llm"]["model"] = _GPT
        _apply_settings_matrix(data, {"model"})
        assert data["delegation"] == {"enabled": True}
        assert session_max_concurrent(data["delegation"]) == 6

    def test_a_family_value_is_clamped(self, family_caps):
        family_caps[family_of(_CLAUDE)]["session_max_concurrent"] = 99
        data = {"llm": {"model": _CLAUDE}}
        _apply_settings_matrix(data, set())
        assert data["delegation"][FAMILY_SESSION_MAX_CONCURRENT_KEY] == 20

    def test_a_frozen_session_model_switch_carries_the_new_family_value(
        self, family_caps
    ):
        agent = {"agent_id": "a", "display_name": "A", "llm": {"model": _CLAUDE}}
        _apply_settings_matrix(agent, set())
        blob = {"agent": agent}
        updated, policy, delta = patch_frozen_session(
            blob, {}, {"llm": {"model": _GPT}}
        )
        assert FAMILY_SESSION_MAX_CONCURRENT_KEY not in updated["agent"]["delegation"]
        # The pinned runtime's merge receives the removal explicitly.
        assert delta["delegation"] == {FAMILY_SESSION_MAX_CONCURRENT_KEY: None}

        back = {"agent": deepcopy(updated["agent"])}
        updated, _, delta = patch_frozen_session(
            back, policy, {"llm": {"model": _CLAUDE}}
        )
        assert updated["agent"]["delegation"][FAMILY_SESSION_MAX_CONCURRENT_KEY] == 10
        assert delta["delegation"] == {FAMILY_SESSION_MAX_CONCURRENT_KEY: 10}

    def test_a_cosmetic_patch_does_not_touch_the_delegation_block(self, family_caps):
        agent = {"agent_id": "a", "display_name": "A", "llm": {"model": _CLAUDE}}
        _apply_settings_matrix(agent, set())
        _, _, delta = patch_frozen_session(
            {"agent": agent}, {}, {"llm": {"temperature": 0.3}}
        )
        assert "delegation" not in delta


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


class TestGate:
    @pytest.mark.parametrize(
        ("gate", "pinned_gate", "contract", "lane"),
        list(
            itertools.product(
                [False, True],
                [False, True],
                [False, True],
                ["stateless", "pinned", None],
            )
        ),
    )
    def test_truth_table(self, tmp_path, gate, pinned_gate, contract, lane):
        ctx, _ = session_parent(
            tmp_path, lane=lane, gate=gate, pinned_gate=pinned_gate, contract=contract
        )
        expected = (
            gate
            and contract
            and (lane == "stateless" or (lane == "pinned" and pinned_gate))
        )
        assert session_fanout_allowed(ctx) is expected
        assert session_fanout_configured(ctx.config["delegation"], lane) is (
            gate and (lane == "stateless" or (lane == "pinned" and pinned_gate))
        )

    @pytest.mark.parametrize("truthy", [1, "true", "yes", {"on": True}, [True]])
    def test_only_a_literal_true_opens_either_gate_key(self, tmp_path, truthy):
        """A hand-edited or stored layer may carry a truthy non-bool; the
        resolver must not read it as on (the parser coerces it the same way)."""
        ctx, _ = session_parent(tmp_path / "gate", gate=truthy)
        assert session_fanout_configured(ctx.config["delegation"], "stateless") is False
        assert session_fanout_allowed(ctx) is False

        ctx, _ = session_parent(
            tmp_path / "pinned", lane="pinned", gate=True, pinned_gate=truthy
        )
        assert session_fanout_configured(ctx.config["delegation"], "pinned") is False
        assert session_fanout_allowed(ctx) is False

    def test_a_worker_never_fans_out_through_the_session_gate(self, tmp_path):
        ctx, _ = session_parent(tmp_path)
        ctx._subagent_parent_kind = None  # the worker default
        assert session_fanout_allowed(ctx) is False

    @pytest.mark.parametrize("value", [1, "true", None])
    def test_only_a_literal_capability_true_counts(self, tmp_path, value):
        ctx, _ = session_parent(tmp_path, contract=True)
        ctx._session_subagent_batch_settle_contract = value
        assert session_fanout_allowed(ctx) is False

    def test_the_cap_follows_the_gate(self, tmp_path):
        ctx, _ = session_parent(tmp_path, max=2, session_max_concurrent=9)
        assert delegation_max_concurrent(ctx) == 9
        ctx._session_subagent_batch_settle_contract = False
        # Not allowed: exactly the pre-WP3 cap (behaviour-neutral gate off).
        assert delegation_max_concurrent(ctx) == 2

    def test_the_worker_cap(self, tmp_path):
        ctx, _ = make_parent(tmp_path, max_concurrent=3)
        assert delegation_max_concurrent(ctx) == 3
        ctx.config["delegation"]["max_concurrent"] = 0
        assert delegation_max_concurrent(ctx) == 1
        ctx.config["delegation"]["max_concurrent"] = "many"
        assert delegation_max_concurrent(ctx) == 4
        del ctx.config["delegation"]["max_concurrent"]
        assert delegation_max_concurrent(ctx) == 4

    def test_per_turn_maximum_reads_the_live_block(self, tmp_path):
        ctx, _ = session_parent(tmp_path)
        assert delegation_max_calls_per_turn(ctx) == 20
        ctx.config["delegation"]["session_max_calls_per_turn"] = 8
        assert delegation_max_calls_per_turn(ctx) == 8

    def test_parallel_tool_calls_prefers_the_live_tool_config(self, tmp_path):
        ctx, _ = make_parent(tmp_path)
        # make_parent's LLMConfig keeps the dataclass default (False).
        assert parent_parallel_tool_calls(ctx) is False
        ctx.config["parallel_tool_calls"] = True
        assert parent_parallel_tool_calls(ctx) is True


# ---------------------------------------------------------------------------
# The description, per mode
# ---------------------------------------------------------------------------


class TestDescription:
    def test_the_not_started_marker_is_what_the_server_renders(self):
        """The settle writes ``not_started_result_text()`` as the call's result
        (shared.session_subagent_batch); the description must name its header
        byte for byte."""
        assert not_started_result_text().startswith(NOT_STARTED_MARKER + "\n")

    def test_the_interrupted_marker_is_what_recovery_renders(self):
        # WP2 renders the interrupted result on the agent
        # (agent.subagents.batch_recovery.INTERRUPTED_HEADER). Pinned to the
        # literal as well, so this file stands alone while that module is new.
        assert INTERRUPTED_MARKER == "[delegate_agent: INTERRUPTED - no final report]"
        batch_recovery = pytest.importorskip("agent.subagents.batch_recovery")
        assert INTERRUPTED_MARKER == batch_recovery.INTERRUPTED_HEADER

    def test_not_allowed_keeps_the_wp0_text_exactly(self, tmp_path):
        """Gate off, capability missing, or pinned without its key: the WP0
        single-child text, byte for byte."""
        cases = [
            dict(gate=False),
            dict(contract=False),
            dict(lane="pinned"),  # gate on, pinned key off
        ]
        for i, case in enumerate(cases):
            ctx, _ = session_parent(tmp_path / str(i), **case)
            lane = case.get("lane", "stateless")
            expected = build_description(
                ctx.config["subagents"]["roster"],
                default="explorer",
                max_concurrent=2,
                single_child_per_response=True,
                background_available=lane != "stateless",
            )
            text = the_tool(ctx).description
            assert text == expected, case
            assert _WP0_SINGLE in text
            for absent in (
                _FANOUT,
                _WRITER_RULE,
                INTERRUPTED_MARKER,
                NOT_STARTED_MARKER,
            ):
                assert absent not in text

    def test_allowed_states_the_effective_cap_writer_rule_and_markers(self, tmp_path):
        ctx, _ = session_parent(tmp_path)
        text = the_tool(ctx).description
        assert _WP0_SINGLE not in text
        assert _FANOUT in text
        assert (
            "Up to 6 subagents run at once; more calls queue and run in waves." in text
        )
        assert "Delegation runs in a turn of its own" in text
        assert _WRITER_RULE in text
        assert "a second is refused, not queued" in text
        assert 'isolation="worktree"' in text
        assert f"A result starting {INTERRUPTED_MARKER} means" in text
        assert f"A result starting {NOT_STARTED_MARKER} means" in text
        # Stateless: still no background offer.
        assert "run_in_background is not available in this session" in text
        # The worker's "partition ... never two writers on the same files"
        # wording is replaced, not repeated.
        assert "All agents share the working tree" not in text

    def test_the_stated_cap_is_the_resolved_one(self, tmp_path):
        ctx, _ = session_parent(tmp_path, session_max_concurrent=9)
        assert "Up to 9 subagents run at once" in the_tool(ctx).description
        ctx, _ = session_parent(
            tmp_path / "family", **{FAMILY_SESSION_MAX_CONCURRENT_KEY: 12}
        )
        assert "Up to 12 subagents run at once" in the_tool(ctx).description
        ctx, _ = session_parent(tmp_path / "one", session_max_concurrent=1)
        assert "Up to 1 subagent runs at once" in the_tool(ctx).description

    def test_a_family_without_parallel_tool_calls_is_told_one_child_at_a_time(
        self, tmp_path
    ):
        ctx, _ = session_parent(tmp_path, parallel_tool_calls=False)
        text = the_tool(ctx).description
        assert _NO_PARALLEL in text
        assert _FANOUT not in text and "run at once" not in text
        # The writer rule and the markers still hold.
        assert _WRITER_RULE in text and NOT_STARTED_MARKER in text

    def test_a_pinned_session_with_its_key_fans_out_and_keeps_background(
        self, tmp_path
    ):
        ctx, _ = session_parent(tmp_path, lane="pinned", pinned_gate=True)
        text = the_tool(ctx).description
        assert _FANOUT in text
        assert "run_in_background=true returns an immediate durable receipt" in text

    def test_worker_descriptions_are_unchanged(self, tmp_path):
        ctx, _ = make_parent(tmp_path, max_concurrent=3)
        # Session knobs and the capability mean nothing to a worker.
        ctx.config["delegation"].update(
            {"session_fanout": True, "session_max_concurrent": 9}
        )
        ctx._session_subagent_batch_settle_contract = True
        ctx._subagent_execution_lane = "stateless"
        assert the_tool(ctx).description == build_description(
            ctx.config["subagents"]["roster"], default="explorer", max_concurrent=3
        )


# ---------------------------------------------------------------------------
# One cap for the description and the semaphore, also after a live change
# ---------------------------------------------------------------------------


def _max_overlap(spans):
    return max(sum(1 for s2, e2 in spans if s2 < e1 and e2 > s1) for s1, e1 in spans)


class TestLiveConfigChange:
    @pytest.mark.asyncio
    async def test_a_live_change_reaches_the_description_and_the_semaphore(
        self, tmp_path
    ):
        ctx, _ = session_parent(tmp_path, max=2, session_max_concurrent=1)
        spans: list = []
        runtime = install(
            ctx, factory=lambda cfg, lim: EchoModel(delay=0.12, spans=spans)
        )
        assert runtime.max_concurrent == 1
        assert "Up to 1 subagent runs at once" in the_tool(ctx).description

        # The live update rewrites tool_context.config in place
        # (PersistentSession.refresh_tool_context_config) and rebuilds tools.
        ctx.config["delegation"]["session_max_concurrent"] = 3
        tool = the_tool(ctx)
        assert "Up to 3 subagents run at once" in tool.description
        assert runtime.max_concurrent == 3
        outs = await asyncio.gather(
            *(invoke(tool, f"call-{i}", **brief_args(i)) for i in range(4))
        )
        assert all(f"echo: brief {i}" in out for i, out in enumerate(outs))
        assert _max_overlap(spans) == 3

        # Gate off again: back to the pre-WP3 cap and the WP0 text, both sides.
        ctx.config["delegation"]["session_fanout"] = False
        assert runtime.max_concurrent == 2
        assert _WP0_SINGLE in the_tool(ctx).description

    @pytest.mark.asyncio
    async def test_a_raised_cap_starts_queued_children_without_a_release(
        self, tmp_path
    ):
        """The live config path calls ``runtime.refresh_concurrency()``: calls
        queued behind the old cap start at once, not when a child finishes."""
        ctx, _ = session_parent(tmp_path, session_max_concurrent=1)
        spans: list = []
        runtime = install(
            ctx, factory=lambda cfg, lim: EchoModel(delay=0.4, spans=spans)
        )
        tool = the_tool(ctx)
        calls = [
            asyncio.create_task(invoke(tool, f"call-{i}", **brief_args(i)))
            for i in range(3)
        ]
        await asyncio.sleep(0.1)
        assert runtime._semaphore.held == 1 and runtime._semaphore.waiting == 2

        ctx.config["delegation"]["session_max_concurrent"] = 3
        assert runtime.refresh_concurrency() == 2
        await asyncio.gather(*calls)
        starts = sorted(s for s, _ in spans)
        ends = sorted(e for _, e in spans)
        # All three ran together: the queued two started before the first ended.
        assert starts[2] < ends[0]
        assert _max_overlap(spans) == 3

    def test_from_context_reads_the_same_function_as_the_description(self, tmp_path):
        ctx, _ = session_parent(tmp_path, max=2, session_max_concurrent=5)
        runtime = install(ctx, factory=lambda cfg, lim: EchoModel())
        assert runtime.max_concurrent == delegation_max_concurrent(ctx) == 5


# ---------------------------------------------------------------------------
# The live limiter
# ---------------------------------------------------------------------------


class TestLiveConcurrencyLimit:
    @pytest.mark.asyncio
    async def test_fifo_waves_and_a_raised_limit(self):
        box = {"limit": 1}
        limit = LiveConcurrencyLimit(lambda: box["limit"])
        order: list = []
        gates = {i: asyncio.Event() for i in range(3)}

        async def worker(i):
            async with limit:
                order.append(i)
                await gates[i].wait()

        tasks = [asyncio.create_task(worker(i)) for i in range(3)]
        await asyncio.sleep(0.01)
        assert order == [0] and limit.held == 1 and limit.waiting == 2
        box["limit"] = 3
        # A raised limit admits queued callers at the next release.
        gates[0].set()
        await asyncio.sleep(0.01)
        assert order == [0, 1, 2] and limit.held == 2
        gates[1].set()
        gates[2].set()
        await asyncio.gather(*tasks)
        assert limit.held == 0

    @pytest.mark.asyncio
    async def test_wake_admits_queued_callers_after_a_raised_limit(self):
        box = {"limit": 1}
        limit = LiveConcurrencyLimit(lambda: box["limit"])
        await limit.acquire()
        waiters = [asyncio.create_task(limit.acquire()) for _ in range(3)]
        await asyncio.sleep(0.01)
        assert limit.wake() == 0  # nothing changed: still full
        box["limit"] = 3
        assert limit.wake() == 2  # no release needed
        await asyncio.sleep(0.01)
        assert [w.done() for w in waiters] == [True, True, False]
        assert limit.held == 3

    @pytest.mark.asyncio
    async def test_a_new_admission_first_admits_callers_a_raised_limit_fits(self):
        """``acquire`` re-reads the limit for the queue before queueing the new
        caller: the queued one goes first (FIFO), the newcomer waits."""
        box = {"limit": 1}
        limit = LiveConcurrencyLimit(lambda: box["limit"])
        await limit.acquire()
        queued = asyncio.create_task(limit.acquire())
        await asyncio.sleep(0.01)
        box["limit"] = 2
        newcomer = asyncio.create_task(limit.acquire())
        await asyncio.sleep(0.01)
        assert queued.done() and not newcomer.done()
        assert limit.held == 2
        limit.release()
        await asyncio.wait_for(newcomer, 1)

    @pytest.mark.asyncio
    async def test_a_lowered_limit_drains_without_preempting(self):
        box = {"limit": 2}
        limit = LiveConcurrencyLimit(lambda: box["limit"])
        await limit.acquire()
        await limit.acquire()
        box["limit"] = 1
        waiter = asyncio.create_task(limit.acquire())
        await asyncio.sleep(0.01)
        limit.release()  # 1 held, limit 1: still full
        await asyncio.sleep(0.01)
        assert not waiter.done()
        limit.release()
        await asyncio.wait_for(waiter, 1)
        assert limit.held == 1

    @pytest.mark.asyncio
    async def test_a_cancelled_waiter_gives_a_handed_slot_back(self):
        limit = LiveConcurrencyLimit(1)
        await limit.acquire()
        first = asyncio.create_task(limit.acquire())
        second = asyncio.create_task(limit.acquire())
        await asyncio.sleep(0.01)
        limit.release()  # hands the slot to `first`
        first.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first
        await asyncio.wait_for(second, 1)
        assert limit.held == 1

    def test_limits_and_errors(self):
        assert LiveConcurrencyLimit(0).limit == 1
        assert LiveConcurrencyLimit(lambda: 1 / 0, fallback=4).limit == 4
        with pytest.raises(ValueError):
            LiveConcurrencyLimit(1).release()


# ---------------------------------------------------------------------------
# The capability reaches the session from every attach path
# ---------------------------------------------------------------------------


def test_every_attach_entry_point_accepts_the_capability():
    """Pinned attach passes the body's advertisement as this keyword; the
    stateless executor folds the claim bundle's top-level one in
    (``turn_executor.claim_bundle_attach``)."""
    import agent.api.persistent_app as pa

    for fn in (pa._attach_session, pa._attach_session_inner):
        assert (
            SESSION_SUBAGENT_BATCH_SETTLE_CONTRACT_KEY
            in inspect.signature(fn).parameters
        ), fn.__name__


class _StopAttach(Exception):
    pass


@pytest.mark.parametrize(
    ("advertised", "expected"),
    [(1, True), (True, False), (2, False), (None, False), ("1", False), (1.0, False)],
)
@pytest.mark.asyncio
async def test_attach_accepts_only_the_exact_int_1(advertised, expected):
    """Like the other attach contracts, only ``int`` 1 counts: bool ``True``
    (an int subclass) and any other version are refused."""
    import agent.api.persistent_app as papp

    seen: dict = {}

    class FakeSession:
        def __init__(self, *args, **kwargs):
            seen.update(kwargs)
            raise _StopAttach  # everything after construction is irrelevant

    agent = SimpleNamespace(
        config=SimpleNamespace(workspace=SimpleNamespace(backend="none")),
        _tactical_llm=None,
        _llm=object(),
        _auxiliary_llm=None,
        postgres_conn=MagicMock(),
        vector_conn=None,
    )
    workspace = {
        "cloud_mount": None,
        "cloud_sync": None,
        "protected_cloud": False,
        "project_ids": [],
        "datasources": None,
    }
    client = SimpleNamespace(
        get_thread_workspace=AsyncMock(return_value=workspace), agent_id=None
    )
    with (
        patch.object(papp, "_session", None),
        patch.object(papp, "_thread_id", None),
        patch.object(papp, "_event_writer", None),
        patch.object(papp, "_agent", agent),
        patch.object(papp, "_orchestrator_client", client),
        patch.object(papp, "PersistentSession", FakeSession),
        patch.object(papp, "_session_backend_is_lite", return_value=True),
        patch.object(papp, "_officer_cfg", return_value=None),
        patch.object(papp, "_apply_session_embedding_env"),
    ):
        with pytest.raises(_StopAttach):
            await papp._attach_session_inner(
                "11111111-1111-4111-8111-111111111111",
                config_override={},
                session_subagent_batch_settle_contract=advertised,
            )
    assert seen["subagent_batch_settle_contract"] is expected
