"""Pool surfacing — B4 of knowledge-base/knowledge/features/officer_backlog_pools.md.

B3 made the tick enforce policy; B4 makes the officer able to SEE it. That is
not cosmetic. A policy enforced but invisible invites doctrine drift: he
watches a pool sit idle, concludes the queue is healthy, and never learns its
breaker is open. Everything rendered here is state the tick already wrote.

The other half is the precedence law. A slot's category decides the contract
its worker is held to, and an officer dispatching cross-category is warned
rather than refused — but never silently contradicted, which would leave the
worker reading one contract while occupying a slot that means another.
"""

import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.officer_backlog import pool_status_lines, ready_depth_by_pool
from orchestrator.services.officer_slots import (
    below_floor_pools,
    capacity_lines,
    validate_slots_spec,
)
from orchestrator.services.project_loop_spawn import officer_slot_category

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)

POOLS = {
    "researchers": {"count": 2, "category": "researcher"},
    "executors": {"count": 1, "category": "executor"},
}
META = {"enabled": True, "auto_pull": True, "slots": POOLS}


# =============================================================================
# The capacity line — utilization is the wrong question on its own
# =============================================================================


class TestCapacityLine:
    def test_legacy_rosters_render_exactly_as_before(self):
        # No categories, no ready depth: a century that never opted into pools
        # must see a byte-identical line.
        flat = {"slots": {"line": {"count": 2}, "heavy": {"count": 1}}}
        assert capacity_lines(flat, {"line": 1}) == (
            "Capacity: heavy 0/1, line 1/2 worker slots in use."
        )

    def test_flat_cap_path_is_untouched(self):
        assert capacity_lines({"max_concurrent_workers": 3}, {None: 2}) == (
            "Capacity: 2/3 worker slots in use."
        )

    def test_pools_carry_their_ready_depth(self):
        line = capacity_lines(
            META, {"researchers": 1}, ready_by_pool={"researchers": 4, "executors": 2}
        )
        assert "researchers 1/2 (ready 4)" in line
        assert "executors 0/1 (ready 2)" in line

    def test_a_pool_below_its_floor_says_so(self):
        # The floor IS the slot count (§13.2): if every agent lands at once,
        # each must find a ticket. An idle slot with a healthy queue is slack
        # and fine; an empty queue is the thing to act on.
        line = capacity_lines(
            META, {}, ready_by_pool={"researchers": 1, "executors": 1}
        )
        assert "researchers 0/2 (ready 1, BELOW FLOOR)" in line
        assert "executors 0/1 (ready 1)" in line  # at its floor, not below

    def test_depth_is_omitted_rather_than_faked_when_unread(self):
        # ready_by_pool=None means the KB was not read. Rendering "ready:0"
        # would be an assertion nobody measured.
        line = capacity_lines(META, {"researchers": 1})
        assert "ready" not in line

    def test_uncategorized_slots_never_show_depth(self):
        meta = {"slots": {"adhoc": {"count": 1}, "researchers": POOLS["researchers"]}}
        line = capacity_lines(meta, {}, ready_by_pool={"researchers": 3})
        assert "adhoc 0/1," in line and "adhoc 0/1 (ready" not in line

    def test_oldest_claim_age_is_the_counterweight_to_a_deep_queue(self):
        line = capacity_lines(
            META,
            {"researchers": 1},
            ready_by_pool={"researchers": 9},
            oldest_claim_age_hours=51.4,
        )
        assert "Oldest open claim 51h." in line


# =============================================================================
# The floor predicate — one definition, two readers
# =============================================================================


class TestBelowFloorPools:
    """The marker and the wake's closing call-to-action share this predicate.

    On 2026-08-20 an officer read "ready 0, BELOW FLOOR" on all three of his
    pools across ten consecutive wakes and filed sleep on every one, because
    the marker sat mid-line while the last thing he read was "file a sleep".
    The names are now also rendered as the wake's final line — so the two
    renderings must never disagree about which pools are short.
    """

    def test_it_agrees_with_the_marker(self):
        ready = {"researchers": 1, "executors": 1}
        line = capacity_lines(META, {}, ready_by_pool=ready)
        starved = below_floor_pools(META, ready)
        assert starved == ["researchers"]
        assert "researchers 0/2 (ready 1, BELOW FLOOR)" in line
        assert "executors 0/1 (ready 1)" in line

    def test_a_healthy_queue_names_nobody(self):
        # The negative control: a full queue must produce no call to action,
        # or the closing line becomes noise the officer learns to skip.
        assert below_floor_pools(META, {"researchers": 2, "executors": 4}) == []

    def test_unread_depth_is_not_starvation(self):
        # ready_by_pool=None means the KB was never read. "unavailable" is
        # never "empty" — inventing a floor breach here would send the officer
        # to arm tickets against a number nobody measured.
        assert below_floor_pools(META, None) == []

    def test_uncategorized_slots_have_no_floor(self):
        meta = {"slots": {"adhoc": {"count": 3}, "researchers": POOLS["researchers"]}}
        assert below_floor_pools(meta, {"researchers": 2}) == []

    def test_a_century_without_pools_names_nobody(self):
        assert below_floor_pools({"max_concurrent_workers": 3}, {"anything": 0}) == []

    def test_a_missing_pool_counts_as_zero_ready(self):
        # The tick returns no key for a pool it could not count as eligible;
        # that pool is starved, not absent.
        assert below_floor_pools(META, {"executors": 5}) == ["researchers"]


# =============================================================================
# Policy rendering — what the tick enforces, the officer can read
# =============================================================================


class TestPoolStatusLines:
    def test_an_open_breaker_is_stated_with_its_cause(self):
        state = {
            "backlog_breakers": {
                "researchers": {
                    "until": (NOW + timedelta(minutes=18)).isoformat(),
                    "cause": "2 consecutive job failures on distinct tickets",
                    "tickets": ["feature-a", "feature-b"],
                }
            }
        }
        lines = pool_status_lines(META, state, NOW)
        breaker = next(line for line in lines if "BREAKER OPEN" in line)
        assert "researchers" in breaker and "18m" in breaker
        assert "feature-a, feature-b" in breaker
        # The point of naming the tickets: he should read the failures before
        # re-readying anything in that pool.
        assert "Read those failures" in breaker

    def test_an_expired_breaker_is_not_rendered(self):
        state = {
            "backlog_breakers": {
                "researchers": {"until": (NOW - timedelta(minutes=1)).isoformat()}
            }
        }
        assert not [
            line for line in pool_status_lines(META, state, NOW) if "BREAKER" in line
        ]

    def test_stalled_claims_say_they_will_not_self_release(self):
        # Auto-release is deliberately rejected — it recreates the silent
        # duplicate-execution failure. So the line has to tell him it is his.
        state = {
            "backlog_stale_claims": [
                {
                    "job_id": "1ad5d2a0-1111-2222-3333-444444444444",
                    "ticket_note_id": "feature-a",
                    "status": "pending_review",
                    "age_hours": 27.0,
                }
            ]
        }
        line = next(
            line
            for line in pool_status_lines(META, state, NOW)
            if "Claimed but stalled" in line
        )
        assert "feature-a" in line and "1ad5d2a0" in line and "27.0h" in line
        assert "NOT released automatically" in line

    def test_a_long_stall_list_is_capped_and_says_so(self):
        state = {
            "backlog_stale_claims": [
                {
                    "job_id": f"job{i}",
                    "ticket_note_id": f"t{i}",
                    "status": "paused",
                    "age_hours": 5,
                }
                for i in range(8)
            ]
        }
        line = next(
            line
            for line in pool_status_lines(META, state, NOW)
            if "Claimed but stalled" in line
        )
        assert "(+3 more)" in line

    def test_auto_pull_off_is_stated_so_idleness_is_not_a_mystery(self):
        meta = {**META, "auto_pull": False}
        lines = pool_status_lines(meta, {}, NOW)
        assert any("Auto-pull: OFF" in line for line in lines)

    def test_auto_pull_on_and_quiet_renders_nothing(self):
        assert pool_status_lines(META, {}, NOW) == []

    def test_a_century_without_pools_renders_nothing(self):
        assert pool_status_lines({"slots": {"line": {"count": 1}}}, {}, NOW) == []


# =============================================================================
# Ready depth reuses the tick's own eligibility, not a cheaper count
# =============================================================================


def _vector_db_from_conn(conn):
    acq = MagicMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    vector_db = MagicMock()
    vector_db.acquire = MagicMock(return_value=acq)
    return vector_db


def _vector_db(rows):
    conn = MagicMock()
    # The BP-12 summary path reads every requested category in one statement.
    conn.fetch = AsyncMock(return_value=rows)
    return _vector_db_from_conn(conn)


class TestReadyDepth:
    @pytest.mark.asyncio
    async def test_empty_vector_result_is_exact_zero_with_one_query(self):
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=[])
        db = AsyncMock()

        depth = await ready_depth_by_pool(
            db, _vector_db_from_conn(conn), str(uuid.uuid4()), POOLS, now=NOW
        )

        assert depth == {"researchers": 0, "executors": 0}
        assert conn.fetch.await_count == 1
        db.ticket_claim_states.assert_not_awaited()
        db.unresolved_knowledge_note_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_max_roster_is_one_three_query_batch_independent_of_pool_count(self):
        categories = ("researcher", "tester", "executor")
        pools = {
            f"pool-{index:02d}": {
                "count": 20,
                "category": categories[index % len(categories)],
            }
            for index in range(8)
        }
        rows = [
            {
                "note_id": f"{category}-{index}",
                "tags": ["ready", f"category:{category}"],
                "ready_at": NOW,
            }
            for category in categories
            for index in range(2)
        ]
        conn = MagicMock()
        conn.fetch = AsyncMock(return_value=rows)
        db = AsyncMock()
        db.ticket_claim_states.return_value = {}
        db.unresolved_knowledge_note_ids.return_value = set()

        depth = await ready_depth_by_pool(
            db, _vector_db_from_conn(conn), str(uuid.uuid4()), pools, now=NOW
        )

        assert depth == {pool: 2 for pool in pools}
        assert conn.fetch.await_count == 1
        assert db.ticket_claim_states.await_count == 1
        assert db.unresolved_knowledge_note_ids.await_count == 1

    @pytest.mark.asyncio
    async def test_batch_preserves_claim_ambiguity_and_materialization_semantics(self):
        rows = [
            {
                "note_id": "research-ok",
                "tags": ["ready", "category:researcher"],
                "ready_at": NOW,
            },
            {
                "note_id": "test-claimed",
                "tags": ["ready", "category:tester"],
                "ready_at": NOW - timedelta(hours=2),
            },
            {
                "note_id": "ambiguous",
                "tags": [
                    "ready",
                    "category:researcher",
                    "category:tester",
                ],
                "ready_at": NOW,
            },
            {
                "note_id": "executor-pending-sync",
                "tags": ["ready", "category:executor"],
                "ready_at": NOW,
            },
        ]
        db = AsyncMock()
        db.ticket_claim_states.return_value = {
            "test-claimed": {
                "ready_generation_at": NOW - timedelta(hours=1),
                "has_non_terminal": False,
            }
        }
        db.unresolved_knowledge_note_ids.return_value = {"executor-pending-sync"}

        depth = await ready_depth_by_pool(
            db, _vector_db(rows), str(uuid.uuid4()), POOLS, now=NOW
        )

        assert depth == {"researchers": 1, "executors": 0}

    @pytest.mark.asyncio
    async def test_candidate_ceiling_is_unavailable_not_a_truncated_depth(
        self, monkeypatch
    ):
        import orchestrator.services.officer_backlog as module

        monkeypatch.setattr(module, "_READY_DEPTH_MAX_CANDIDATES", 2)
        rows = [
            {
                "note_id": f"ticket-{index}",
                "tags": ["ready", "category:researcher"],
                "ready_at": NOW,
            }
            for index in range(3)
        ]
        db = AsyncMock()

        depth = await ready_depth_by_pool(
            db,
            _vector_db(rows),
            str(uuid.uuid4()),
            {"researchers": POOLS["researchers"]},
            now=NOW,
        )

        assert depth == {}
        db.ticket_claim_states.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_concurrent_viewers_share_only_the_inflight_observation(self):
        fetch_started = asyncio.Event()
        release_fetch = asyncio.Event()
        conn = MagicMock()

        async def _fetch(*_args):
            fetch_started.set()
            await release_fetch.wait()
            return [
                {
                    "note_id": "ticket-a",
                    "tags": ["ready", "category:researcher"],
                    "ready_at": NOW,
                }
            ]

        conn.fetch = AsyncMock(side_effect=_fetch)
        vector_db = _vector_db_from_conn(conn)
        db = AsyncMock()
        db.ticket_claim_states.return_value = {}
        db.unresolved_knowledge_note_ids.return_value = set()
        project_id = str(uuid.uuid4())
        pool = {"researchers": POOLS["researchers"]}

        viewers = [
            asyncio.create_task(
                ready_depth_by_pool(
                    db, vector_db, project_id, pool, caller="officer_summary"
                )
            )
            for _ in range(12)
        ]
        await fetch_started.wait()
        await asyncio.sleep(0)
        release_fetch.set()
        results = await asyncio.gather(*viewers)

        assert results == [{"researchers": 1}] * 12
        assert conn.fetch.await_count == 1
        assert db.ticket_claim_states.await_count == 1
        assert db.unresolved_knowledge_note_ids.await_count == 1

        # Once complete, the observation is not retained as a stale cache.
        await ready_depth_by_pool(db, vector_db, project_id, pool)
        assert conn.fetch.await_count == 2

    @pytest.mark.asyncio
    async def test_more_than_twenty_five_candidates_are_counted_exactly(self):
        rows = [
            {
                "note_id": f"ticket-{index:02d}",
                "priority": 1,
                "tags": ["ready", "category:researcher"],
                "ready_at": NOW,
            }
            for index in range(30)
        ]
        db = AsyncMock()
        db.ticket_claim_states.return_value = {}

        depth = await ready_depth_by_pool(
            db,
            _vector_db(rows),
            str(uuid.uuid4()),
            {"researchers": POOLS["researchers"]},
            now=NOW,
        )

        assert depth == {"researchers": 30}

    @pytest.mark.asyncio
    async def test_counts_only_what_the_tick_would_actually_take(self):
        # A "ready 4" the tick reads as 1 would have the officer waiting for
        # dispatches that are never coming.
        rows = [
            {
                "note_id": "ok",
                "tags": ["ready", "category:researcher"],
                "ready_at": NOW,
            },
            {  # claimed since it was armed
                "note_id": "claimed",
                "tags": ["ready", "category:researcher"],
                "ready_at": NOW - timedelta(hours=2),
            },
            {  # ambiguous
                "note_id": "ambiguous",
                "tags": ["ready", "category:researcher", "category:executor"],
                "ready_at": NOW,
            },
            {  # armed tag, no timestamp -> fails closed
                "note_id": "unauthorized",
                "tags": ["ready", "category:researcher"],
                "ready_at": None,
            },
        ]
        db = AsyncMock()
        db.ticket_claim_states.return_value = {
            "claimed": {
                "ready_generation_at": NOW - timedelta(hours=1),
                "has_non_terminal": False,
            }
        }
        depth = await ready_depth_by_pool(
            db,
            _vector_db(rows),
            str(uuid.uuid4()),
            {"researchers": POOLS["researchers"]},
            now=NOW,
        )
        assert depth == {"researchers": 1}

    @pytest.mark.asyncio
    async def test_a_kb_outage_omits_the_pool_rather_than_reporting_zero(self):
        db = AsyncMock()
        vector_db = MagicMock()
        vector_db.acquire = MagicMock(side_effect=RuntimeError("pgvector down"))
        depth = await ready_depth_by_pool(
            db, vector_db, str(uuid.uuid4()), POOLS, now=NOW
        )
        # Zero would read as "starved queue, go file tickets"; absent reads as
        # "unknown", which is the truth.
        assert depth == {}


# =============================================================================
# Slot spec — category and the optional per-slot ceiling
# =============================================================================


class TestPrecedenceLaw:
    """§6: the slot's category decides the contract; a mismatch is named."""

    def _compose(self, *args, **kwargs):
        # The officer admission service owns the kickoff composition (the
        # former ``main._compose_category_kickoff`` was a re-export of it).
        from orchestrator.services.job_admission_officer import (
            compose_category_kickoff,
        )

        return compose_category_kickoff(*args, **kwargs)

    def test_a_pool_slot_resolves_its_category(self):
        assert officer_slot_category(META, "researchers") == "researcher"
        assert officer_slot_category(META, "executors") == "executor"

    def test_an_uncategorized_or_unknown_slot_has_none(self):
        assert officer_slot_category({"slots": {"a": {"count": 1}}}, "a") is None
        assert officer_slot_category(META, "nope") is None
        assert officer_slot_category(META, None) is None

    def test_the_contract_leads_and_the_officer_brief_follows(self):
        text = self._compose("researcher", "Work ticket feature-a.")
        assert text.startswith("Your deliverable is an ANSWER")
        assert text.rstrip().endswith("Work ticket feature-a.")

    def test_a_matching_category_adds_no_noise(self):
        text = self._compose("researcher", "brief", requested_category="researcher")
        assert "NOTE:" not in text

    def test_a_cross_category_dispatch_is_named_not_refused(self):
        # Warn-not-forbid. The worker must never read an executor's delivery
        # contract while sitting in a researcher slot with no way to tell which
        # one the officer meant.
        text = self._compose(
            "researcher",
            "brief",
            requested_category="executor",
            slot="researchers",
        )
        assert text.startswith("Your deliverable is an ANSWER")
        assert "dispatched this as executor work into the researchers slot" in text
        assert "the contract above is the one you are held to" in text
        assert "say so in your completion report" in text

    def test_no_kickoff_still_yields_the_contract(self):
        assert "ANSWER" in self._compose("researcher", None)


class TestBacklogDoctrineMatchesTheMachinery:
    """The charter posture text (§8) vs what the tick actually does.

    Doctrine that disagrees with the code is worse than no doctrine: the
    officer follows the words, the machinery does something else, and the gap
    only shows up as behaviour nobody can explain. These pin the load-bearing
    claims against the constants they describe, so changing one without the
    other fails here rather than in a century.
    """

    def _doctrine(self):
        from pathlib import Path

        text = Path("config/experts/centurion/persona.txt").read_text()
        assert "<backlog_doctrine>" in text, "backlog doctrine block is missing"
        return text.split("<backlog_doctrine>")[1].split("</backlog_doctrine>")[0]

    def test_the_floor_it_states_is_the_floor_the_tick_enforces(self):
        # §13.2: floor == the pool's slot count, not a constant.
        doctrine = self._doctrine()
        assert "at least as many tickets as that pool has slots" in doctrine

    def test_it_states_slack_is_healthy_not_a_utilization_target(self):
        doctrine = self._doctrine()
        assert "idle slot with a healthy queue is slack" in doctrine
        assert "Utilization is not the target" in doctrine

    def test_it_states_the_anti_amplification_firewall(self):
        # The one invariant separating a century from an agent that spawns
        # agents. If this line ever softens, so does the firewall.
        doctrine = self._doctrine()
        assert "Nothing dispatches until YOU stamp it ready" in doctrine
        assert "no bulk-ready" in doctrine

    def test_it_states_one_shot_claims_the_way_the_tick_implements_them(self):
        doctrine = self._doctrine()
        assert "Dispatch consumes readiness" in doctrine
        # Terminal outcomes hold the claim — the whole point.
        assert "in any outcome, success or failure" in doctrine

    def test_it_promises_no_auto_release_which_is_what_the_tick_does(self):
        doctrine = self._doctrine()
        assert "Claims are never released for you" in doctrine
        assert "Two jobs must never work one ticket" in doctrine

    def test_the_breaker_window_it_quotes_matches_the_constant(self):
        from orchestrator.services.officer_backlog import (
            BREAKER_FAILURES,
            BREAKER_OPEN_MINUTES,
        )

        doctrine = self._doctrine()
        assert BREAKER_FAILURES == 2 and "fails twice in a row" in doctrine
        assert f"{int(BREAKER_OPEN_MINUTES)} minutes" in doctrine or (
            "thirty minutes" in doctrine and BREAKER_OPEN_MINUTES == 30
        )
        # Per-pool, never global.
        assert "that pool only" in doctrine

    def test_it_carries_the_evidence_repricing(self):
        doctrine = self._doctrine()
        assert "An answer is a deliverable, a screenshot is evidence" in doctrine
        assert "regression rails rather than a score" in doctrine
        # The sentence that names the actual failure mode.
        assert "most likely to have been skipped" in doctrine

    def test_the_doctrine_has_no_format_placeholders(self):
        # The persona is .format()-ed with agent_display_name; a stray brace in
        # this block would raise KeyError at every officer spawn.
        doctrine = self._doctrine()
        assert "{" not in doctrine and "}" not in doctrine


class TestSlotSpec:
    def test_a_category_makes_a_slot_a_pool(self):
        cleaned = validate_slots_spec({"r": {"count": 2, "category": "Researcher"}})
        assert cleaned["r"]["category"] == "researcher"

    def test_an_unknown_category_fails_at_provision_not_at_dispatch(self):
        with pytest.raises(ValueError, match="category must be one of"):
            validate_slots_spec({"r": {"count": 1, "category": "designer"}})

    def test_the_spend_ceiling_is_optional_and_positive(self):
        assert (
            "spend_ceiling_daily" not in validate_slots_spec({"r": {"count": 1}})["r"]
        )
        assert (
            validate_slots_spec({"r": {"count": 1, "spend_ceiling_daily": 15}})["r"][
                "spend_ceiling_daily"
            ]
            == 15.0
        )
        with pytest.raises(ValueError, match="must be positive"):
            validate_slots_spec({"r": {"count": 1, "spend_ceiling_daily": 0}})

    @pytest.mark.parametrize("value", [True, "nan", "inf", float("nan")])
    def test_spend_ceiling_rejects_boolean_or_non_finite_values(self, value):
        with pytest.raises(ValueError, match="spend_ceiling_daily"):
            validate_slots_spec({"r": {"count": 1, "spend_ceiling_daily": value}})
