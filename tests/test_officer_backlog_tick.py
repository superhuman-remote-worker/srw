"""Officer auto-pull tick — B3 of knowledge-base/knowledge/features/officer_backlog_pools.md.

The tick spends money and creates work unattended, so the tests that matter
here are the ones about NOT dispatching. In rough order of how expensive the
bug would be:

* A terminal job still holds its ticket (one-shot claims). The original design
  released the claim on terminal, which re-dispatched every completed ticket a
  minute later and re-burned every failing one at breaker cadence, forever.
* Claim predicates count paused/pending-review jobs; capacity does NOT count
  paused (owner ruling 2026-08-18: a paused job keeps its ticket but vacates
  its slot — two paused zombies starved a whole kit on that date). A resume
  briefly overlapping a fresh dispatch is the accepted cost.
* Honest failure reports never open a breaker; only job failures do.
* A `ready` tag with no `ready_at` fails closed.
"""

import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from orchestrator.services.officer_backlog import (
    STALE_CLAIM_HOURS,
    _scan_eligible_tickets,
    auto_pull_enabled,
    breaker_is_open,
    eligible_tickets,
    evaluate_breaker,
    officer_backlog_tick_once,
    pools_from_meta,
    stale_claims,
    tick_officer as _tick_officer,
)
from orchestrator.services.work_categories import EXECUTOR, RESEARCHER

NOW = datetime(2026, 8, 15, 12, 0, tzinfo=timezone.utc)
OFFICER_THREAD_ID = "11111111-1111-1111-1111-111111111111"
OFFICER_PROJECT_ID = "22222222-2222-2222-2222-222222222222"
OWNER_ID = "33333333-3333-3333-3333-333333333333"
KB_DS = "44444444-4444-4444-4444-444444444444"
REPO_DS = "55555555-5555-5555-5555-555555555555"


async def _noop_provision(_job, *, category=None):
    return None


async def tick_officer(*args, **kwargs):
    """Unit default mirrors the lifespan's configured provisioner."""
    kwargs.setdefault("provision_repo", _noop_provision)
    # Existing behavioral tests exercise a deliberately released century.
    # Production defaults dark; dedicated tests below prove that boundary.
    kwargs.setdefault("release_enabled", True)
    return await _tick_officer(*args, **kwargs)


def _row(note_id, *, tags, ready_at=None, title="T", note_type="feature"):
    return {
        "note_id": note_id,
        "note_type": note_type,
        "title": title,
        "priority": 1,
        "tags": list(tags),
        "ready_at": ready_at,
    }


# =============================================================================
# Configuration gates — the tick is dormant unless deliberately switched on
# =============================================================================


class TestAutoPullGate:
    def test_ships_off(self):
        # §13.1: a century whose officer has not triaged a backlog must not
        # start pulling whatever happens to be tagged.
        assert auto_pull_enabled({}) is False
        assert auto_pull_enabled({"auto_pull": False}) is False

    def test_accepts_the_shapes_json_round_trips_produce(self):
        for value in (True, "true", "True", 1):
            assert auto_pull_enabled({"auto_pull": value}) is True

    def test_only_categorized_slots_are_pools(self):
        # A slot without a category keeps today's behaviour exactly:
        # officer-directed capacity the tick never touches.
        pools = pools_from_meta(
            {
                "slots": {
                    "researchers": {"count": 2, "category": "researcher"},
                    "adhoc": {"count": 1},
                }
            }
        )
        assert set(pools) == {"researchers"}

    def test_no_roster_means_no_pools(self):
        assert pools_from_meta({}) == {}
        assert pools_from_meta({"max_concurrent_workers": 3}) == {}

    @pytest.mark.asyncio
    async def test_scheduler_enumerates_only_commissioned_posts(self):
        db = AsyncMock()
        db.list_commissioned_officer_posts_for_backlog.return_value = []
        db.list_officer_threads.side_effect = AssertionError(
            "runtime officer enumeration is not backlog authority"
        )

        totals = await officer_backlog_tick_once(db, MagicMock())

        assert totals == {
            "dispatched": 0,
            "skipped": 0,
            "breakers_opened": 0,
            "wakes": 0,
        }
        db.list_commissioned_officer_posts_for_backlog.assert_awaited_once()
        db.list_officer_threads.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_deployment_release_gate_blocks_new_queue_reads(self):
        db = _db()
        vector_db = MagicMock()
        vector_db.acquire.side_effect = AssertionError(
            "a dark release gate must not read the ticket queue"
        )

        counts = await _tick_officer(
            db,
            vector_db,
            _officer_row(),
            now=NOW,
            release_enabled=False,
            provision_repo=_noop_provision,
        )

        assert counts == {
            "dispatched": 0,
            "skipped": 0,
            "breakers_opened": 0,
            "wakes": 0,
        }
        db.create_job.assert_not_awaited()
        vector_db.acquire.assert_not_called()
        # BP-07 work was already admitted and owns its claim/capacity. Its
        # recovery stays live while new unattended admission is dark.
        db.list_officer_job_preflights.assert_awaited_once()


# =============================================================================
# One-shot claims — the bug that would have burned the most money
# =============================================================================


class TestEligibility:
    def test_an_armed_unclaimed_ticket_is_eligible(self):
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        ready, notes = eligible_tickets(rows, {}, NOW)
        assert [t["note_id"] for t in ready] == ["feature-a"]
        assert notes == []

    def test_a_terminal_job_still_holds_its_ticket(self):
        # THE one-shot rule. Disposition is asynchronous and officer-owned, so
        # a completed job holds its claim until he reviews and re-arms.
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher"],
                ready_at=NOW - timedelta(hours=3),
            )
        ]
        claims = {"feature-a": NOW - timedelta(hours=2)}  # dispatched after arming
        ready, notes = eligible_tickets(rows, claims, NOW)
        assert ready == []
        assert "claimed at" in notes[0]

    def test_re_arming_after_the_claim_makes_it_eligible_again(self):
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        claims = {"feature-a": NOW - timedelta(hours=2)}  # armed since
        ready, _ = eligible_tickets(rows, claims, NOW)
        assert [t["note_id"] for t in ready] == ["feature-a"]

    def test_legacy_claim_requires_an_explicit_post_cutover_rearm(self):
        barrier = NOW - timedelta(hours=1)
        claims = {
            "feature-a": {
                "ready_generation_at": None,
                "legacy_rearm_after": barrier,
                "has_non_terminal": False,
            }
        }
        old_ready = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher"],
                ready_at=barrier,
            )
        ]
        ready, notes = eligible_tickets(old_ready, claims, NOW)
        assert ready == []
        assert "legacy claim requires re-ready" in notes[0]

        rearmed = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher"],
                ready_at=barrier + timedelta(microseconds=1),
            )
        ]
        ready, notes = eligible_tickets(rearmed, claims, NOW)
        assert [ticket["note_id"] for ticket in ready] == ["feature-a"]
        assert notes == []

    def test_a_claim_exactly_at_the_arming_instant_counts_as_claimed(self):
        # Ties go to "claimed": dispatching twice costs a job, refusing costs
        # one tick of latency.
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        ready, _ = eligible_tickets(rows, {"feature-a": NOW}, NOW)
        assert ready == []

    def test_ready_tag_without_ready_at_fails_closed(self):
        # The post-vault-rebuild case. A dispatch authorization with no
        # timestamp is not an authorization.
        rows = [_row("feature-a", tags=["ready", "category:researcher"])]
        ready, notes = eligible_tickets(rows, {}, NOW)
        assert ready == []
        assert "no ready_at" in notes[0]

    def test_an_ambiguous_ticket_is_reported_not_guessed(self):
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher", "category:executor"],
                ready_at=NOW,
            )
        ]
        ready, notes = eligible_tickets(rows, {}, NOW)
        assert ready == []
        assert "multiple category" in notes[0]

    def test_a_typo_in_the_expert_pin_never_reaches_agent_boot(self):
        # It would look like a job failure there and chain-trip the breaker on
        # a spelling mistake.
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher", "expert:scholr"],
                ready_at=NOW,
            )
        ]
        ready, notes = eligible_tickets(rows, {}, NOW)
        assert ready == []
        assert "unknown expert" in notes[0]

    def test_ordering_is_preserved_so_priority_still_leads(self):
        rows = [
            _row("high", tags=["ready", "category:researcher"], ready_at=NOW),
            _row("low", tags=["ready", "category:researcher"], ready_at=NOW),
        ]
        ready, _ = eligible_tickets(rows, {}, NOW)
        assert [t["note_id"] for t in ready] == ["high", "low"]


# =============================================================================
# Circuit breaker — honesty must not be punished
# =============================================================================


def _terminal(job_id, ticket, status):
    return {"id": job_id, "ticket_note_id": ticket, "status": status}


class TestBreaker:
    def test_two_failures_on_distinct_tickets_trip_it(self):
        history = [_terminal("j2", "t2", "failed"), _terminal("j1", "t1", "failed")]
        tripped = evaluate_breaker(history, {}, "line")
        assert tripped and tripped["tripped_on_job"] == "j2"
        assert tripped["tickets"] == ["t2", "t1"]

    def test_honest_negative_completions_never_trip_it(self):
        # A worker reporting goal_achieved=false completes SUCCESSFULLY. If
        # two of those opened a breaker, the fleet would learn to stop filing
        # honest negatives — the exact incentive this design fights.
        history = [
            _terminal("j2", "t2", "completed"),
            _terminal("j1", "t1", "completed"),
        ]
        assert evaluate_breaker(history, {}, "line") is None

    def test_blocked_undelivered_outcomes_never_trip_it(self):
        history = [
            {
                **_terminal("j3", "t3", "cancelled"),
                "completion_outcome_kind": "blocked_undelivered",
            },
            {
                **_terminal("j2", "t2", "cancelled"),
                "completion_outcome_kind": "blocked_undelivered",
            },
            _terminal("j1", "t1", "failed"),
        ]
        assert evaluate_breaker(history, {}, "line") is None

    def test_a_success_between_two_failures_breaks_the_chain(self):
        history = [
            _terminal("j3", "t3", "failed"),
            _terminal("j2", "t2", "completed"),
            _terminal("j1", "t1", "failed"),
        ]
        assert evaluate_breaker(history, {}, "line") is None

    def test_two_failures_on_the_SAME_ticket_are_one_incident(self):
        history = [_terminal("j2", "t1", "failed"), _terminal("j1", "t1", "failed")]
        assert evaluate_breaker(history, {}, "line") is None

    def test_it_does_not_re_trip_on_the_failure_it_already_tripped_on(self):
        # Derived-from-history alone would re-open the breaker every 30 minutes
        # forever once those two rows stop changing.
        history = [_terminal("j2", "t2", "failed"), _terminal("j1", "t1", "failed")]
        state = {"backlog_breakers": {"line": {"tripped_on_job": "j2"}}}
        assert evaluate_breaker(history, state, "line") is None

    def test_a_newer_failure_after_the_window_trips_again(self):
        history = [_terminal("j3", "t3", "failed"), _terminal("j2", "t2", "failed")]
        state = {"backlog_breakers": {"line": {"tripped_on_job": "j2"}}}
        tripped = evaluate_breaker(history, state, "line")
        assert tripped and tripped["tripped_on_job"] == "j3"

    def test_too_little_history_never_trips(self):
        assert evaluate_breaker([_terminal("j1", "t1", "failed")], {}, "line") is None
        assert evaluate_breaker([], {}, "line") is None

    def test_open_is_per_pool_and_expires(self):
        state = {
            "backlog_breakers": {
                "line": {"until": (NOW + timedelta(minutes=5)).isoformat()},
                "heavy": {"until": (NOW - timedelta(minutes=5)).isoformat()},
            }
        }
        assert breaker_is_open(state, "line", NOW) is True
        assert breaker_is_open(state, "heavy", NOW) is False
        # A burning research pool must not stop the executor from shipping.
        assert breaker_is_open(state, "executors", NOW) is False


# =============================================================================
# Stale claims — surfaced, never auto-released
# =============================================================================


class TestStaleClaims:
    def test_a_claim_that_has_not_moved_is_surfaced_oldest_first(self):
        claims = [
            {
                "id": "j1",
                "ticket_note_id": "t1",
                "officer_slot": "line",
                "status": "paused",
                "updated_at": NOW - timedelta(hours=STALE_CLAIM_HOURS + 2),
            },
            {
                "id": "j2",
                "ticket_note_id": "t2",
                "officer_slot": "line",
                "status": "pending_review",
                "updated_at": NOW - timedelta(hours=STALE_CLAIM_HOURS + 10),
            },
        ]
        out = stale_claims(claims, NOW)
        assert [c["job_id"] for c in out] == ["j2", "j1"]
        assert out[0]["age_hours"] >= STALE_CLAIM_HOURS

    def test_a_fresh_claim_is_not_stale(self):
        claims = [
            {
                "id": "j1",
                "ticket_note_id": "t1",
                "status": "processing",
                "updated_at": NOW - timedelta(minutes=30),
            }
        ]
        assert stale_claims(claims, NOW) == []

    def test_deployment_stale_claim_policy_is_shared_and_configurable(
        self, monkeypatch
    ):
        monkeypatch.setenv("JOB_LIVENESS_STALE_CLAIM_MINUTES", "300")
        claim = {
            "id": "j1",
            "ticket_note_id": "t1",
            "status": "paused",
            "updated_at": NOW - timedelta(minutes=270),
        }
        assert stale_claims([claim], NOW) == []
        claim["updated_at"] = NOW - timedelta(minutes=301)
        assert [row["job_id"] for row in stale_claims([claim], NOW)] == ["j1"]


# =============================================================================
# The tick end to end, against a doubled DB
# =============================================================================


def _officer_row(**over):
    row = {
        "id": OFFICER_THREAD_ID,
        "project_id": OFFICER_PROJECT_ID,
        "status": "active",
        "metadata": {
            "config_override": {
                "officer": {
                    "enabled": True,
                    "auto_pull": True,
                    "slots": {
                        "researchers": {"count": 1, "category": RESEARCHER},
                    },
                }
            }
        },
    }
    row.update(over)
    return row


def _db(
    *,
    claims=None,
    slot_claims=None,
    locked_claim_at=None,
    locked_legacy_rearm_after=None,
    locked_has_non_terminal=False,
    slots=None,
):
    """A doubled PostgresDB.

    ``locked_claim_at`` is what the in-transaction re-read of the claim ledger
    returns — the racing-replica case. None means "still unclaimed".

    ``slots`` overrides the runtime roster so a test can pin a worker tier —
    connector resolution must measure against the worker's backend, never the
    officer's.
    """
    db = AsyncMock()
    db.get_officer_capacity_lineage.return_value = [OFFICER_THREAD_ID]
    db.get_project.return_value = None
    # No project_workspace_defaults row: the installation decides the tier.
    db.fetchrow.return_value = None
    db.ticket_claim_states.return_value = claims or {}
    db.list_officer_slot_claims.return_value = slot_claims or []
    db.list_officer_distinct_terminal_outcomes.return_value = []
    db.list_stale_officer_claims.return_value = []
    db.get_oldest_open_officer_claim.return_value = None
    db.merge_thread_officer_state.return_value = True
    db.list_officer_job_preflights.return_value = []
    db.insert_officer_ticket_claim.return_value = {
        "ticket_note_id": "feature-a",
        "ready_generation_at": NOW,
    }

    created = {}

    async def _create_job(**kwargs):
        created.update(kwargs)
        return {"id": str(kwargs.get("job_id") or uuid.uuid4()), **kwargs}

    db.create_job.side_effect = _create_job
    db.created = created

    async def _claim_preflight(job_id, **kwargs):
        return {
            "id": job_id,
            "context": {
                "work_category": created.get("context", {}).get("work_category"),
                "provisioning_preflight": {"state": "in-progress"},
            },
            "preflight_attempt_token": str(uuid.uuid4()),
        }

    db.claim_officer_job_preflight.side_effect = _claim_preflight
    db.finish_officer_job_preflight.return_value = True
    db.get_job.side_effect = lambda job_id: {
        "id": job_id,
        **created,
        "context": {
            **(created.get("context") or {}),
            "provisioning_preflight": {"state": "activated"},
        },
    }

    async def _floor_wake(project_id, *, notifier=None, **kwargs):
        if notifier is None:
            return {"attempted": True, "queued": False, "state": "retryable"}
        result = await notifier(
            db,
            project_id,
            source="backlog_floor_breach",
            dedup_key=f"floor:{project_id}:{kwargs['pool']}:episode",
            payload=kwargs.get("payload"),
        )
        return {
            "attempted": True,
            "queued": bool(result),
            "state": "queued" if result else "retryable",
        }

    db.queue_officer_floor_wake.side_effect = _floor_wake
    db.resolve_officer_floor_wake_retry.return_value = False

    conn = MagicMock()
    conn.execute = AsyncMock()
    conn.fetch = AsyncMock(return_value=[])
    conn.fetchval = AsyncMock(return_value=None)

    runtime_metadata = {
        "config_override": {
            "officer": {
                "enabled": True,
                "auto_pull": True,
                "slots": slots
                or {
                    "researchers": {"count": 1, "category": RESEARCHER},
                    "executors": {"count": 1, "category": EXECUTOR},
                },
            }
        }
    }
    post = {
        "project_id": uuid.UUID(OFFICER_PROJECT_ID),
        "thread_id": uuid.UUID(OFFICER_THREAD_ID),
        "config_override": {},
        "incarnations": [],
        "updated_at": NOW,
    }
    thread = {
        "id": uuid.UUID(OFFICER_THREAD_ID),
        "project_id": uuid.UUID(OFFICER_PROJECT_ID),
        "status": "active",
        "metadata": runtime_metadata,
        "user_id": None,
        "created_at": NOW - timedelta(days=1),
    }

    async def _fetchrow(query, *args):
        if "LEFT JOIN threads" in query:
            return {
                "project_id": post["project_id"],
                "thread_id": post["thread_id"],
                "config_override": post["config_override"],
                "incarnations": post["incarnations"],
                "post_updated_at": post["updated_at"],
                "current_thread_id": thread["id"],
                "thread_project_id": thread["project_id"],
                "thread_status": thread["status"],
                "thread_metadata": thread["metadata"],
                "thread_user_id": thread["user_id"],
                "thread_created_at": thread["created_at"],
            }
        if "FROM project_officers" in query and "FOR UPDATE" in query:
            return post
        if "FROM threads" in query and "FOR UPDATE" in query:
            return thread
        if "MAX(claim.ready_generation_at) AS newest_generation" in query:
            return {
                "newest_generation": locked_claim_at,
                "legacy_rearm_after": locked_legacy_rearm_after,
                "has_non_terminal": locked_has_non_terminal,
            }
        raise AssertionError(query)

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    tx = MagicMock()
    tx.__aenter__ = AsyncMock(return_value=None)
    tx.__aexit__ = AsyncMock(return_value=False)
    conn.transaction = MagicMock(return_value=tx)
    acq = MagicMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    db.acquire = MagicMock(return_value=acq)
    return db


def _vector_db(rows):
    conn = MagicMock()
    conn.fetch = AsyncMock(side_effect=[rows, []])
    conn.fetchrow = AsyncMock(return_value=None)
    acq = MagicMock()
    acq.__aenter__ = AsyncMock(return_value=conn)
    acq.__aexit__ = AsyncMock(return_value=False)
    vector_db = MagicMock()
    vector_db.acquire = MagicMock(return_value=acq)
    return vector_db


class TestTickOfficer:
    @pytest.mark.asyncio
    async def test_eleven_claimed_head_tickets_do_not_hide_a_valid_tail(self):
        claimed = {f"claimed-{index:02d}": NOW for index in range(11)}
        db = _db(claims=claimed)
        rows = [
            _row(
                note_id,
                tags=["ready", "category:researcher"],
                ready_at=NOW,
            )
            for note_id in claimed
        ] + [
            _row(
                "valid-tail",
                tags=["ready", "category:researcher"],
                ready_at=NOW,
            )
        ]

        counts = await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)

        assert counts["dispatched"] == 1
        assert db.created["context"]["ticket_note_id"] == "valid-tail"

    @pytest.mark.asyncio
    async def test_app_database_failure_is_unavailable_not_empty(self):
        db = _db()
        db.ticket_claim_states.side_effect = RuntimeError("app database down")
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]

        scan = await _scan_eligible_tickets(
            db,
            _vector_db(rows),
            OFFICER_PROJECT_ID,
            RESEARCHER,
            NOW,
            minimum=None,
        )

        assert scan.unavailable is True
        assert scan.exhausted is False
        assert scan.tickets == []
        assert "app-state" in (scan.error or "")

    @pytest.mark.asyncio
    async def test_pending_canonical_ready_write_is_ineligible(self):
        db = _db()
        db.unresolved_knowledge_note_ids.return_value = {"feature-pending"}
        rows = [
            _row(
                "feature-pending",
                tags=["ready", "category:researcher"],
                ready_at=NOW,
            )
        ]

        scan = await _scan_eligible_tickets(
            db,
            _vector_db(rows),
            OFFICER_PROJECT_ID,
            RESEARCHER,
            NOW,
            minimum=None,
        )

        assert scan.exhausted is True
        assert scan.tickets == []
        assert scan.notes == ["feature-pending: canonical knowledge sync unresolved"]

    @pytest.mark.asyncio
    async def test_exact_exhaustion_is_distinct_from_unavailable(self):
        scan = await _scan_eligible_tickets(
            _db(),
            _vector_db([]),
            OFFICER_PROJECT_ID,
            RESEARCHER,
            NOW,
            minimum=None,
        )

        assert scan.exhausted is True
        assert scan.lower_bound is False
        assert scan.unavailable is False
        assert scan.tickets == []

    @pytest.mark.asyncio
    async def test_sufficient_candidate_result_is_explicitly_a_lower_bound(self):
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher"],
                ready_at=NOW,
            )
        ]
        scan = await _scan_eligible_tickets(
            _db(),
            _vector_db(rows),
            OFFICER_PROJECT_ID,
            RESEARCHER,
            NOW,
            minimum=1,
        )

        assert scan.lower_bound is True
        assert scan.exhausted is False
        assert scan.unavailable is False
        assert len(scan.tickets) == 1

    @pytest.mark.asyncio
    async def test_equal_priority_timestamp_page_boundary_has_no_gap_or_duplicate(
        self, monkeypatch
    ):
        import orchestrator.services.officer_backlog as module

        created_at = NOW - timedelta(days=1)
        first = [
            {
                **_row(
                    f"ticket-{index:03d}",
                    tags=["ready", "category:researcher"],
                    ready_at=NOW,
                ),
                "created_at": created_at,
            }
            for index in range(100)
        ]
        tail = [
            {
                **_row(
                    "ticket-100",
                    tags=["ready", "category:researcher"],
                    ready_at=NOW,
                ),
                "created_at": created_at,
            }
        ]
        cursors = []
        include_counts = []

        async def _fetch(_vector, _project, *, after=None, **_kwargs):
            cursors.append(after)
            include_counts.append(_kwargs.get("include_counts"))
            return (first if after is None else tail), {}

        monkeypatch.setattr(module, "fetch_backlog", _fetch)
        scan = await _scan_eligible_tickets(
            _db(), MagicMock(), OFFICER_PROJECT_ID, RESEARCHER, NOW, minimum=None
        )

        ids = [ticket["note_id"] for ticket in scan.tickets]
        assert ids == [f"ticket-{index:03d}" for index in range(101)]
        assert len(ids) == len(set(ids))
        assert cursors[1].note_id == "ticket-099"
        assert include_counts == [False, False]
        assert scan.exhausted is True

    @pytest.mark.asyncio
    async def test_dispatches_an_armed_ticket_with_the_category_contract(self):
        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db, _vector_db(rows), _officer_row(), now=NOW, notify=None
        )

        assert counts["dispatched"] == 1
        created = db.created
        assert created["context"]["ticket_note_id"] == "feature-a"
        assert created["context"]["work_category"] == RESEARCHER
        assert created["context"]["officer_slot"] == "researchers"
        # The contract rides the kickoff, never `instructions` — that parameter
        # replaces the rendered instructions.md template wholesale.
        assert (
            "ANSWER, not a product increment" in created["context"]["kickoff_message"]
        )
        assert "instructions" not in created
        # No expert pin on the ticket -> the category default.
        assert created["config_name"] == "scholar"
        # The INSERT shares the post-lock transaction; admission and the row
        # write therefore have one linearization point.
        assert created["conn"] is not None

    @pytest.mark.asyncio
    async def test_the_autonomy_exemption_rides_config_not_context(self):
        # Both grant PEPs read config_override. In context it would be inert,
        # and the completion would park in the pending_review dead zone holding
        # its claim.
        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert db.created["config_override"]["autonomy"] == "full"
        assert "autonomy" not in db.created["context"]

    @pytest.mark.asyncio
    async def test_runner_kind_is_a_value_the_check_constraint_accepts(self):
        # jobs_runner_kind_check allows user | lifecycle | service. `lifecycle`
        # is also the class whose grants carry the full autonomy ceiling the
        # stamp above depends on — a mocked DB would never have caught either.
        from pathlib import Path

        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert db.created["runner_kind"] == "lifecycle"

        schema = Path("src/orchestrator/database/schema_current.sql").read_text()
        constraint = next(
            line for line in schema.splitlines() if "jobs_runner_kind_check" in line
        )
        assert f"'{db.created['runner_kind']}'::text" in constraint

    @pytest.mark.asyncio
    async def test_a_claim_landing_under_the_lock_is_a_quiet_skip(self):
        # Two replicas ticking together is normal contention, not an incident.
        # The unique index would refuse the second INSERT anyway; re-reading the
        # ledger under the lock turns a stack trace into a log line.
        db = _db(locked_claim_at=NOW)
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_stale_claim_under_the_lock_does_not_block_a_re_armed_ticket(self):
        # The claim predates this arming, so the officer has re-readied since.
        db = _db(locked_claim_at=NOW - timedelta(hours=2))
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert counts["dispatched"] == 1

    @pytest.mark.asyncio
    async def test_a_legacy_barrier_landing_under_the_lock_is_a_quiet_skip(self):
        db = _db(locked_legacy_rearm_after=NOW)
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_post_cutover_rearm_passes_the_legacy_barrier_under_lock(self):
        db = _db(locked_legacy_rearm_after=NOW - timedelta(microseconds=1))
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert counts["dispatched"] == 1

    @pytest.mark.asyncio
    async def test_a_refused_grant_skips_the_pool_without_tripping_the_breaker(self):
        # The officer's kit exceeding what the owner may be granted is a real
        # refusal, but nothing ran — it is not a job failure.
        db = _db()

        async def _grants(config_override, *, user_id, project_ids):
            raise RuntimeError("GrantDenied: model not granted")

        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db,
            _vector_db(rows),
            _officer_row(user_id=str(uuid.uuid4())),
            now=NOW,
            enforce_grants=_grants,
        )
        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1
        assert counts["breakers_opened"] == 0

    @pytest.mark.asyncio
    async def test_an_expert_pin_overrides_the_category_default(self):
        db = _db()
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:researcher", "expert:designer"],
                ready_at=NOW,
            )
        ]
        await tick_officer(db, _vector_db(rows), _officer_row(), now=NOW)
        assert db.created["config_name"] == "designer"

    @pytest.mark.asyncio
    async def test_an_auto_pulled_job_carries_the_projects_connector_defaults(
        self, monkeypatch
    ):
        """Without this the tick creates jobs with no connectors at all.

        The REST create path resolves them (`2afbf956`), but this path bypasses
        it to keep the claim INSERT in the post-lock transaction — so the fix
        did not reach here. A worker dispatched with an empty selection has no
        repository checkout and no clone/commit/push; it can only report that
        it could not do the work. Hand-dispatched, that cost Better Resavio a
        night. Under auto-pull it would repeat every tick, unattended.
        """
        import orchestrator.services.officer_backlog as mod

        resolved = ([KB_DS, REPO_DS], {KB_DS: 2, REPO_DS: 4})
        monkeypatch.setattr(
            mod, "default_datasource_selection", AsyncMock(return_value=resolved)
        )

        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db, _vector_db(rows), _officer_row(user_id=OWNER_ID), now=NOW
        )

        assert counts["dispatched"] == 1
        assert db.created["datasource_ids"] == [KB_DS, REPO_DS]
        assert db.created["datasource_policy_revisions"] == {KB_DS: 2, REPO_DS: 4}
        provenance = db.created["datasource_selection_provenance"]
        assert provenance["origin"] == "default"
        assert provenance["creation_path"] == "officer_backlog_tick"
        assert provenance["effective_work_owner_id"] == OWNER_ID

    @pytest.mark.asyncio
    async def test_connectors_resolve_against_the_workers_tier_not_the_officers(
        self, monkeypatch
    ):
        """The tier argument decides whether the repository survives.

        The policy service withholds clone-based repositories from lite tiers.
        An officer's own post IS lite (he holds no workspace by design), so
        resolving with his backend silently drops the repository and produces
        exactly the symptom this resolution exists to remove — a worker that
        can read the KB but cannot reach the code. The worker's slot backend is
        the only correct measure.
        """
        import orchestrator.services.officer_backlog as mod

        spy = AsyncMock(return_value=([KB_DS, REPO_DS], {}))
        monkeypatch.setattr(mod, "default_datasource_selection", spy)

        db = _db(
            slots={
                "researchers": {
                    "count": 1,
                    "category": RESEARCHER,
                    "backend": "sandbox",
                }
            }
        )
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        await tick_officer(
            db, _vector_db(rows), _officer_row(user_id=OWNER_ID), now=NOW
        )

        assert spy.await_args.args[3] == "sandbox"

    @pytest.mark.asyncio
    async def test_unavailable_connectors_skip_the_pool_rather_than_dispatch(
        self, monkeypatch
    ):
        """A revoked owner, membership or connector is not a job failure.

        Dispatching anyway would put a worker on the ticket holding a partial
        credential contract, and it would burn the ticket's one-shot claim to
        do it. Skipping leaves the pool visibly below floor instead.
        """
        import orchestrator.services.officer_backlog as mod
        from orchestrator.services.datasource_policy import DatasourceUnavailableError

        monkeypatch.setattr(
            mod,
            "default_datasource_selection",
            AsyncMock(side_effect=DatasourceUnavailableError()),
        )

        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db, _vector_db(rows), _officer_row(user_id=OWNER_ID), now=NOW
        )

        assert counts["dispatched"] == 0
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_auto_pull_off_dispatches_nothing(self):
        db = _db()
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["auto_pull"] = False
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 0
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_held_officer_dispatches_nothing(self):
        # Conference fence: the meeting may be revising the direction this
        # dispatch would act on.
        db = _db()
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["hold"] = True
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 0
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_an_open_breaker_skips_only_its_own_pool(self):
        db = _db()
        row = _officer_row()
        row["metadata"]["officer_state"] = {
            "backlog_breakers": {
                "researchers": {"until": (NOW + timedelta(minutes=5)).isoformat()}
            }
        }
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1

    @pytest.mark.asyncio
    async def test_a_kb_outage_skips_cleanly_without_touching_the_breaker(self):
        db = _db()
        vector_db = MagicMock()
        vector_db.acquire = MagicMock(side_effect=RuntimeError("pgvector down"))
        counts = await tick_officer(db, vector_db, _officer_row(), now=NOW)
        assert counts["dispatched"] == 0
        assert counts["breakers_opened"] == 0

    @pytest.mark.asyncio
    async def test_a_floor_breach_wakes_the_officer_once_per_window(self):
        db = _db()
        woken = []

        async def _notify(_db, project_id, *, source, dedup_key, payload=None):
            woken.append((source, dedup_key, payload))
            return True

        # 1-slot pool, zero ready tickets -> below its floor.
        counts = await tick_officer(
            db, _vector_db([]), _officer_row(), now=NOW, notify=_notify
        )
        assert counts["wakes"] == 1
        assert woken[0][0] == "backlog_floor_breach"
        assert woken[0][2]["floor"] == 1 and woken[0][2]["ready"] == 0

    @pytest.mark.asyncio
    async def test_the_floor_wake_is_debounced(self):
        db = _db()
        db.queue_officer_floor_wake.side_effect = None
        db.queue_officer_floor_wake.return_value = {
            "attempted": False,
            "queued": False,
            "state": "policy_debounce",
        }
        woken = []

        async def _notify(_db, project_id, *, source, dedup_key, payload=None):
            woken.append(source)
            return True

        row = _officer_row()
        row["metadata"]["officer_state"] = {
            "backlog_floor_wakes": {
                "researchers": (NOW - timedelta(hours=1)).isoformat()
            }
        }
        counts = await tick_officer(db, _vector_db([]), row, now=NOW, notify=_notify)
        assert counts["wakes"] == 0
        assert woken == []

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "failure_class", ["missing_notifier", "notifier_false", "outbox"]
    )
    async def test_failed_floor_wake_never_increments_success_metric(
        self, failure_class
    ):
        db = _db()
        db.queue_officer_floor_wake.side_effect = None
        db.queue_officer_floor_wake.return_value = {
            "attempted": True,
            "queued": False,
            "state": "retryable",
            "failure_class": failure_class,
        }

        counts = await tick_officer(
            db,
            _vector_db([]),
            _officer_row(),
            now=NOW,
            notify=None,
        )

        assert counts["wakes"] == 0

    @pytest.mark.asyncio
    async def test_grants_are_enforced_against_the_post_owner(self):
        # The internal spawn path bypasses the endpoint's PEP, so a slot that
        # pins a VM backend must still be checked explicitly.
        db = _db()
        seen = {}

        async def _grants(config_override, *, user_id, project_ids):
            seen["user_id"] = user_id
            seen["project_ids"] = project_ids

        row = _officer_row(user_id="00000000-0000-0000-0000-0000000000aa")
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        await tick_officer(db, _vector_db(rows), row, now=NOW, enforce_grants=_grants)
        assert seen["user_id"] == "00000000-0000-0000-0000-0000000000aa"
        assert seen["project_ids"] == [row["project_id"]]

    @pytest.mark.asyncio
    async def test_provisioning_is_told_the_category(self):
        # It decides loop_floor from this, and loop_floor demands a provisioned
        # project cloud baseline. A researcher delivers a KB note and never
        # touches the cloud folder — requiring one would make every research
        # ticket undispatchable on a project that has no cloud folder yet.
        # Found on the first live k3d dispatch, which sealed itself on exactly
        # that: "project loop requires a provisioned cloud folder".
        db = _db()
        seen = {}

        async def _provision(job, *, category=None):
            seen["category"] = category

        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db, _vector_db(rows), _officer_row(), now=NOW, provision_repo=_provision
        )
        assert counts["dispatched"] == 1
        assert seen["category"] == RESEARCHER

    @pytest.mark.asyncio
    async def test_a_failed_repo_provision_seals_the_job_and_does_not_count(self):
        db = _db()

        async def _provision(_job, *, category=None):
            raise RuntimeError("gitea down")

        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        counts = await tick_officer(
            db, _vector_db(rows), _officer_row(), now=NOW, provision_repo=_provision
        )
        assert counts["dispatched"] == 0
        assert counts["skipped"] == 1
        failed = db.finish_officer_job_preflight.await_args.kwargs
        assert failed["activated"] is False
        assert failed["failure_class"] == "infrastructure"

    @pytest.mark.asyncio
    async def test_state_is_persisted_to_officer_state_not_config(self):
        # Live counters belong on the runtime plane; config_override.officer
        # flows into config resolution on every dispatch.
        db = _db()
        await tick_officer(db, _vector_db([]), _officer_row(), now=NOW)
        db.merge_thread_officer_state.assert_awaited()
        patch = db.merge_thread_officer_state.await_args.args[1]
        assert "backlog_stale_claims" in patch
        assert patch["backlog_stale_claim_policy"] == {
            "threshold_minutes": 240,
            "threshold_source": "deployment_default",
        }


class TestExecutorSerialization:
    @pytest.mark.asyncio
    async def test_a_live_executor_blocks_the_next_one(self):
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 2, "category": EXECUTOR}
        }
        db = _db(
            slot_claims=[
                {
                    "id": str(uuid.uuid4()),
                    "status": "processing",
                    "work_category": EXECUTOR,
                    "ticket_note_id": "feature-old",
                    "updated_at": NOW,
                    "created_at": NOW,
                }
            ]
        )
        rows = [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 0
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_paused_executor_frees_the_singleton(self):
        # Owner ruling 2026-08-18: a paused job occupies no slot — nothing is
        # running, so the lane is free. Its later resume may briefly overlap a
        # fresh dispatch; that is accepted over a paused zombie starving the
        # executor lane. The mock returns the paused claim on the LIVE read,
        # so the tick's own status filter is what must free the lane here.
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 2, "category": EXECUTOR}
        }
        paused = {
            "id": str(uuid.uuid4()),
            "status": "paused",
            "work_category": EXECUTOR,
            "ticket_note_id": "feature-old",
            "updated_at": NOW,
            "created_at": NOW,
        }
        db = _db()

        async def _claims(_lineage, **kwargs):
            return [] if kwargs.get("terminal_only") else [paused]

        db.list_officer_slot_claims.side_effect = _claims

        rows = [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 1
        db.create_job.assert_called_once()

    @pytest.mark.asyncio
    async def test_an_undispositioned_previous_ticket_blocks_the_next_executor(self):
        # Terminal is not the same as reviewed. The deliverable gate checks
        # that files exist, never what is in them, so without this an executor
        # chain builds on unreviewed work and review debt compounds straight
        # into the deliverable.
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 1, "category": EXECUTOR}
        }
        finished = {
            "id": str(uuid.uuid4()),
            "status": "completed",
            "work_category": EXECUTOR,
            "ticket_note_id": "feature-old",
            "ready_generation_at": NOW - timedelta(hours=2),
            "created_at": NOW - timedelta(hours=2),
            "updated_at": NOW - timedelta(hours=1),
        }
        db = _db()

        async def _claims(
            _lineage,
            *,
            slot=None,
            work_category=None,
            include_terminal=False,
            terminal_only=False,
            limit=20,
        ):
            return [finished] if include_terminal else []

        db.list_officer_slot_claims.side_effect = _claims

        # The old ticket is still active and was NOT re-armed after dispatch.
        vector_db = _vector_db(
            [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        )
        old_ticket = {
            "note_id": "feature-old",
            "status": "active",
            "ready_at": NOW - timedelta(hours=3),
            "tags": ["category:executor"],
        }
        vector_db.acquire.return_value.__aenter__.return_value.fetchrow = AsyncMock(
            return_value=old_ticket
        )

        counts = await tick_officer(db, vector_db, row, now=NOW)
        assert counts["dispatched"] == 0
        db.create_job.assert_not_called()

    @pytest.mark.asyncio
    async def test_a_closed_previous_ticket_releases_the_executor_lane(self):
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 1, "category": EXECUTOR}
        }
        finished = {
            "id": str(uuid.uuid4()),
            "status": "completed",
            "work_category": EXECUTOR,
            "ticket_note_id": "feature-old",
            "ready_generation_at": NOW - timedelta(hours=2),
            "created_at": NOW - timedelta(hours=2),
            "updated_at": NOW - timedelta(hours=1),
        }
        db = _db()

        async def _claims(
            _lineage,
            *,
            slot=None,
            work_category=None,
            include_terminal=False,
            terminal_only=False,
            limit=20,
        ):
            return [finished] if include_terminal else []

        db.list_officer_slot_claims.side_effect = _claims

        vector_db = _vector_db(
            [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        )
        vector_db.acquire.return_value.__aenter__.return_value.fetchrow = AsyncMock(
            return_value={"note_id": "feature-old", "status": "resolved"}
        )

        counts = await tick_officer(db, vector_db, row, now=NOW)
        assert counts["dispatched"] == 1

    @pytest.mark.asyncio
    async def test_a_re_readied_previous_ticket_also_releases_the_lane(self):
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 1, "category": EXECUTOR}
        }
        finished = {
            "id": str(uuid.uuid4()),
            "status": "completed",
            "work_category": EXECUTOR,
            "ticket_note_id": "feature-old",
            "ready_generation_at": NOW - timedelta(hours=2),
            "created_at": NOW - timedelta(hours=2),
            "updated_at": NOW - timedelta(hours=1),
        }
        db = _db()

        async def _claims(
            _lineage,
            *,
            slot=None,
            work_category=None,
            include_terminal=False,
            terminal_only=False,
            limit=20,
        ):
            return [finished] if include_terminal else []

        db.list_officer_slot_claims.side_effect = _claims

        vector_db = _vector_db(
            [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        )
        vector_db.acquire.return_value.__aenter__.return_value.fetchrow = AsyncMock(
            return_value={
                "note_id": "feature-old",
                "status": "active",
                # Re-armed AFTER the consumed ledger generation = reviewed.
                "ready_at": NOW - timedelta(minutes=30),
            }
        )

        counts = await tick_officer(db, vector_db, row, now=NOW)
        assert counts["dispatched"] == 1

    @pytest.mark.asyncio
    async def test_re_readied_legacy_ticket_releases_the_executor_lane(self):
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 1, "category": EXECUTOR}
        }
        barrier = NOW - timedelta(hours=1)
        finished = {
            "id": str(uuid.uuid4()),
            "status": "completed",
            "work_category": EXECUTOR,
            "ticket_note_id": "feature-old",
            "ready_generation_at": None,
            "legacy_rearm_after": barrier,
            "created_at": barrier,
            "updated_at": barrier,
        }
        db = _db()

        async def _claims(
            _lineage,
            *,
            slot=None,
            work_category=None,
            include_terminal=False,
            terminal_only=False,
            limit=20,
        ):
            return [finished] if include_terminal else []

        db.list_officer_slot_claims.side_effect = _claims
        vector_db = _vector_db(
            [_row("feature-a", tags=["ready", "category:executor"], ready_at=NOW)]
        )
        vector_db.acquire.return_value.__aenter__.return_value.fetchrow = AsyncMock(
            return_value={
                "note_id": "feature-old",
                "status": "active",
                "ready_at": barrier + timedelta(microseconds=1),
            }
        )

        counts = await tick_officer(db, vector_db, row, now=NOW)
        assert counts["dispatched"] == 1

    @pytest.mark.asyncio
    async def test_parallel_safe_exempts_a_ticket_from_the_singleton(self):
        row = _officer_row()
        row["metadata"]["config_override"]["officer"]["slots"] = {
            "executors": {"count": 2, "category": EXECUTOR}
        }
        db = _db(
            slot_claims=[
                {
                    "id": str(uuid.uuid4()),
                    "status": "processing",
                    "work_category": EXECUTOR,
                    "ticket_note_id": "feature-old",
                    "updated_at": NOW,
                    "created_at": NOW,
                }
            ]
        )
        rows = [
            _row(
                "feature-a",
                tags=["ready", "category:executor", "parallel-safe"],
                ready_at=NOW,
            )
        ]
        counts = await tick_officer(db, _vector_db(rows), row, now=NOW)
        assert counts["dispatched"] == 1


class TestWorkspaceRefusal:
    """Slice A2b, spec §7: a refused workspace default reaches the post."""

    @pytest.mark.asyncio
    async def test_a_refused_workspace_default_is_recorded_on_the_post(
        self, monkeypatch
    ):
        from fastapi import HTTPException

        from orchestrator.services.officer_backlog import (
            _officer_meta,
            pool_status_lines,
        )

        message = "This Project's container template 'gone' no longer exists."
        monkeypatch.setattr(
            "orchestrator.services.manifest_workspace_selection."
            "select_project_workspace_default",
            AsyncMock(side_effect=HTTPException(409, message)),
        )
        db = _db()
        rows = [_row("feature-a", tags=["ready", "category:researcher"], ready_at=NOW)]
        officer = _officer_row(user_id=OWNER_ID)
        counts = await tick_officer(db, _vector_db(rows), officer, now=NOW)

        assert counts["dispatched"] == 0 and counts["skipped"] == 1
        db.create_job.assert_not_awaited()
        patch_ = db.merge_thread_officer_state.await_args.args[1]
        assert patch_["backlog_workspace_refusals"] == {
            "researchers": {
                "reason": message,
                "ticket": "feature-a",
                "at": NOW.isoformat(),
            }
        }
        lines = pool_status_lines(_officer_meta(officer), patch_, NOW)
        assert any(message in line for line in lines)
