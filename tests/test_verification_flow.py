"""Verification ledger flow tests.

Covers the endpoint contract and the multi-round continuity that had zero
coverage before this work — including a regression test for the live incident
(job 52949749) where a fresh critic approved a byte-identical deliverable.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from orchestrator.services import job_controls as job_controls_module
from orchestrator.services import (
    job_datasource_selection as job_datasource_selection_module,
)
from orchestrator.services import job_dispatcher as job_dispatcher_module
from orchestrator.services import notification_service as notification_service_module
from orchestrator.services import session_wake as session_wake_module
from orchestrator.services import subjob_completion as subjob_completion_module
import fastapi as fastapi_module


TARGET_ID = "t1"


@pytest.fixture
def ledger_state():
    """In-memory stand-in for jobs.context.verification_rounds.

    ``freeze_data`` defaults to ``None`` (matching a target row with no
    completion freeze yet); tests that need to pin the TARGET's progress
    markers set it explicitly.

    ``critic_targets`` maps a critic job id to the target its
    ``context.verification_target`` names. Anything not listed defaults to
    ``TARGET_ID`` — the ordinary case, where the critic really was spawned for
    the job it is writing to.
    """
    return {
        "rounds": [],
        "freeze_data": None,
        "critic_targets": {},
        "rejections": {},
    }


@pytest.fixture
def fake_db(ledger_state):
    db = MagicMock()

    async def _append(job_id, record):
        if any(
            r["critic_job_id"] == record["critic_job_id"]
            for r in ledger_state["rounds"]
        ):
            return 0
        ledger_state["rounds"].append(record)
        return len(ledger_state["rounds"])

    async def _incr_rejections(job_id):
        counts = ledger_state["rejections"]
        counts[job_id] = counts.get(job_id, 0) + 1
        return counts[job_id]

    async def _get_job(job_id):
        if job_id == TARGET_ID:
            return {
                "id": job_id,
                "context": {"verification_rounds": ledger_state["rounds"]},
                "freeze_data": ledger_state.get("freeze_data"),
            }
        # A critic row: parented to, and pointed at, its target.
        target = ledger_state["critic_targets"].get(job_id, TARGET_ID)
        if target is None:
            return None  # critic row missing entirely
        return {
            "id": job_id,
            "parent_job_id": target,
            "context": {"verification_target": target},
            "freeze_data": None,
        }

    db.append_verification_round = AsyncMock(side_effect=_append)
    db.get_job = AsyncMock(side_effect=_get_job)
    db.increment_verdict_rejections = AsyncMock(side_effect=_incr_rejections)
    return db


class TestRecordVerificationRound:
    @pytest.mark.asyncio
    async def test_first_round_assigns_ids_and_computes_returned(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "missing source"}],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "returned"
        assert result["round"] == 1
        assert result["assigned"][0]["id"] == "F1"
        assert ledger_state["rounds"][0]["asserted_verdict"] == "returned"

    @pytest.mark.asyncio
    async def test_asserted_approved_loses_to_open_blocking_finding(
        self, fake_db, ledger_state
    ):
        """The rule that makes the incident impossible."""
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["rounds"].append(
            {
                "round": 1,
                "critic_job_id": "c1",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "head_commit": "aaa",
                "opened": [{"id": "F1", "severity": "high", "claim": "missing source"}],
                "dispositions": [],
                "ts": "t",
            }
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c2",
            asserted_verdict="approved",
            opened=[],
            dispositions=[
                {"id": "F1", "disposition": "DISPUTED", "reason": "looks fine"}
            ],
            head_commit="aaa",
        )

        assert result["verdict"] == "returned"
        assert [f["id"] for f in result["open_findings"]] == ["F1"]

    @pytest.mark.asyncio
    async def test_returned_with_no_findings_raises_409(self, fake_db):
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id="t1",
                critic_job_id="c1",
                asserted_verdict="returned",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )
        assert exc.value.status_code == 409

    @staticmethod
    async def _submit_invalid(fake_db, critic_job_id: str):
        """One guaranteed-rejected submission (returned with no findings)."""
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id="t1",
                critic_job_id=critic_job_id,
                asserted_verdict="returned",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )
        assert exc.value.status_code == 409
        return exc.value

    @pytest.mark.asyncio
    async def test_rejections_below_cap_do_not_escalate(self, fake_db, monkeypatch):
        """Two invalid submissions: plain 409s, no escalation, empty ledger.
        knowledge-history/done/rejected_verdict_livelocks_critic_and_wedges_parent.md
        """

        escalate = AsyncMock()
        monkeypatch.setattr(subjob_completion_module, "escalate_target", escalate)

        for _ in range(2):
            exc = await self._submit_invalid(fake_db, "c1")
            assert not exc.detail.get("escalated")

        escalate.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_third_rejection_escalates_target_with_reason(
        self, fake_db, ledger_state, monkeypatch
    ):
        """The cap: rejection 3 escalates the TARGET (via _escalate_target,
        which stays loop-aware) and flags the 409 so the agent-side client
        turns its retry instruction into a stop order."""

        escalate = AsyncMock()
        monkeypatch.setattr(subjob_completion_module, "escalate_target", escalate)

        await self._submit_invalid(fake_db, "c1")
        await self._submit_invalid(fake_db, "c1")
        exc = await self._submit_invalid(fake_db, "c1")

        assert exc.detail.get("escalated") is True
        escalate.assert_awaited_once()
        args = escalate.await_args.args
        assert args[0] == "t1"
        assert args[1]["id"] == "t1"  # the already-fetched target row
        assert "3 rejected submissions" in args[2]
        assert "Cannot return a job with no findings" in args[2]
        assert ledger_state["rounds"] == []  # nothing was ever recorded

    @pytest.mark.asyncio
    async def test_rejection_counter_is_per_critic(self, fake_db, monkeypatch):
        """A fresh critic (new round) starts at zero — c1 exhausting the cap
        must not poison c2."""

        escalate = AsyncMock()
        monkeypatch.setattr(subjob_completion_module, "escalate_target", escalate)

        for _ in range(3):
            await self._submit_invalid(fake_db, "c1")
        assert escalate.await_count == 1

        exc = await self._submit_invalid(fake_db, "c2")
        assert not exc.detail.get("escalated")
        assert escalate.await_count == 1

    @pytest.mark.asyncio
    async def test_missing_disposition_raises_409(self, fake_db, ledger_state):
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["rounds"].append(
            {
                "round": 1,
                "critic_job_id": "c1",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "head_commit": "aaa",
                "opened": [{"id": "F1", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        )

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id="t1",
                critic_job_id="c2",
                asserted_verdict="approved",
                opened=[],
                dispositions=[],
                head_commit="bbb",
            )
        assert exc.value.status_code == 409
        assert "F1" in str(exc.value.detail)

    @pytest.mark.asyncio
    async def test_duplicate_append_returns_existing_verdict(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        kwargs = dict(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="aaa",
        )
        first = await _record_verification_round_impl(**kwargs)
        second = await _record_verification_round_impl(**kwargs)

        assert second["verdict"] == first["verdict"]
        assert len(ledger_state["rounds"]) == 1

    @pytest.mark.asyncio
    async def test_empty_critic_job_id_raises_400(self, fake_db):
        """A falsy critic_job_id must never reach the ledger: the Task 1
        dedup guard keys on it, so two distinct empty-id rounds would
        collide and the second would be silently dropped as a "duplicate"
        of the first."""
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id="t1",
                critic_job_id="",
                asserted_verdict="approved",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )
        assert exc.value.status_code == 400


class TestRecordVerificationRoundCleanApproval:
    """Every other test in this module that inspects ``result["verdict"]``
    exercises a 'returned' outcome, or expects an exception. None of them
    proves the wiring can produce a genuine, computed 'approved' verdict — a
    hardcoded ``return {"verdict": "returned", ...}`` inserted anywhere on
    this path would still pass the entire rest of the suite. This is the
    positive-path counterpart to
    ``test_asserted_approved_loses_to_open_blocking_finding`` above.
    """

    @pytest.mark.asyncio
    async def test_no_open_blocking_findings_computes_approved(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="approved",
            opened=[],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "approved"
        assert result["open_findings"] == []
        assert ledger_state["rounds"][0]["verdict"] == "approved"


class TestExplicitReturnAtNonBlockingSeverity:
    """A critic that explicitly calls ``return_job_with_feedback`` must not
    have its verdict silently rewritten to 'approved' just because none of its
    findings reached ``BLOCKING_SEVERITY``.

    The pure-function combinations live in tests/test_verification_ledger.py;
    these prove the endpoint actually threads ``asserted_verdict`` into the
    computation and into the round it persists.
    """

    @pytest.mark.asyncio
    async def test_returned_with_only_medium_findings_records_returned(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "medium", "claim": "shaky citation"}],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "returned"
        assert ledger_state["rounds"][0]["verdict"] == "returned"

    @pytest.mark.asyncio
    async def test_approved_with_only_medium_findings_still_approves(
        self, fake_db, ledger_state
    ):
        """The contrast case: the server computes STRICTER than the model, never
        laxer. Without this, an implementation that returns on any open finding
        at all would pass the test above."""
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="approved",
            opened=[],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "approved"


class TestReturnWithNoNewFindingsButPriorOpen:
    """Round 2's most common shape: no NEW problems, but a predecessor's
    finding is still unaddressed. Rejecting this made
    ``return_job_with_feedback`` uncallable for that critic."""

    @pytest.mark.asyncio
    async def test_returned_with_empty_opened_and_prior_open_is_accepted(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["rounds"].append(
            {
                "round": 1,
                "critic_job_id": "c1",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "head_commit": "aaa",
                "opened": [{"id": "F1", "severity": "high", "claim": "missing tests"}],
                "dispositions": [],
                "ts": "t",
            }
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c2",
            asserted_verdict="returned",
            opened=[],
            dispositions=[{"id": "F1", "disposition": "STILL_OPEN"}],
            head_commit="bbb",
        )

        assert result["verdict"] == "returned"
        assert [f["id"] for f in result["open_findings"]] == ["F1"]
        assert len(ledger_state["rounds"]) == 2

    @pytest.mark.asyncio
    async def test_returning_while_resolving_the_last_finding_does_not_approve(
        self, fake_db, ledger_state
    ):
        """The laxer-than-asserted path this relaxation opened.

        Allowing `returned` with an empty `opened` (the round-2 shape) means a
        critic can, in the same call, disposition the last open finding
        RESOLVED. `open_after` is then empty — and computing `approved` there
        advances a target the critic explicitly refused to pass. Before the
        relaxation this shape was a hard 409, so it is newly reachable, and
        the critic brief now actively teaches the empty-`findings` return.
        """
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["rounds"].append(
            {
                "round": 1,
                "critic_job_id": "c1",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "content_tree": "aaa",
                "opened": [{"id": "F1", "severity": "high", "claim": "missing tests"}],
                "dispositions": [],
                "ts": "t",
            }
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id=TARGET_ID,
            critic_job_id="c2",
            asserted_verdict="returned",
            opened=[],
            dispositions=[
                {"id": "F1", "disposition": "RESOLVED", "quote": "here they are"}
            ],
            head_commit="bbb",
        )

        assert result["open_findings"] == []  # the fold really did close F1
        assert result["verdict"] == "returned"
        assert ledger_state["rounds"][1]["verdict"] == "returned"

    @pytest.mark.asyncio
    async def test_returned_with_nothing_open_anywhere_still_raises_409(self, fake_db):
        """The original guard must not be weakened away: an empty return with
        no prior open findings has nothing to return ON."""
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id="t1",
                critic_job_id="c1",
                asserted_verdict="returned",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )
        assert exc.value.status_code == 409


class TestCriticMustBelongToTheTarget:
    """The endpoint takes ``target_job_id`` from the URL and ``critic_job_id``
    from the body, authenticated only by ``X-Internal-Key`` — and the target is
    chosen by the MODEL (``approve_job_verdict(job_id=...)`` flows straight through).

    A confused critic writing to the wrong job's ledger is fail-closed for its
    REAL target (which then escalates for lack of a verdict), but it pollutes
    an unrelated job's ledger with phantom findings that get injected into that
    job's next critic brief and can force its cap / no-progress escalation.
    Same principle as the rest of this design: don't trust the model's
    assertion about which job it is judging.
    """

    @pytest.mark.asyncio
    async def test_critic_pointed_at_another_target_is_rejected(
        self, fake_db, ledger_state
    ):
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["critic_targets"]["c-elsewhere"] = "some-other-job"

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id=TARGET_ID,
                critic_job_id="c-elsewhere",
                asserted_verdict="returned",
                opened=[{"severity": "high", "claim": "x"}],
                dispositions=[],
                head_commit="aaa",
            )

        assert exc.value.status_code == 403
        assert ledger_state["rounds"] == [], "a stranger's finding reached the ledger"

    @pytest.mark.asyncio
    async def test_non_critic_job_id_is_rejected(self, fake_db, ledger_state):
        """A job with no ``verification_target`` at all — a scholar, a
        delegation child, or an ordinary job id typed by the model."""
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["critic_targets"]["not-a-critic"] = ""

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id=TARGET_ID,
                critic_job_id="not-a-critic",
                asserted_verdict="approved",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )

        assert exc.value.status_code == 403
        assert ledger_state["rounds"] == []

    @pytest.mark.asyncio
    async def test_missing_critic_job_is_rejected(self, fake_db, ledger_state):
        from fastapi import HTTPException

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["critic_targets"]["ghost"] = None  # get_job returns None

        with pytest.raises(HTTPException) as exc:
            await _record_verification_round_impl(
                postgres_db=fake_db,
                target_job_id=TARGET_ID,
                critic_job_id="ghost",
                asserted_verdict="approved",
                opened=[],
                dispositions=[],
                head_commit="aaa",
            )

        assert exc.value.status_code == 403
        assert ledger_state["rounds"] == []

    @pytest.mark.asyncio
    async def test_the_right_critic_still_records(self, fake_db, ledger_state):
        """Contrast case: an implementation that rejects everything would pass
        the three tests above."""
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id=TARGET_ID,
            critic_job_id="c1",
            asserted_verdict="approved",
            opened=[],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "approved"

    @pytest.mark.asyncio
    async def test_jsonb_string_context_is_parsed_not_rejected(
        self, fake_db, ledger_state
    ):
        """asyncpg returns JSONB as a string on the app pool (no codec
        registered), so an isinstance-only check would reject EVERY real
        critic — the failure mode documented in
        knowledge-base/knowledge/issues/jsonb_isinstance_guard_without_parse_silent_dead_paths.md.
        """
        import json

        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        async def _get_job(job_id):
            if job_id == TARGET_ID:
                return {
                    "id": job_id,
                    "context": {"verification_rounds": ledger_state["rounds"]},
                    "freeze_data": None,
                }
            return {
                "id": job_id,
                "context": json.dumps({"verification_target": TARGET_ID}),
                "freeze_data": None,
            }

        fake_db.get_job = AsyncMock(side_effect=_get_job)

        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id=TARGET_ID,
            critic_job_id="c1",
            asserted_verdict="approved",
            opened=[],
            dispositions=[],
            head_commit="aaa",
        )

        assert result["verdict"] == "approved"


class TestRecordVerificationRoundContentTree:
    """The ledger row must carry ``content_tree``, taken from the TARGET's own
    freeze — same server-authoritative rule as ``head_commit``, and for the
    same reason (the critic runs on its own branch, so its content is a
    different thing from the target's)."""

    @pytest.mark.asyncio
    async def test_target_freeze_content_tree_is_recorded(self, fake_db, ledger_state):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["freeze_data"] = {
            "head_commit": "target-sha",
            "content_tree": "target-tree",
        }

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="critic-own-branch-sha",
            content_tree="critic-own-branch-tree",
        )

        assert ledger_state["rounds"][0]["content_tree"] == "target-tree"

    @pytest.mark.asyncio
    async def test_falls_back_to_caller_supplied_content_tree(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["freeze_data"] = {"summary": "older freeze, no content_tree"}

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="caller-sha",
            content_tree="caller-tree",
        )

        assert ledger_state["rounds"][0]["content_tree"] == "caller-tree"


class TestRecordVerificationRoundHeadCommitAuthority:
    """Amendment item 2: the recorded round's ``head_commit`` is
    server-authoritative from the TARGET's ``freeze_data``, not whatever the
    caller (critic) supplied.

    The critic runs on its own ``subjob/<id>/critic`` branch, so its own HEAD
    is a different thing from the target's — comparing it against the
    previous round's would be meaningless. The caller-supplied value is only
    a fallback for when the target has no freeze_data (or an older freeze
    that predates this field).
    """

    @pytest.mark.asyncio
    async def test_target_freeze_head_commit_wins_over_caller_supplied(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["freeze_data"] = {"head_commit": "target-sha"}

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="critic-own-branch-sha",
        )

        assert ledger_state["rounds"][0]["head_commit"] == "target-sha"

    @pytest.mark.asyncio
    async def test_falls_back_to_caller_supplied_when_target_has_no_freeze_data(
        self, fake_db, ledger_state
    ):
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["freeze_data"] = None  # target hasn't completed yet

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="caller-sha",
        )

        assert ledger_state["rounds"][0]["head_commit"] == "caller-sha"

    @pytest.mark.asyncio
    async def test_falls_back_when_target_freeze_data_lacks_the_key(
        self, fake_db, ledger_state
    ):
        """An older freeze recorded before this field existed."""
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        ledger_state["freeze_data"] = {"summary": "pre-existing freeze, no head_commit"}

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "x"}],
            dispositions=[],
            head_commit="caller-sha-2",
        )

        assert ledger_state["rounds"][0]["head_commit"] == "caller-sha-2"


class TestLedgerIsNotPubliclySeedable:
    """The whole design rests on "the server owns the ledger": findings get
    server-assigned ids, the verdict is computed from them, and the gate's cap
    and no-progress guards read them.

    ``POST /api/jobs`` accepts a caller-supplied ``context`` and strips the
    system-only markers from it. ``verification_rounds`` was not in that set,
    so any caller could pre-seed a job's ledger — planting phantom findings
    that get injected into its first critic's brief, or a round count that
    trips the cap on round one.
    """

    def test_verification_rounds_is_stripped_from_a_public_payload(self):
        from orchestrator.schemas.job_create import JobCreate
        from orchestrator.services.job_create_ingress import (
            strip_public_job_reserved_markers as _strip_public_job_reserved_markers,
        )

        job = JobCreate(
            description="d",
            context={
                "verification_rounds": [
                    {"round": 1, "critic_job_id": "x", "verdict": "approved"}
                ],
                "kept": "ok",
            },
        )
        _strip_public_job_reserved_markers(job)

        assert "verification_rounds" not in job.context
        assert job.context["kept"] == "ok"  # ordinary keys survive

    def test_it_joins_the_other_verification_markers(self):
        """``verification_target`` was already stripped; the ledger the target
        side of that pair owns must be too."""
        from orchestrator.services.job_create_ingress import (
            PUBLIC_JOB_CONTEXT_RESERVED_KEYS as _PUBLIC_JOB_CONTEXT_RESERVED_KEYS,
        )

        assert "verification_target" in _PUBLIC_JOB_CONTEXT_RESERVED_KEYS
        assert "verification_rounds" in _PUBLIC_JOB_CONTEXT_RESERVED_KEYS


class TestVerificationGateDecision:
    def test_first_round_spawns(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        action, _ = _verification_gate_decision([], content_tree="aaa", max_rounds=3)
        assert action == "spawn"

    def test_unchanged_head_with_open_blocking_escalates(self):
        """THE INCIDENT REGRESSION TEST.

        Job 52949749 was returned twice, then re-submitted byte-identical and
        approved by a fresh critic. Identical HEAD + an open blocking finding
        must never reach a judge again.
        """
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "content_tree": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        ]

        action, reason = _verification_gate_decision(
            rounds, content_tree="aaa", max_rounds=3
        )
        assert action == "escalate"
        assert "no progress" in reason.lower()

    def test_changed_head_with_open_blocking_spawns(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "content_tree": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        ]

        action, _ = _verification_gate_decision(
            rounds, content_tree="bbb", max_rounds=3
        )
        assert action == "spawn"

    def test_cap_reached_with_open_blocking_escalates(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": i,
                "critic_job_id": f"c{i}",
                "content_tree": f"h{i}",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": f"F{i}", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
            for i in range(1, 4)
        ]

        action, reason = _verification_gate_decision(
            rounds, content_tree="h9", max_rounds=3
        )
        assert action == "escalate"
        assert "round limit" in reason.lower()

    def test_legacy_rounds_without_content_tree_abstain(self):
        """A round written before ``content_tree`` existed carries only
        ``head_commit`` — a value known to be WRONG for this comparison in
        both directions. It is captured before the freeze commit (which runs
        with allow_empty=True), so it never matches; and it reverts on a
        re-clone after a failed push, so when it does match it is reporting an
        infrastructure hiccup as "no progress" and escalating a healthy job
        BACKWARDS.

        Comparing it is therefore strictly worse than not comparing at all.
        The guard abstains — "cannot determine progress", spawn normally — and
        the round cap still bounds the loop.
        """
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "head_commit": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        ]

        action, _ = _verification_gate_decision(rounds, content_tree=None, max_rounds=3)
        assert action == "spawn"

    def test_legacy_rounds_still_hit_the_round_cap(self):
        """Abstaining on no-progress must not disable the OTHER guard: a
        legacy job still escalates at the cap."""
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": i,
                "critic_job_id": f"c{i}",
                "head_commit": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": f"F{i}", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
            for i in range(1, 4)
        ]

        action, reason = _verification_gate_decision(
            rounds, content_tree=None, max_rounds=3
        )
        assert action == "escalate"
        assert "round limit" in reason.lower()

    def test_a_current_content_tree_against_a_legacy_row_abstains(self):
        """A tree hash and a commit SHA are different value spaces, and the
        gate no longer takes a commit SHA at all. A current round with a
        content hash, judged against a legacy row that has none, must ABSTAIN
        (spawn) rather than manufacture a comparison.
        """
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "head_commit": "aaa",  # legacy row: no content_tree
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        ]

        action, _ = _verification_gate_decision(
            rounds, content_tree="aaa", max_rounds=3
        )
        assert action == "spawn"

    def test_cap_applies_to_non_blocking_open_findings_too(self):
        """Downstream of the verdict-rule change: an explicit 'returned' at
        medium/low severity now resumes the target instead of silently
        approving it, so a round can END with open findings but NONE blocking.

        If the gate's guards only ever looked at the blocking subset, that
        state would spawn a fresh critic forever — dodging both the round cap
        and the no-progress check, with no terminal state at all. The guards
        run on the OPEN set; only a genuinely empty open set spawns freely.
        """
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": i,
                "critic_job_id": f"c{i}",
                "content_tree": f"h{i}",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": f"F{i}", "severity": "medium", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
            for i in range(1, 4)
        ]

        action, reason = _verification_gate_decision(
            rounds, content_tree="h9", max_rounds=3
        )
        assert action == "escalate"
        assert "round limit" in reason.lower()

    def test_no_progress_applies_to_non_blocking_open_findings_too(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "content_tree": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "medium", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
        ]

        action, reason = _verification_gate_decision(
            rounds, content_tree="aaa", max_rounds=3
        )
        assert action == "escalate"
        assert "no progress" in reason.lower()

    def test_empty_open_set_spawns_regardless_of_round_count(self):
        """Contrast case: nothing is open, so nothing is being re-litigated —
        the cap must not fire and strand a job whose findings were all
        resolved."""
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": i,
                "critic_job_id": f"c{i}",
                "content_tree": f"h{i}",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [],
                "dispositions": [],
                "ts": "t",
            }
            for i in range(1, 6)
        ]

        action, _ = _verification_gate_decision(rounds, content_tree="h9", max_rounds=3)
        assert action == "spawn"

    def test_unlimited_rounds_never_hits_cap(self):
        from tests.b08_completion_helpers import (
            verification_gate_decision as _verification_gate_decision,
        )

        rounds = [
            {
                "round": i,
                "critic_job_id": f"c{i}",
                "content_tree": f"h{i}",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": f"F{i}", "severity": "high", "claim": "c"}],
                "dispositions": [],
                "ts": "t",
            }
            for i in range(1, 9)
        ]

        action, _ = _verification_gate_decision(rounds, content_tree="h9", max_rounds=0)
        assert action == "spawn"


class TestEscalateTarget:
    """``_escalate_target`` is status-aware: an ordinary job must never park a
    project-loop job on ``pending_review`` — the loop advance hook fires only
    on terminal statuses, so a parked loop job wedges the whole loop.
    """

    @pytest.mark.asyncio
    async def test_ordinary_job_escalates_to_pending_review(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = {"id": "t1", "context": {}}
        status = await _escalate_target("t1", job, "no progress: still broken")

        assert status == "pending_review"
        update_mock.assert_awaited_once_with(
            "t1", status="pending_review", error_message="no progress: still broken"
        )

    @pytest.mark.asyncio
    async def test_loop_job_escalates_to_completed_not_pending_review(
        self, monkeypatch
    ):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = {"id": "t1", "context": {"loop_id": "loop-1"}}
        status = await _escalate_target("t1", job, "round limit reached")

        assert status == "completed"
        update_mock.assert_awaited_once_with(
            "t1", status="completed", error_message="round limit reached"
        )


def _patch_escalation_collaborators(monkeypatch, main_module):
    """Stub the three side effects ``_escalate_target`` fires and return them."""
    update_mock = AsyncMock()
    wake_mock = AsyncMock()
    kick_mock = MagicMock()
    notify_mock = AsyncMock()
    monkeypatch.setattr(
        main_module.app.state.resources.postgres_db, "update_job_status", update_mock
    )
    monkeypatch.setattr(session_wake_module, "maybe_wake_session", wake_mock)
    monkeypatch.setattr(session_wake_module, "kick_drain", kick_mock)
    monkeypatch.setattr(
        notification_service_module.notification_service,
        "record_review_returned",
        notify_mock,
    )
    return update_mock, wake_mock, kick_mock, notify_mock


class TestEscalateTargetWakesAndNotifies:
    """``_escalate_target`` is a TERMINAL transition with no /complete of its
    own — the same property its sibling ``_set_target_to_autonomy_status``
    carries a comment about. Without the wake it arrives a sweeper tick late
    for every escalated job; without a notification, "escalates to a human"
    means "sits in a queue nobody is paged about".
    """

    @pytest.mark.asyncio
    async def test_wakes_the_creating_session(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        _, wake_mock, kick_mock, _ = _patch_escalation_collaborators(
            monkeypatch, main_module
        )

        job = {"id": "t1", "context": {}, "user_id": "u1", "config_name": "developer"}
        await _escalate_target("t1", job, "no progress")

        wake_mock.assert_awaited_once_with(
            main_module.app.state.resources.postgres_db, "t1", "pending_review"
        )
        kick_mock.assert_called_once_with(main_module.app.state.resources.postgres_db)

    @pytest.mark.asyncio
    async def test_wake_uses_the_loop_terminal_status(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        _, wake_mock, _, _ = _patch_escalation_collaborators(monkeypatch, main_module)

        job = {"id": "t1", "context": {"loop_id": "l1"}, "user_id": "u1"}
        await _escalate_target("t1", job, "round limit reached")

        wake_mock.assert_awaited_once_with(
            main_module.app.state.resources.postgres_db, "t1", "completed"
        )

    @pytest.mark.asyncio
    async def test_notifies_the_owner_with_the_reason(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        _, _, _, notify_mock = _patch_escalation_collaborators(monkeypatch, main_module)

        job = {"id": "t1", "context": {}, "user_id": "u1", "config_name": "developer"}
        await _escalate_target("t1", job, "No progress since round 2: F1 open.")

        notify_mock.assert_awaited_once()
        kwargs = notify_mock.call_args.kwargs
        assert kwargs["user_id"] == "u1"
        assert kwargs["job_id"] == "t1"
        assert kwargs["config_name"] == "developer"
        assert "No progress since round 2" in kwargs["reason"]

    @pytest.mark.asyncio
    async def test_loop_job_is_not_notified(self, monkeypatch):
        """A loop job resolves 'completed' and its reason is read by the loop
        retro from ``error_message``. Paging a human per iteration is noise,
        not signal."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        _, _, _, notify_mock = _patch_escalation_collaborators(monkeypatch, main_module)

        job = {"id": "t1", "context": {"loop_id": "l1"}, "user_id": "u1"}
        await _escalate_target("t1", job, "round limit reached")

        notify_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ownerless_job_is_not_notified_and_still_escalates(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        update_mock, _, _, notify_mock = _patch_escalation_collaborators(
            monkeypatch, main_module
        )

        status = await _escalate_target("t1", {"id": "t1", "context": {}}, "why")

        assert status == "pending_review"
        update_mock.assert_awaited_once()
        notify_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_notification_failure_does_not_break_the_escalation(
        self, monkeypatch
    ):
        """The status write is the load-bearing part. A notifier outage must
        not leave the target in 'reviewing' — the wedge this design removes."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        update_mock, _, _, notify_mock = _patch_escalation_collaborators(
            monkeypatch, main_module
        )
        notify_mock.side_effect = RuntimeError("SMTP down")

        job = {"id": "t1", "context": {}, "user_id": "u1", "config_name": "developer"}
        status = await _escalate_target("t1", job, "why")

        assert status == "pending_review"
        update_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_wake_failure_does_not_break_the_escalation(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import escalate_target as _escalate_target

        update_mock, wake_mock, _, _ = _patch_escalation_collaborators(
            monkeypatch, main_module
        )
        wake_mock.side_effect = RuntimeError("db down")

        job = {"id": "t1", "context": {}, "user_id": "u1"}
        status = await _escalate_target("t1", job, "why")

        assert status == "pending_review"
        update_mock.assert_awaited_once()


def _make_completion_job(
    *,
    job_id: str = "target-1",
    freeze_content_tree,
    verification_rounds,
    max_rounds: int = 3,
    is_loop: bool = False,
):
    """A minimal job row that clears every guard in
    ``_trigger_verification_on_complete`` (not a subjob, verification
    enabled, freeze indicates job completion) with a controllable ledger and
    freeze ``content_tree`` — enough to exercise the REAL function end to end.
    """
    context: dict = {"verification_rounds": verification_rounds}
    if is_loop:
        context["loop_id"] = "loop-1"
    return {
        "id": job_id,
        "parent_job_id": None,
        "config_override": None,
        "resolved_config": {
            "verification": {"enabled": True, "max_rounds": max_rounds}
        },
        "freeze_data": {
            "freeze_type": "job_complete",
            "summary": "done",
            "deliverables": [],
            "confidence": 0.9,
            "content_tree": freeze_content_tree,
        },
        "context": context,
        "status": "reviewing",
        "description": "do the thing",
        "config_name": "developer",
        "project_id": None,
        "user_id": None,
        "repo_name": None,
        "branch_name": None,
    }


class TestTriggerVerificationContentTreeWiring:
    """Proves ``content_tree`` actually FLOWS from a completion freeze into the
    gate comparison inside the real ``_trigger_verification_on_complete`` — not
    just that the pure decision function behaves correctly when handed a value
    directly (that's what ``TestVerificationGateDecision`` above proves, and it
    stayed green through TWO generations of dead wiring: first when nothing
    wrote the field into a freeze at all, then when the field it compared moved
    on every round regardless of what the agent produced).
    """

    @pytest.mark.asyncio
    async def test_same_content_tree_in_freeze_escalates_via_real_trigger(
        self, monkeypatch
    ):
        """THE INCIDENT REGRESSION TEST, exercised through the real trigger
        function instead of the decision function directly."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        prior_round = {
            "round": 1,
            "critic_job_id": "c1",
            "content_tree": "aaa",
            "verdict": "returned",
            "asserted_verdict": "returned",
            "opened": [{"id": "F1", "severity": "high", "claim": "still broken"}],
            "dispositions": [],
            "ts": "t",
        }
        job = _make_completion_job(
            freeze_content_tree="aaa", verification_rounds=[prior_round]
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_awaited_once()
        call_args = update_mock.call_args
        assert call_args.args[0] == "target-1"
        assert call_args.kwargs["status"] == "pending_review"
        assert "no progress" in call_args.kwargs["error_message"].lower()
        assert any("escalated" in a for a in actions)

    @pytest.mark.asyncio
    async def test_changed_content_tree_in_freeze_spawns_via_real_trigger(
        self, monkeypatch
    ):
        """Contrast case: proves the comparison is real, not a constant. An
        implementation that always escalates (or never reads content_tree at
        all) would pass the test above but must fail this one."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(return_value={"id": "critic-999"}),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        # No critic in flight — this test is about the gate, not the
        # duplicate-spawn guard (which fails closed on the non-UUID id here).
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )

        prior_round = {
            "round": 1,
            "critic_job_id": "c1",
            "content_tree": "aaa",
            "verdict": "returned",
            "asserted_verdict": "returned",
            "opened": [{"id": "F1", "severity": "high", "claim": "still broken"}],
            "dispositions": [],
            "ts": "t",
        }
        job = _make_completion_job(
            freeze_content_tree="bbb", verification_rounds=[prior_round]
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_not_awaited()
        assert any("critic job" in a and "created" in a for a in actions)

    @pytest.mark.asyncio
    async def test_loop_job_no_progress_escalates_to_completed(self, monkeypatch):
        """Global constraint: a project-loop job must resolve ``completed``
        (never ``pending_review``) even on a no-progress escalation, or it
        wedges the loop's advance hook forever."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        prior_round = {
            "round": 1,
            "critic_job_id": "c1",
            "content_tree": "aaa",
            "verdict": "returned",
            "asserted_verdict": "returned",
            "opened": [{"id": "F1", "severity": "high", "claim": "still broken"}],
            "dispositions": [],
            "ts": "t",
        }
        job = _make_completion_job(
            freeze_content_tree="aaa",
            verification_rounds=[prior_round],
            is_loop=True,
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "completed"


class TestUndeliveredCompletionSkipsTheCritic:
    """A completion whose job-ending push failed must not spawn a critic.

    The agent marks the freeze ``delivery_failed`` when the final push does not
    land (src/core/phase.py, _push_job_ending_state). The deliverables then
    exist only on a pod about to be reclaimed, and the job repository is empty
    or stale — so a critic would clone that repo, correctly observe the
    deliverable missing, and return the job for work that EXISTS but was never
    delivered. On dev job `40efbb39` that misdiagnosis cost a 105-minute
    livelock and a verdict describing an infrastructure fault as a work fault
    (knowledge-history/done/git_push_fails_silently_via_workspace_backend.md).

    Escalating instead is both cheaper and more accurate, and routing it
    through ``_escalate_target`` puts the real reason in ``error_message``
    where an operator and the cockpit can see it — with the loop-job status
    rule already handled there.
    """

    @staticmethod
    def _undelivered(job):
        job["freeze_data"]["delivery_failed"] = True
        job["freeze_data"]["delivery_error"] = (
            "The job-ending git push failed at job completion."
        )
        return job

    @pytest.mark.asyncio
    async def test_undelivered_completion_escalates_instead_of_spawning(
        self, monkeypatch
    ):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        create_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "create_job", create_mock
        )

        # No prior rounds: the gate would otherwise say "spawn", so an escalation
        # here can only come from the delivery check.
        job = self._undelivered(
            _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        create_mock.assert_not_awaited()
        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "pending_review"
        reason = update_mock.call_args.kwargs["error_message"].lower()
        assert "deliver" in reason or "push" in reason
        assert any("escalated" in a for a in actions)

    @pytest.mark.asyncio
    async def test_delivered_completion_still_spawns(self, monkeypatch):
        """Contrast: proves the check reads the flag rather than always firing."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(return_value={"id": "critic-999"}),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )

        job = _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_not_awaited()
        assert any("critic job" in a and "created" in a for a in actions)

    @pytest.mark.asyncio
    async def test_undelivered_loop_job_escalates_to_completed(self, monkeypatch):
        """The loop-job status rule must hold on this path too, or the loop wedges."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(return_value={"id": "critic-999"}),
        )

        job = self._undelivered(
            _make_completion_job(
                freeze_content_tree="aaa", verification_rounds=[], is_loop=True
            )
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "completed"


class TestNoDuplicateCriticSpawn:
    """``complete_job`` deliberately accepts entry statuses
    processing/reviewing/pending_review/completed, so a retried ``/complete``
    on a target already in 'reviewing' reaches the trigger a second time.

    With no existence check that spawns a SECOND critic for the same round.
    Both then compute from a pre-append read, producing duplicate `round`
    numbers and — worse — duplicate finding IDs, because ``assign_ids``
    derives from ``next_finding_index(rounds)`` and ``fold_open_findings``
    keys by id. This is the only interleaving that can produce an unwarranted
    approval.
    """

    @staticmethod
    def _patch(monkeypatch, main_module, *, live_critic: bool):
        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=live_critic),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            AsyncMock(),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        return create_job_mock

    @pytest.mark.asyncio
    async def test_second_trigger_does_not_spawn_a_second_critic(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        create_job_mock = self._patch(monkeypatch, main_module, live_critic=True)

        job = _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        create_job_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_first_trigger_still_spawns(self, monkeypatch):
        """Contrast case: an implementation that never spawns would pass the
        test above and disable verification entirely."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        create_job_mock = self._patch(monkeypatch, main_module, live_critic=False)

        job = _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        create_job_mock.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_the_guard_runs_before_creating_the_critic(self, monkeypatch):
        """A guard consulted only AFTER create_job would be useless."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        order: list[str] = []

        async def _guard(_target):
            order.append("guard")
            return False

        async def _create(**_kw):
            order.append("create")
            return {"id": "critic-999"}

        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(side_effect=_guard),
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "create_job",
            AsyncMock(side_effect=_create),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )

        job = _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, []
        )

        assert order == ["guard", "create"]


class TestCriticDatasourceFailureUnblocksTarget:
    @pytest.mark.asyncio
    async def test_revoked_inherited_connector_escalates_target(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(
                side_effect=fastapi_module.HTTPException(
                    status_code=403,
                    detail="One or more selected connectors are unavailable",
                )
            ),
        )
        escalate = AsyncMock(return_value="pending_review")
        monkeypatch.setattr(subjob_completion_module, "escalate_target", escalate)

        job = _make_completion_job(freeze_content_tree="aaa", verification_rounds=[])
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        escalate.assert_awaited_once()
        assert any("connector access changed" in action for action in actions)


class TestDuplicateFindingIdsCannotHideABlockingFinding:
    """Defence in depth behind the spawn guard: even if two rounds somehow
    open the same finding id, folding must never let a blocking finding
    disappear — ``open_by_id[fid] = entry`` silently overwrote, so a
    later low-severity F2 could erase an earlier high-severity F2 and turn a
    computed 'returned' into 'approved'.
    """

    def test_a_colliding_low_finding_cannot_erase_a_high_one(self):
        from orchestrator.services.verification_ledger import (
            compute_verdict,
            fold_open_findings,
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "cA",
                "opened": [{"id": "F2", "severity": "high", "claim": "data loss"}],
                "dispositions": [],
            },
            {
                "round": 1,
                "critic_job_id": "cB",  # racing twin, same pre-append read
                "opened": [{"id": "F2", "severity": "low", "claim": "typo"}],
                "dispositions": [],
            },
        ]

        open_findings = fold_open_findings(rounds)

        assert [f["id"] for f in open_findings] == ["F2"]
        assert open_findings[0]["severity"] == "high"
        assert compute_verdict("approved", open_findings) == "returned"

    def test_an_ordinary_reopen_of_a_resolved_id_still_works(self):
        """Contrast: after a RESOLVED disposition the id is gone from the open
        set, so a later round opening it again is a normal reopen, not a
        collision, and must take the new severity."""
        from orchestrator.services.verification_ledger import fold_open_findings

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "opened": [{"id": "F1", "severity": "high", "claim": "x"}],
                "dispositions": [],
            },
            {
                "round": 2,
                "critic_job_id": "c2",
                "opened": [],
                "dispositions": [
                    {"id": "F1", "disposition": "RESOLVED", "quote": "fixed"}
                ],
            },
            {
                "round": 3,
                "critic_job_id": "c3",
                "opened": [{"id": "F1", "severity": "low", "claim": "y"}],
                "dispositions": [],
            },
        ]

        open_findings = fold_open_findings(rounds)
        assert [f["severity"] for f in open_findings] == ["low"]


class TestTriggerVerificationInstructionsWiring:
    """Task 7: ``format_verification_instructions`` was rendered and then
    discarded — ``create_job`` has no ``instructions`` parameter, so every
    critic ran on a generic description instead of the rendered brief.

    Fixed by threading the rendered text through ``context["instructions"]``,
    the same delivery channel the scholar subjob already uses successfully
    (``_dispatch_job_to_agent`` extracts ``context["instructions"]`` and the
    agent writes it to ``instructions.md`` in the workspace — see
    ``src/agent.py``'s ``metadata.get("instructions")`` handling).

    This test exercises the REAL ``_trigger_verification_on_complete`` end to
    end and inspects what is actually handed to ``postgres_db.create_job``,
    so a future regression back to computing-and-discarding fails loudly
    here instead of silently in production.
    """

    @pytest.mark.asyncio
    async def test_instructions_reach_critic_context_with_prior_findings(
        self, monkeypatch
    ):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        # No critic in flight — this test is about the gate, not the
        # duplicate-spawn guard (which fails closed on the non-UUID id here).
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )

        prior_round = {
            "round": 1,
            "critic_job_id": "c1",
            "content_tree": "aaa",
            "verdict": "returned",
            "asserted_verdict": "returned",
            "opened": [{"id": "F1", "severity": "high", "claim": "still broken"}],
            "dispositions": [],
            "ts": "t",
        }
        job = _make_completion_job(
            freeze_content_tree="bbb", verification_rounds=[prior_round]
        )
        actions: list[str] = []

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, actions
        )

        create_job_mock.assert_awaited_once()
        context = create_job_mock.call_args.kwargs["context"]
        instructions = context.get("instructions")

        assert instructions, (
            "critic context is missing 'instructions' — the rendered brief "
            "was computed and discarded (the Task 7 defect)"
        )
        # The target job's own description reaches the critic.
        assert "do the thing" in instructions
        # The open finding left by the previous round is folded in — proves
        # prior_findings flows end-to-end, not just that SOME instructions
        # text was delivered.
        assert "F1" in instructions
        assert "still broken" in instructions

    @pytest.mark.asyncio
    async def test_a_later_critic_with_nothing_open_is_not_told_it_is_first(
        self, monkeypatch
    ):
        """The round COUNT must flow, not just the open findings. A round-3
        critic whose predecessors resolved everything has an empty open set —
        indistinguishable from round 1 unless `len(rounds)` reaches the
        renderer — and was being told "This is a first review."
        """
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            trigger_verification_on_complete as _trigger_verification_on_complete,
        )

        create_job_mock = AsyncMock(return_value={"id": "critic-999"})
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "create_job", create_job_mock
        )
        monkeypatch.setattr(
            job_dispatcher_module, "trigger_dispatch", lambda *, dependencies: None
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "has_live_verification_critic",
            AsyncMock(return_value=False),
        )
        monkeypatch.setattr(
            job_datasource_selection_module,
            "revalidate_job_datasource_selection",
            AsyncMock(return_value=([], {})),
        )

        rounds = [
            {
                "round": 1,
                "critic_job_id": "c1",
                "content_tree": "aaa",
                "verdict": "returned",
                "asserted_verdict": "returned",
                "opened": [{"id": "F1", "severity": "high", "claim": "broken"}],
                "dispositions": [],
                "ts": "t",
            },
            {
                "round": 2,
                "critic_job_id": "c2",
                "content_tree": "bbb",
                "verdict": "approved",
                "asserted_verdict": "approved",
                "opened": [],
                "dispositions": [
                    {"id": "F1", "disposition": "RESOLVED", "quote": "fixed"}
                ],
                "ts": "t",
            },
        ]
        job = _make_completion_job(
            freeze_content_tree="ccc", verification_rounds=rounds
        )

        await _trigger_verification_on_complete(
            job, {"error": None, "should_stop": True}, []
        )

        instructions = create_job_mock.call_args.kwargs["context"]["instructions"]
        assert "2 previous review rounds" in instructions
        assert "reviewer number 3" in instructions


class TestFailClosedVerdictHandling:
    def test_non_critic_subjob_is_ignored(self):
        """A delegation child has parent_job_id and freeze_data but no verdict.

        Without the verification_target discriminator it hits the implicit
        approval branch and advances its parent before siblings finish.
        """
        from orchestrator.services.verification_workflow import (
            is_verification_critic as _is_verification_critic,
        )

        assert (
            _is_verification_critic({"context": {"verification_target": "t1"}}) is True
        )
        assert _is_verification_critic({"context": {"scholar_target": "t1"}}) is False
        assert _is_verification_critic({"context": {}}) is False
        assert (
            _is_verification_critic({"context": '{"verification_target": "t1"}'})
            is True
        )

    def test_completed_critic_without_ledger_record_escalates(self, ledger_state):
        """No verdict must never mean approval."""
        from orchestrator.services.verification_workflow import (
            resolve_critic_outcome as _resolve_critic_outcome,
        )

        outcome, reason = _resolve_critic_outcome(
            critic_job_id="c1", critic_status="completed", rounds=[]
        )
        assert outcome == "escalate"
        assert "no verdict" in reason.lower()

    def test_failed_critic_with_verdict_still_escalates(self):
        """A critic that failed must not approve its target."""
        from orchestrator.services.verification_workflow import (
            resolve_critic_outcome as _resolve_critic_outcome,
        )

        outcome, _ = _resolve_critic_outcome(
            critic_job_id="c1",
            critic_status="failed",
            rounds=[{"critic_job_id": "c1", "verdict": "approved"}],
        )
        assert outcome == "escalate"

    def test_completed_critic_with_record_uses_computed_verdict(self):
        from orchestrator.services.verification_workflow import (
            resolve_critic_outcome as _resolve_critic_outcome,
        )

        outcome, _ = _resolve_critic_outcome(
            critic_job_id="c1",
            critic_status="completed",
            rounds=[{"critic_job_id": "c1", "verdict": "returned"}],
        )
        assert outcome == "returned"


def _make_critic_job(
    *,
    critic_job_id: str = "critic-1",
    target_job_id: str = "target-1",
    status: str = "completed",
    verdict_in_context: bool = True,
    creation_order=None,
) -> dict:
    """A minimal critic (or non-critic subjob) job row for
    _handle_critic_verdict_on_complete wiring tests."""
    context: dict = {}
    if verdict_in_context:
        context["verification_target"] = target_job_id
    return {
        "id": critic_job_id,
        "parent_job_id": target_job_id,
        "context": context,
        "status": status,
        "creation_order": creation_order,
        "freeze_data": None,
    }


def _make_target_job(
    *,
    job_id: str = "target-1",
    rounds=None,
    is_loop: bool = False,
) -> dict:
    context: dict = {"verification_rounds": rounds or []}
    if is_loop:
        context["loop_id"] = "loop-1"
    return {
        "id": job_id,
        "context": context,
        "resolved_config": {},
        "status": "reviewing",
    }


class TestHandleCriticVerdictOnCompleteWiring:
    """Wiring-level regression tests for the rewritten
    ``_handle_critic_verdict_on_complete``. ``TestFailClosedVerdictHandling``
    above proves the two pure helpers in isolation; these prove the real
    async function actually uses them end to end — including the two extra
    fail-closed properties that have no dedicated pure-function test:
    delegation children must not be misread as verdict-less critics, and a
    critic that hasn't reached a resting state (paused mid-retry) must not
    have its target judged at all yet.
    """

    @pytest.mark.asyncio
    async def test_delegation_child_is_ignored_not_read_as_verdictless_critic(
        self, monkeypatch
    ):
        """THE defect this task removes: previously any parent_job_id +
        freeze_data with no 'verdict' key was implicit approval — including a
        delegation child's own ordinary completion freeze, which would
        advance the parent before its siblings finish."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        get_job_mock = AsyncMock()
        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "get_job", get_job_mock
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = _make_critic_job(
            critic_job_id="child-1",
            verdict_in_context=False,
            creation_order=0,
        )
        job["freeze_data"] = {"freeze_type": "job_complete", "summary": "child done"}
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        get_job_mock.assert_not_awaited()
        update_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_non_actionable_critic_status_leaves_target_untouched(
        self, monkeypatch
    ):
        """A critic paused mid-retry (LLM outage, budget, vm-upgrade, ...)
        has not reached a resting state — escalating the target here would
        yank it out from under a critic that may still deliver a real
        verdict on its own once it resumes."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        get_job_mock = AsyncMock()
        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db, "get_job", get_job_mock
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = _make_critic_job(status="paused")
        job["freeze_data"] = {"freeze_type": "llm_unavailable"}
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        get_job_mock.assert_not_awaited()
        update_mock.assert_not_awaited()
        assert actions == []

    @pytest.mark.asyncio
    async def test_approved_sets_target_autonomy_status(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(
            rounds=[
                {
                    "round": 1,
                    "critic_job_id": "critic-1",
                    "verdict": "approved",
                    "asserted_verdict": "approved",
                    "opened": [],
                    "dispositions": [],
                    "ts": "t",
                }
            ]
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        set_status_mock = AsyncMock(return_value="completed")
        monkeypatch.setattr(
            subjob_completion_module,
            "set_target_to_autonomy_status",
            set_status_mock,
        )

        job = _make_critic_job()
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        assert set_status_mock.await_args.args == ("target-1",)
        assert any("approved" in a for a in actions)

    @pytest.mark.asyncio
    async def test_returned_resumes_target_with_rendered_findings(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(
            rounds=[
                {
                    "round": 1,
                    "critic_job_id": "critic-1",
                    "verdict": "returned",
                    "asserted_verdict": "returned",
                    "opened": [
                        {"id": "F1", "severity": "high", "claim": "missing tests"}
                    ],
                    "dispositions": [],
                    "ts": "t",
                }
            ]
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        resume_mock = AsyncMock()
        monkeypatch.setattr(
            job_controls_module.JobControlOperations,
            "internal_resume_job",
            resume_mock,
        )

        job = _make_critic_job()
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        resume_mock.assert_awaited_once()
        _, kwargs = resume_mock.call_args
        assert resume_mock.call_args.args[0] == "target-1"
        feedback = kwargs.get("feedback", "")
        assert "F1" in feedback
        assert "missing tests" in feedback
        assert "high" in feedback

    @pytest.mark.asyncio
    async def test_returned_resume_routes_through_freeze_clearing_write(
        self, monkeypatch
    ):
        """The parent of a 'returned' verdict always arrives frozen
        (``job_complete``), and ``get_dispatchable_jobs`` requires
        ``freeze_data IS NULL`` — so the critic resume MUST go through
        ``queue_job_for_resume``, the single write that sheds the freeze.
        The test above stubs ``_internal_resume_job`` wholesale, so it alone
        cannot see a regression back to a hand-rolled UPDATE that forgets
        the clear. This pins the seam one level deeper.
        See knowledge-history/done/critic_feedback_resume_parent_freeze_data_wedge.md
        (write contract locked against real Postgres in
        tests/test_queue_job_for_resume.py).
        """
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(
            rounds=[
                {
                    "round": 1,
                    "critic_job_id": "critic-1",
                    "verdict": "returned",
                    "asserted_verdict": "returned",
                    "opened": [
                        {"id": "F1", "severity": "high", "claim": "missing tests"}
                    ],
                    "dispositions": [],
                    "ts": "t",
                }
            ]
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        queue_mock = AsyncMock(return_value=True)
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "queue_job_for_resume",
            queue_mock,
        )
        monkeypatch.setattr(job_dispatcher_module, "trigger_dispatch", MagicMock())

        await _handle_critic_verdict_on_complete(_make_critic_job(), [])

        queue_mock.assert_awaited_once()
        args = queue_mock.await_args.args
        assert args[0] == "target-1"
        updates = args[1]
        assert "F1" in updates["queued_feedback"]
        assert updates["queued_feedback_reason"]

    @pytest.mark.asyncio
    async def test_returned_finding_without_severity_still_resumes_target(
        self, monkeypatch
    ):
        """``fold_open_findings`` guarantees a finding carries ``id`` but NOT
        ``severity`` (see its docstring). The rendering loop used to index
        ``f['severity']`` directly — a finding missing that key raises
        KeyError, which propagates out of this function. The /complete
        endpoint wraps this call in a bare ``try/except Exception: log`` (see
        orchestrator/main.py's complete_job step 3), so the exception is
        swallowed and the target is left in 'reviewing' forever: never
        resumed, never escalated. That silent wedge is exactly the failure
        class this whole plan exists to remove — a rendering bug must not be
        able to produce it.
        """
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(
            rounds=[
                {
                    "round": 1,
                    "critic_job_id": "critic-1",
                    "verdict": "returned",
                    "asserted_verdict": "returned",
                    # No "severity" key — assign_ids always sets one on the
                    # normal write path, but fold_open_findings' contract
                    # does not depend on that, and must not crash if it's
                    # ever absent (e.g. an older or hand-written record).
                    "opened": [{"id": "F1", "claim": "missing tests"}],
                    "dispositions": [],
                    "ts": "t",
                }
            ]
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        resume_mock = AsyncMock()
        monkeypatch.setattr(
            job_controls_module.JobControlOperations,
            "internal_resume_job",
            resume_mock,
        )

        job = _make_critic_job()
        actions: list[str] = []

        # Must not raise — that is the entire point of this test.
        await _handle_critic_verdict_on_complete(job, actions)

        resume_mock.assert_awaited_once()
        feedback = resume_mock.call_args.kwargs.get("feedback", "")
        assert "F1" in feedback
        assert "missing tests" in feedback
        assert "unknown" in feedback

    @pytest.mark.asyncio
    async def test_completed_with_no_ledger_record_escalates_target(self, monkeypatch):
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(rounds=[])
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = _make_critic_job()
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "pending_review"
        assert "no verdict" in update_mock.call_args.kwargs["error_message"].lower()

    @pytest.mark.asyncio
    async def test_completed_loop_target_escalates_to_completed(self, monkeypatch):
        """Escalation is status-aware: a project-loop target must resolve
        'completed' (never 'pending_review'), reusing _escalate_target."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(rounds=[], is_loop=True)
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )

        job = _make_critic_job()
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "completed"

    @pytest.mark.asyncio
    async def test_failed_critic_with_prior_approved_round_escalates_not_approves(
        self, monkeypatch
    ):
        """A critic that approved, then hit an infra error before its own
        job finished, must not approve the target — the scholar handler
        already gates on this ('a NON-TERMINAL scholar report must not
        unblock the parent'); the critic handler previously had no
        equivalent gate."""
        import orchestrator.main as main_module
        from tests.b08_completion_helpers import (
            handle_critic_verdict_on_complete as _handle_critic_verdict_on_complete,
        )

        target = _make_target_job(
            rounds=[
                {
                    "round": 1,
                    "critic_job_id": "critic-1",
                    "verdict": "approved",
                    "asserted_verdict": "approved",
                    "opened": [],
                    "dispositions": [],
                    "ts": "t",
                }
            ]
        )
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "get_job",
            AsyncMock(return_value=target),
        )
        update_mock = AsyncMock()
        monkeypatch.setattr(
            main_module.app.state.resources.postgres_db,
            "update_job_status",
            update_mock,
        )
        set_status_mock = AsyncMock()
        monkeypatch.setattr(
            subjob_completion_module,
            "set_target_to_autonomy_status",
            set_status_mock,
        )

        job = _make_critic_job(status="failed")
        actions: list[str] = []

        await _handle_critic_verdict_on_complete(job, actions)

        set_status_mock.assert_not_awaited()
        update_mock.assert_awaited_once()
        assert update_mock.call_args.kwargs["status"] == "pending_review"


# =============================================================================
# End-to-end continuity (Task 11): the gap that made the incident possible
# was that nothing carried findings from one round to the next, so a fresh
# critic reviewed blind and could approve over an open blocking finding it
# had never been told about.
# =============================================================================


class TestMultiRoundContinuity:
    @pytest.mark.asyncio
    async def test_round_two_critic_is_told_about_round_one_findings(
        self, fake_db, ledger_state
    ):
        """The gap that made the incident possible: nothing carried findings
        from one round to the next."""
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )
        from orchestrator.services.verification_ledger import (
            fold_open_findings,
            render_prior_findings,
        )

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "missing walnut-shell source"}],
            dispositions=[],
            head_commit="aaa",
        )

        # This is exactly what format_verification_instructions folds into
        # the round-2 critic's brief (see _trigger_verification_on_complete),
        # so it proves what the NEXT critic is actually told, not just what
        # the ledger stores.
        brief = render_prior_findings(fold_open_findings(ledger_state["rounds"]))
        assert "F1" in brief
        assert "missing walnut-shell source" in brief
        assert "may not close a finding by re-judging" in brief

    @pytest.mark.asyncio
    async def test_round_three_cannot_approve_over_open_finding(
        self, fake_db, ledger_state
    ):
        """The incident itself, as an assertion: job 52949749 was returned
        twice at severity high, then approved on a byte-identical
        deliverable by a fresh critic that never saw the findings.

        Round 1 opens F1 (high) and returns. Round "3" here is a fresh
        critic with no memory of round 1 — it never engages with the
        finding on its merits, it only DISPUTES it (the critic's opinion
        that the finding doesn't apply), which — unlike RESOLVED — does NOT
        close a finding (see fold_open_findings / render_prior_findings:
        "You may not close a finding by re-judging it"). The computed
        verdict must stay 'returned' regardless of what the critic asserted.
        """
        from tests.b08_completion_helpers import (
            record_verification_round as _record_verification_round_impl,
        )

        await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c1",
            asserted_verdict="returned",
            opened=[{"severity": "high", "claim": "missing walnut-shell source"}],
            dispositions=[],
            head_commit="aaa",
        )
        result = await _record_verification_round_impl(
            postgres_db=fake_db,
            target_job_id="t1",
            critic_job_id="c3",
            asserted_verdict="approved",
            opened=[],
            dispositions=[
                {
                    "id": "F1",
                    "disposition": "DISPUTED",
                    "reason": "covered in the safety section",
                }
            ],
            head_commit="aaa",
        )

        assert result["verdict"] == "returned"
        assert [f["id"] for f in result["open_findings"]] == ["F1"]
