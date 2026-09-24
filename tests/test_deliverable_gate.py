"""P1-C deliverable-contract gate at the seal (orchestrator side).

knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md §4 P1-C / §7 annex E.

Verifier #1 sealed "26/27 todos done" with 0/7 required deliverables and
consumed a human-priced officer review cycle; F14's validator rejected a
COMPLETE job's correct deliverable list over a missing ``repo/`` prefix.

Covered here:
  - manifest parsing + path normalization (both sides tolerate ``repo/``
    and ``./`` — the F14 regression shape)
  - gate pass → seal proceeds, ``context.deliverable_gate`` stamped
  - missing → bounce through the P1-A resume-with-feedback lane with a
    PRECISE missing/present reason; no seal, caller told to early-return
  - ordinary in-repo bounce cap → ``pending_review`` (loop jobs retain their
    historical terminal handling); explicit external publication cap →
    terminal blocked/undelivered, including loop jobs
  - Gitea unavailable / repo unresolvable → fail-open skip with stamp
  - no manifest / no completion claim → no-op
  - ``kb:<slug>`` entries: verified against the knowledge_index when the
    vector store answers; fail-open (never "missing") when it can't
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from orchestrator.services.deliverable_gate import (  # noqa: E402
    DELIVERABLE_GATE_BOUNCE_CAP,
    cloned_repo_deliverables,
    evaluate_deliverable_gate,
    gate_applies,
    is_cloned_repo_deliverable,
    normalize_deliverable_path,
    parse_required_deliverables,
    run_deliverable_gate,
)
from orchestrator.services.deliverable_contracts import (  # noqa: E402
    BLOCKED_UNDELIVERED_OUTCOME,
    DeliveryContractConflict,
    prepare_delivery_contract,
)
from orchestrator.schemas import job_create as job_create_module

# =============================================================================
# Fixtures
# =============================================================================

JOB_ID = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
SHA = "c0ffee1234deadbeef5678"
DATASOURCE_ID = "22222222-2222-4222-8222-222222222222"
PR_RECORD_ID = "33333333-3333-4333-8333-333333333333"
PR_HEAD_SHA = "d" * 40


def repository_datasource(
    *, read_only: bool = False, project_read_only: bool = False
) -> dict:
    return {
        "id": DATASOURCE_ID,
        "type": "repository",
        "connection_url": "https://github.com/Acme/Widget.git",
        "config": {"forge": "github"},
        "read_only": read_only,
        "project_read_only": project_read_only,
        "policy_revision": 7,
    }


def make_job(
    *,
    manifest: list[str] | None = None,
    context_extra: dict | None = None,
    repo_name: str | None = "job-aaaaaaaa",
    branch_name: str | None = None,
    parent_job_id: str | None = None,
    project_id: str | None = None,
    freeze_data: dict | None = None,
) -> dict:
    context: dict = dict(context_extra or {})
    if manifest is not None:
        context["required_deliverables"] = manifest
    return {
        "id": JOB_ID,
        "status": "processing",
        "repo_name": repo_name,
        "branch_name": branch_name,
        "parent_job_id": parent_job_id,
        "project_id": project_id,
        "context": context,
        "freeze_data": freeze_data,
    }


def completion_result(goal_achieved: bool = True) -> dict:
    return {
        "should_stop": True,
        "goal_achieved": goal_achieved,
        "freeze_data": {"freeze_type": "job_complete", "summary": "done"},
    }


def make_gitea(tree_paths: list[str] | None, *, initialized: bool = True):
    gitea = MagicMock()
    gitea.is_initialized = initialized
    gitea.list_tree = AsyncMock(
        return_value=(
            None
            if tree_paths is None
            else [{"path": p, "type": "blob", "sha": "x"} for p in tree_paths]
        )
    )
    gitea.get_branch_head_sha = AsyncMock(return_value=SHA)
    return gitea


def make_db():
    db = AsyncMock()
    db.merge_job_context = AsyncMock(return_value=True)
    db.get_job = AsyncMock(return_value=None)
    db.get_job_deliverable_contract = AsyncMock(return_value=None)
    db.get_job_pull_request_authority = AsyncMock(return_value=None)
    db.resolve_datasources_for_job = AsyncMock(return_value=[])
    db.mark_job_pr_deliverable_verified = AsyncMock(return_value=True)
    return db


def make_pr_job(*, repo: str = "acme/widget", with_record: bool = True) -> dict:
    extra = {}
    if with_record:
        extra["pull_request"] = {
            "forge": "github",
            "repo": repo,
            "number": 9,
            "url": "https://github.com/acme/widget/pull/9",
            "head": "feature/delivery",
            "base": "develop",
        }
    return make_job(
        manifest=["pr:acme/widget"],
        context_extra=extra,
        project_id="11111111-2222-4333-8444-555555555555",
    )


def configure_pr_contract(db, *, datasource: dict | None = None) -> None:
    row = datasource or repository_datasource()
    db.get_job_deliverable_contract = AsyncMock(
        return_value={
            "pr_repositories": ["acme/widget"],
            "pr_bindings": [
                {
                    "repository": "acme/widget",
                    "datasource_id": DATASOURCE_ID,
                    "forge": "github",
                    "policy_revision": 7,
                }
            ],
        }
    )
    db.get_job_pull_request_authority = AsyncMock(
        return_value={
            "record_id": PR_RECORD_ID,
            "record_generation": 1,
            "datasource_id": DATASOURCE_ID,
            "repository": "acme/widget",
            "forge": "github",
            "number": 9,
            "head": "feature/delivery",
            "base": "develop",
            "source_revision": PR_HEAD_SHA,
            "policy_revision": 7,
        }
    )
    db.resolve_datasources_for_job = AsyncMock(return_value=[row])


def make_vector_db(existing_slugs: set[str]):
    """vector_db.acquire() async-context whose fetchrow knows those slugs."""
    conn = AsyncMock()

    async def fetchrow(_query, _project_id, slug):
        return {"ok": 1} if slug in existing_slugs else None

    conn.fetchrow = AsyncMock(side_effect=fetchrow)
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=conn)
    ctx.__aexit__ = AsyncMock(return_value=False)
    vector_db = MagicMock()
    vector_db.acquire = MagicMock(return_value=ctx)
    return vector_db


def stamped(db) -> dict:
    """The last context.deliverable_gate stamp written through the mock db."""
    assert db.merge_job_context.await_count >= 1
    args = db.merge_job_context.await_args_list[-1][0]
    assert args[0] == JOB_ID
    return args[1]["deliverable_gate"]


# =============================================================================
# Normalization (F14)
# =============================================================================


class TestNormalization:
    def test_repo_prefix_stripped(self):
        assert normalize_deliverable_path("repo/output/a.md") == "output/a.md"

    def test_dot_slash_and_leading_slash_stripped(self):
        assert normalize_deliverable_path("./output/a.md") == "output/a.md"
        assert normalize_deliverable_path("./repo/output/a.md") == "output/a.md"
        assert normalize_deliverable_path("/output/a.md") == "output/a.md"

    def test_kb_entries_keep_prefix(self):
        assert normalize_deliverable_path("kb: my-note ") == "kb:my-note"
        assert normalize_deliverable_path("kb:") is None

    def test_garbage_dropped(self):
        assert normalize_deliverable_path("") is None
        assert normalize_deliverable_path("   ") is None
        assert normalize_deliverable_path(42) is None
        assert normalize_deliverable_path(None) is None

    def test_parse_dedupes_across_spellings(self):
        manifest = parse_required_deliverables(
            {
                "required_deliverables": [
                    "repo/output/a.md",
                    "./output/a.md",
                    "output/b.md",
                    "",
                    17,
                ]
            }
        )
        assert manifest == ["output/a.md", "output/b.md"]

    def test_parse_accepts_json_string_context_and_bare_values(self):
        assert parse_required_deliverables(
            '{"required_deliverables": ["output/a.md"]}'
        ) == ["output/a.md"]
        assert parse_required_deliverables("not json") == []
        assert parse_required_deliverables(
            {"required_deliverables": "output/a.md"}
        ) == ["output/a.md"]
        assert parse_required_deliverables({}) == []
        assert parse_required_deliverables(None) == []


# =============================================================================
# Trigger predicate
# =============================================================================


class TestGateApplies:
    def test_applies_to_claimed_completion_with_manifest(self):
        job = make_job(manifest=["output/a.md"])
        assert gate_applies(job, completion_result(), "completed") is True
        assert gate_applies(job, completion_result(), "pending_review") is True
        assert gate_applies(job, completion_result(), "reviewing") is True

    def test_no_manifest_is_noop(self):
        job = make_job(manifest=None)
        assert gate_applies(job, completion_result(), "completed") is False

    def test_honest_incomplete_stop_is_not_gated(self):
        """pending_review WITHOUT a completion claim must never bounce."""
        job = make_job(manifest=["output/a.md"])
        result = {"should_stop": True, "goal_achieved": False, "freeze_data": None}
        assert gate_applies(job, result, "pending_review") is False

    def test_row_freeze_claim_counts(self):
        """The handler wrote the job_complete freeze to the row already."""
        job = make_job(
            manifest=["output/a.md"],
            freeze_data={"freeze_type": "job_complete"},
        )
        result = {"should_stop": True, "goal_achieved": False}
        assert gate_applies(job, result, "pending_review") is True

    def test_non_gated_statuses_pass_through(self):
        job = make_job(manifest=["output/a.md"])
        for status in ("paused", "failed", "waiting", None):
            assert gate_applies(job, completion_result(), status) is False


# =============================================================================
# Evaluation against the Gitea tree
# =============================================================================


class TestEvaluate:
    @pytest.mark.asyncio
    async def test_prefix_tolerant_both_sides(self):
        """F14 regression shape: unprefixed manifest ↔ prefixed tree and
        vice versa must both count as present."""
        job = make_job(manifest=["repo/output/x.md", "./output/y.md"])
        gitea = make_gitea(["output/x.md", "repo/output/y.md"])
        report = await evaluate_deliverable_gate(job, db=make_db(), gitea=gitea)
        assert report["passed"] is True
        assert report["missing"] == []
        assert sorted(report["present"]) == ["output/x.md", "output/y.md"]
        assert report["commit_sha"] == SHA

    @pytest.mark.asyncio
    async def test_missing_listed_precisely(self):
        job = make_job(manifest=["output/a.md", "output/b.md"])
        gitea = make_gitea(["output/a.md", "unrelated.txt"])
        report = await evaluate_deliverable_gate(job, db=make_db(), gitea=gitea)
        assert report["passed"] is False
        assert report["missing"] == ["output/b.md"]
        assert report["present"] == ["output/a.md"]

    @pytest.mark.asyncio
    async def test_gitea_uninitialized_skips(self):
        job = make_job(manifest=["output/a.md"])
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([], initialized=False)
        )
        assert report["skipped"] is True

    @pytest.mark.asyncio
    async def test_unresolvable_repo_skips(self):
        job = make_job(manifest=["output/a.md"], repo_name=None)
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(["output/a.md"])
        )
        assert report["skipped"] is True

    @pytest.mark.asyncio
    async def test_subjob_resolves_parent_repo(self):
        job = make_job(
            manifest=["output/a.md"],
            repo_name=None,
            branch_name="job/aaaaaaaa",
            parent_job_id="99999999-8888-7777-6666-555555555555",
        )
        db = make_db()
        db.get_job = AsyncMock(return_value={"repo_name": "parent-repo"})
        gitea = make_gitea(["output/a.md"])
        report = await evaluate_deliverable_gate(job, db=db, gitea=gitea)
        assert report["passed"] is True
        gitea.list_tree.assert_awaited_once_with("parent-repo", "job/aaaaaaaa")

    @pytest.mark.asyncio
    async def test_unreadable_tree_skips(self):
        job = make_job(manifest=["output/a.md"])
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(None)
        )
        assert report["skipped"] is True

    @pytest.mark.asyncio
    async def test_undelivered_completion_skips(self):
        """A failed job-ending push is infrastructure, not a missing deliverable.

        The agent sets ``delivery_failed`` when its final push does not land
        (src/core/phase.py, _push_job_ending_state). The files exist; they are
        on a workspace pod about to be reclaimed, and Gitea is empty or stale.

        This gate reads Gitea, so without the check every manifest entry reads
        "missing" and the job is bounced back to the agent to produce files it
        already produced — onto a workspace that may no longer exist. It is the
        one infrastructure failure that looks like a CLEAN read: the tree is
        perfectly readable, it is just empty, which is why the four existing
        skip cases do not catch it.

        knowledge-history/done/git_push_fails_silently_via_workspace_backend.md
        """
        job = make_job(
            manifest=["output/a.md"],
            freeze_data={
                "freeze_type": "job_complete",
                "delivery_failed": True,
                "delivery_error": "The job-ending git push failed at job completion.",
            },
        )
        # A readable, EMPTY tree — exactly what an undelivered job leaves behind.
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([])
        )
        assert report["skipped"] is True
        # The agent's own reason is carried through, not replaced by a generic
        # one — it is what reaches the stamp an operator reads.
        assert report["reason"] == "The job-ending git push failed at job completion."

    @pytest.mark.asyncio
    async def test_undelivered_without_a_reason_still_skips(self):
        """delivery_error is optional; the flag alone must be enough."""
        job = make_job(
            manifest=["output/a.md"],
            freeze_data={"freeze_type": "job_complete", "delivery_failed": True},
        )
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([])
        )
        assert report["skipped"] is True
        assert "push" in str(report["reason"]).lower()

    @pytest.mark.asyncio
    async def test_delivered_completion_is_still_evaluated(self):
        """Contrast: the check must read the flag, not skip every completion."""
        job = make_job(
            manifest=["output/a.md"],
            freeze_data={"freeze_type": "job_complete"},
        )
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([])
        )
        assert report.get("skipped") is not True
        assert report["passed"] is False
        assert report["missing"] == ["output/a.md"]

    @pytest.mark.asyncio
    async def test_kb_entries_verified_and_fail_open(self):
        job = make_job(
            manifest=["kb:present-note", "kb:absent-note"],
            project_id="11111111-2222-3333-4444-555555555555",
        )
        gitea = make_gitea([])
        report = await evaluate_deliverable_gate(
            job,
            db=make_db(),
            gitea=gitea,
            vector_db=make_vector_db({"present-note"}),
        )
        assert report["present"] == ["kb:present-note"]
        assert report["missing"] == ["kb:absent-note"]

        # No vector store → unverifiable, NEVER missing (fail-open, logged).
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=gitea, vector_db=None
        )
        assert report["passed"] is True
        assert report["missing"] == []
        assert sorted(report["unverified"]) == ["kb:absent-note", "kb:present-note"]


class TestPullRequestDeliverable:
    def test_honest_negative_pr_report_still_requires_delivery_proof(self):
        assert gate_applies(
            make_pr_job(with_record=False),
            completion_result(goal_achieved=False),
            "pending_review",
        )

    @pytest.mark.asyncio
    async def test_missing_or_wrong_pr_record_fails_closed(self):
        missing_db = make_db()
        missing_db.get_job_deliverable_contract = AsyncMock(
            return_value={"pr_repositories": ["acme/widget"], "pr_bindings": [{}]}
        )
        missing = await evaluate_deliverable_gate(
            make_pr_job(with_record=False), db=missing_db, gitea=make_gitea([])
        )
        db = make_db()
        configure_pr_contract(db)
        db.get_job_pull_request_authority.return_value = {
            **db.get_job_pull_request_authority.return_value,
            "repository": "other/private",
        }
        wrong = await evaluate_deliverable_gate(
            make_pr_job(repo="other/private"), db=db, gitea=make_gitea([])
        )
        assert missing["strict"] is True and missing["passed"] is False
        assert wrong["strict"] is True and wrong["passed"] is False
        missing_db.mark_job_pr_deliverable_verified.assert_not_awaited()
        db.mark_job_pr_deliverable_verified.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_detached_or_read_only_connector_fails_closed(self):
        for datasource in (
            None,
            repository_datasource(read_only=True),
            repository_datasource(project_read_only=True),
        ):
            db = make_db()
            configure_pr_contract(db)
            db.resolve_datasources_for_job = AsyncMock(
                return_value=[] if datasource is None else [datasource]
            )
            report = await evaluate_deliverable_gate(
                make_pr_job(), db=db, gitea=make_gitea([])
            )
            assert report["strict"] is True and report["passed"] is False

    @pytest.mark.asyncio
    async def test_unavailable_forge_fails_closed(self):
        db = make_db()
        configure_pr_contract(db)
        with patch(
            "orchestrator.services.deliverable_gate.get_pull_request_status",
            side_effect=OSError("forge unavailable"),
        ):
            report = await evaluate_deliverable_gate(
                make_pr_job(), db=db, gitea=make_gitea([])
            )
        assert report["strict"] is True and report["passed"] is False
        assert "could not be verified" in report["reason"]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("state", ["open", "merged"])
    async def test_matching_live_pr_is_proven_without_changing_merge_policy(
        self, state
    ):
        db = make_db()
        configure_pr_contract(db)
        with patch(
            "orchestrator.services.deliverable_gate.get_pull_request_status",
            return_value={
                "state": state,
                "head": "feature/delivery",
                "base": "develop",
                "head_sha": PR_HEAD_SHA,
            },
        ):
            report = await evaluate_deliverable_gate(
                make_pr_job(), db=db, gitea=make_gitea([])
            )
        assert report["passed"] is True
        db.mark_job_pr_deliverable_verified.assert_awaited_once_with(
            JOB_ID,
            datasource_id=DATASOURCE_ID,
            repository="acme/widget",
            number=9,
            record_id=PR_RECORD_ID,
            record_generation=1,
            head="feature/delivery",
            base="develop",
            head_revision=PR_HEAD_SHA,
            state=state,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "live",
        [
            {
                "state": "open",
                "head": "unrelated",
                "base": "develop",
                "head_sha": PR_HEAD_SHA,
            },
            {
                "state": "open",
                "head": "feature/delivery",
                "base": "main",
                "head_sha": PR_HEAD_SHA,
            },
            {
                "state": "open",
                "head": "feature/delivery",
                "base": "develop",
                "head_sha": "e" * 40,
            },
        ],
    )
    async def test_live_pr_identity_must_match_authoritative_head_and_base(self, live):
        db = make_db()
        configure_pr_contract(db)
        with patch(
            "orchestrator.services.deliverable_gate.get_pull_request_status",
            return_value=live,
        ):
            report = await evaluate_deliverable_gate(
                make_pr_job(), db=db, gitea=make_gitea([])
            )
        assert report["passed"] is False
        assert "different or incomplete" in report["reason"]
        db.mark_job_pr_deliverable_verified.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_strict_bounce_cap_terminalizes_blocked_not_completed(self):
        job = make_pr_job(with_record=False)
        job["context"]["deliverable_gate"] = {"bounces": DELIVERABLE_GATE_BOUNCE_CAP}
        db = make_db()
        configure_pr_contract(db)
        result = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert result.status == "cancelled"
        assert result.outcome_kind == BLOCKED_UNDELIVERED_OUTCOME
        assert result.bounced is False

    @pytest.mark.asyncio
    async def test_strict_loop_bounce_cap_terminalizes_without_review_wedge(self):
        job = make_pr_job(with_record=False)
        job["context"].update(
            {
                "loop_id": "loop-1",
                "deliverable_gate": {
                    "bounces": DELIVERABLE_GATE_BOUNCE_CAP,
                },
            }
        )
        db = make_db()
        configure_pr_contract(db)

        result = await run_deliverable_gate(
            job,
            completion_result(goal_achieved=False),
            "pending_review",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )

        assert result.status == "cancelled"
        assert result.outcome_kind == BLOCKED_UNDELIVERED_OUTCOME
        assert result.bounced is False

    @pytest.mark.asyncio
    async def test_strict_queue_failure_terminalizes_blocked(self):
        db = make_db()
        configure_pr_contract(db)
        result = await run_deliverable_gate(
            make_pr_job(with_record=False),
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(side_effect=OSError("queue unavailable")),
        )
        assert result.status == "cancelled"
        assert result.outcome_kind == BLOCKED_UNDELIVERED_OUTCOME

    @pytest.mark.asyncio
    async def test_strict_queue_cas_refusal_terminalizes_blocked(self):
        """A false queue result is a failure, not a successfully installed bounce."""

        db = make_db()
        configure_pr_contract(db)
        result = await run_deliverable_gate(
            make_pr_job(with_record=False),
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(return_value=False),
        )

        assert result.status == "cancelled"
        assert result.outcome_kind == BLOCKED_UNDELIVERED_OUTCOME
        assert result.bounced is False


# =============================================================================
# The gate — pass / bounce / cap / skip
# =============================================================================


class TestRunGate:
    @pytest.mark.asyncio
    async def test_undelivered_completion_does_not_bounce(self):
        """The composition that makes the skip worth having.

        A bounce here early-returns in the caller (orchestrator/main.py, right
        after apply_deliverable_gate), skipping the status write, the subjob
        graft, the critic spawn and the loop advance. So before this skip
        existed, an undelivered job with a manifest was bounced back to redo
        work it had already done, and never reached the verification escalation
        that would have reported the real reason.

        Asserting only ``report["skipped"]`` at the evaluate level would leave
        that ordering unproven.

        Not bouncing is not the same as accepting: the repository does not
        hold the work, so a ``completed`` seal (which merges that branch) is
        held for review instead, like the bounce cap does
        (worker_git_versioning_stops_midjob_seal_pins_stale_revision.md).
        """
        job = make_job(
            manifest=["output/a.md", "output/b.md"],
            freeze_data={
                "freeze_type": "job_complete",
                "delivery_failed": True,
                "delivery_error": "The job-ending git push failed at job completion.",
            },
        )
        db = make_db()
        queue_resume = AsyncMock()

        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),  # readable but empty — the undelivered shape
            queue_resume=queue_resume,
        )

        assert bounced is False
        assert new_status == "pending_review"
        queue_resume.assert_not_awaited()
        stamp = stamped(db)
        assert stamp["skipped"] is True
        assert stamp["undelivered"] is True
        assert stamp["reason"] == "The job-ending git push failed at job completion."

    @pytest.mark.asyncio
    async def test_pass_seals_and_stamps(self):
        job = make_job(manifest=["output/a.md"])
        db = make_db()
        queue_resume = AsyncMock()
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea(["output/a.md"]),
            queue_resume=queue_resume,
        )
        assert (new_status, bounced) == ("completed", False)
        assert any("deliverable gate passed" in a for a in actions)
        queue_resume.assert_not_awaited()
        stamp = stamped(db)
        assert stamp["passed"] is True
        assert stamp["commit_sha"] == SHA
        assert stamp["bounces"] == 0

    @pytest.mark.asyncio
    async def test_missing_bounces_with_precise_reason(self):
        job = make_job(manifest=["output/a.md", "output/b.md"])
        db = make_db()
        queue_resume = AsyncMock()
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea(["output/a.md"]),
            queue_resume=queue_resume,
        )
        assert bounced is True
        assert new_status is None
        queue_resume.assert_awaited_once()
        args, kwargs = queue_resume.await_args
        assert args[0] == JOB_ID
        feedback = args[1]
        # The precise listing: missing AND present, at the checked sha.
        assert "output/b.md" in feedback
        assert "output/a.md" in feedback
        assert "MISSING (1)" in feedback
        assert "PRESENT (1)" in feedback
        assert SHA[:12] in feedback
        reason = kwargs["reason"]
        assert "1 of 2" in reason
        assert "bounce 1/2" in reason
        stamp = stamped(db)
        assert stamp["passed"] is False
        assert stamp["bounces"] == 1
        assert stamp["missing"] == ["output/b.md"]

    @pytest.mark.asyncio
    async def test_second_bounce_increments(self):
        job = make_job(
            manifest=["output/b.md"],
            context_extra={"deliverable_gate": {"passed": False, "bounces": 1}},
        )
        db = make_db()
        queue_resume = AsyncMock()
        _, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=queue_resume,
        )
        assert bounced is True
        assert stamped(db)["bounces"] == 2

    @pytest.mark.asyncio
    async def test_cap_falls_through_to_pending_review_with_report(self):
        job = make_job(
            manifest=["output/b.md"],
            context_extra={
                "deliverable_gate": {
                    "passed": False,
                    "bounces": DELIVERABLE_GATE_BOUNCE_CAP,
                }
            },
        )
        db = make_db()
        queue_resume = AsyncMock()
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=queue_resume,
        )
        assert bounced is False
        assert new_status == "pending_review"
        queue_resume.assert_not_awaited()
        stamp = stamped(db)
        assert stamp["cap_reached"] is True
        assert stamp["missing"] == ["output/b.md"]
        assert any("cap reached" in a for a in actions)

    @pytest.mark.asyncio
    async def test_cap_keeps_loop_job_terminal(self):
        """pending_review wedges a project loop — cap resolves it completed."""
        job = make_job(
            manifest=["output/b.md"],
            context_extra={
                "loop_id": "some-loop",
                "deliverable_gate": {
                    "passed": False,
                    "bounces": DELIVERABLE_GATE_BOUNCE_CAP,
                },
            },
        )
        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=make_db(),
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert bounced is False
        assert new_status == "completed"

    @pytest.mark.asyncio
    async def test_cap_leaves_reviewing_untouched(self):
        """At the cap a verification-enabled job proceeds to its critic,
        which reads the stamped report."""
        job = make_job(
            manifest=["output/b.md"],
            context_extra={
                "deliverable_gate": {
                    "passed": False,
                    "bounces": DELIVERABLE_GATE_BOUNCE_CAP,
                }
            },
        )
        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "reviewing",
            db=make_db(),
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert (new_status, bounced) == ("reviewing", False)

    @pytest.mark.asyncio
    async def test_reviewing_bounce_prevents_critic_spawn(self):
        """A bounced seal must not spawn the critic — the gate intercepts
        the reviewing lane (deterministic checks first, critic LLM second)."""
        job = make_job(manifest=["output/b.md"])
        queue_resume = AsyncMock()
        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "reviewing",
            db=make_db(),
            gitea=make_gitea([]),
            queue_resume=queue_resume,
        )
        assert bounced is True
        assert new_status is None
        queue_resume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_gitea_down_fails_open(self):
        job = make_job(manifest=["output/a.md"])
        db = make_db()
        queue_resume = AsyncMock()
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([], initialized=False),
            queue_resume=queue_resume,
        )
        assert (new_status, bounced) == ("completed", False)
        queue_resume.assert_not_awaited()
        stamp = stamped(db)
        assert stamp["skipped"] is True
        assert "gitea" in stamp["reason"]
        assert any("skipped" in a for a in actions)

    @pytest.mark.asyncio
    async def test_no_manifest_is_a_noop(self):
        job = make_job(manifest=None)
        db = make_db()
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert (new_status, actions, bounced) == ("completed", [], False)
        db.merge_job_context.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_failed_bounce_queue_falls_through_sealed(self):
        """A broken resume queue must not strand the job refused-but-unbounced."""
        job = make_job(manifest=["output/b.md"])
        queue_resume = AsyncMock(side_effect=RuntimeError("db down"))
        new_status, actions, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=make_db(),
            gitea=make_gitea([]),
            queue_resume=queue_resume,
        )
        assert bounced is False
        assert new_status == "completed"
        assert any("FAILED to queue" in a for a in actions)

    @pytest.mark.asyncio
    async def test_completion_hook_delegates(self):
        """services.completion.apply_deliverable_gate is the thin hook the
        /complete handler calls — same result as the module entry point."""
        from orchestrator.services.completion import apply_deliverable_gate

        job = make_job(manifest=["output/a.md"])
        db = make_db()
        new_status, actions, bounced = await apply_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea(["repo/output/a.md"]),
            queue_resume=AsyncMock(),
        )
        assert (new_status, bounced) == ("completed", False)
        assert stamped(db)["passed"] is True


# =============================================================================
# A seal that cannot name its revision is not a delivery
# =============================================================================


class TestUnprovenDeliveryIsNotAccepted:
    """knowledge-base/knowledge/issues/worker_git_versioning_stops_midjob_seal_pins_stale_revision.md

    VM job afa0004b sealed with ``head_commit: None``: the worker could not
    read its own HEAD, because its git had stopped working two hours earlier.
    The gate then found all three manifest files at the branch tip — the
    phase-3 scaffolds — and passed. Existence at the tip proves nothing when
    the worker cannot say the tip is what it delivered.
    """

    MANIFEST = ["output/environment.md", "output/obstacles.md"]
    # The stale tip really does hold the files: that is what made it pass.
    STALE_TIP = ["output/environment.md", "output/obstacles.md", "plan.md"]

    @staticmethod
    def _freeze(**extra) -> dict:
        return {
            "freeze_type": "job_complete",
            "summary": "done",
            "head_commit": None,
            "content_tree": None,
            **extra,
        }

    @pytest.mark.asyncio
    async def test_null_head_commit_is_not_accepted_as_delivered(self):
        job = make_job(manifest=self.MANIFEST, freeze_data=self._freeze())

        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(self.STALE_TIP)
        )

        assert report.get("passed") is not True
        assert report["undelivered"] is True
        assert "head_commit" in report["reason"]

    @pytest.mark.asyncio
    async def test_null_head_commit_holds_a_completed_seal_for_review(self):
        """Not bounced (resuming the worker cannot repair its git), and not
        sealed ``completed`` either — that would merge the stale branch."""
        job = make_job(manifest=self.MANIFEST, freeze_data=self._freeze())
        db = make_db()
        queue_resume = AsyncMock()

        decision = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea(self.STALE_TIP),
            queue_resume=queue_resume,
        )
        new_status, actions, bounced = decision

        assert (new_status, bounced) == ("pending_review", False)
        queue_resume.assert_not_awaited()
        stamp = stamped(db)
        assert stamp.get("passed") is not True
        assert stamp["undelivered"] is True
        assert not any("gate passed" in a for a in actions)
        assert any("delivery unproven" in a for a in actions)
        # The completion authority turns this into error_message and the
        # review notification's reason — a hold must never be silent.
        assert "head_commit is null" in decision.hold_reason

    @pytest.mark.asyncio
    async def test_null_head_commit_keeps_a_loop_job_terminal(self):
        """Same escalation rule as the bounce cap: a parked loop job would
        wedge its loop, so it resolves terminally with the report stamped."""
        job = make_job(
            manifest=self.MANIFEST,
            context_extra={"loop_id": "some-loop"},
            freeze_data=self._freeze(),
        )
        db = make_db()

        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea(self.STALE_TIP),
            queue_resume=AsyncMock(),
        )

        assert (new_status, bounced) == ("completed", False)
        assert stamped(db)["undelivered"] is True

    @pytest.mark.asyncio
    @pytest.mark.parametrize("status", ["pending_review", "reviewing"])
    async def test_a_review_bound_seal_keeps_its_lane_but_not_a_pass(self, status):
        job = make_job(manifest=self.MANIFEST, freeze_data=self._freeze())
        db = make_db()

        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(goal_achieved=False),
            status,
            db=db,
            gitea=make_gitea(self.STALE_TIP),
            queue_resume=AsyncMock(),
        )

        assert (new_status, bounced) == (status, False)
        assert stamped(db).get("passed") is not True

    @pytest.mark.asyncio
    async def test_a_verified_delivered_commit_is_evaluated_normally(self):
        """``delivered_commit`` is the worker's positive proof (clean tree,
        nothing unpushed, HEAD read after the final commit) — a failed
        pre-commit HEAD read alongside it is only a diagnostic gap."""
        job = make_job(
            manifest=self.MANIFEST,
            freeze_data=self._freeze(delivered_commit="f" * 40),
        )

        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(self.STALE_TIP)
        )

        assert report["passed"] is True

    @pytest.mark.asyncio
    async def test_a_freeze_without_the_key_is_evaluated_normally(self):
        """Absent is not null: records from writers that never carried the
        key are judged on the tree alone, as before."""
        job = make_job(
            manifest=self.MANIFEST,
            freeze_data={"freeze_type": "job_complete", "summary": "done"},
        )

        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(self.STALE_TIP)
        )

        assert report["passed"] is True

    @pytest.mark.asyncio
    async def test_a_lite_job_without_git_is_evaluated_normally(self):
        """Lite tiers have no git at all; a null HEAD is their normal state."""
        job = make_job(manifest=self.MANIFEST, freeze_data=self._freeze())
        job["config_override"] = {"workspace": {"backend": "virtual"}}

        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(self.STALE_TIP)
        )

        assert report["passed"] is True

    @pytest.mark.asyncio
    async def test_a_job_without_a_repository_still_skips(self):
        """No repository means nothing was supposed to be pushed; the
        unresolvable-repo fail-open stands."""
        job = make_job(
            manifest=self.MANIFEST, repo_name=None, freeze_data=self._freeze()
        )

        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(self.STALE_TIP)
        )

        assert report["skipped"] is True
        assert report.get("undelivered") is not True

    @pytest.mark.asyncio
    async def test_a_manifest_outside_the_job_tree_keeps_its_old_handling(self):
        """Only a manifest naming paths in the job's own tree is a repository
        deliverable; a KB-only contract never depended on the branch tip."""
        job = make_job(
            manifest=["kb:findings"],
            freeze_data=self._freeze(delivery_failed=True),
        )

        new_status, _, bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=make_db(),
            gitea=make_gitea(self.STALE_TIP),
            queue_resume=AsyncMock(),
        )

        assert (new_status, bounced) == ("completed", False)


# =============================================================================
# Create-path plumbing (JobCreate → context)
# =============================================================================


class TestCreatePlumbing:
    def test_jobcreate_accepts_and_normalizes(self):
        """The REST model carries the field; the create path stores the
        normalized manifest into context (both spellings collapse)."""

        body = job_create_module.JobCreate(
            description="ship it",
            required_deliverables=["repo/output/a.md", "./output/a.md", "kb:note"],
        )
        assert body.required_deliverables == ["output/a.md", "kb:note"]
        # The exact merge the endpoint performs:
        assert parse_required_deliverables(body.required_deliverables) == [
            "output/a.md",
            "kb:note",
        ]


# =============================================================================
# Cloned repository datasources (knowledge-base/knowledge/issues/
# deliverable_gate_cannot_see_cloned_repo_deliverables.md)
# =============================================================================


class TestClonedRepoPredicate:
    """``repos/<name>/`` is a working tree the platform refuses to version.

    Three seed sites write ``repos/`` into .gitignore on purpose
    (src/core/workspace.py, src/core/datasource_setup.py,
    src/tools/orchestrator/repositories.py — "working-tree only; never
    versioned", guarding the contentless-gitlink bug b1758f38). A gate that
    reads the versioned tree can therefore never see anything under it.
    """

    def test_a_path_inside_a_cloned_repo_is_recognised(self) -> None:
        assert (
            is_cloned_repo_deliverable("repos/KurortEngine/docs/design/theme.md")
            is True
        )

    def test_the_singular_prefix_is_a_different_thing(self) -> None:
        """One character apart, opposite meanings.

        ``repo/`` is the job's OWN tree and is normalized away by F14;
        ``repos/`` is somebody else's repository, mounted and unversioned.
        """
        assert is_cloned_repo_deliverable("repo/output/x.md") is False
        assert is_cloned_repo_deliverable("output/x.md") is False

    def test_a_bare_directory_is_not_a_deliverable(self) -> None:
        assert is_cloned_repo_deliverable("repos") is False
        assert is_cloned_repo_deliverable("repos/") is False

    def test_kb_entries_are_untouched(self) -> None:
        assert is_cloned_repo_deliverable("kb:some-note-slug") is False

    def test_manifest_collection(self) -> None:
        assert cloned_repo_deliverables(
            ["output/a.md", "repos/K/docs/b.md", "kb:c", "repos/K/e.html"]
        ) == ["repos/K/docs/b.md", "repos/K/e.html"]


class TestGateFailsClosedOnHistoricalClonedRepoPaths:
    @pytest.mark.asyncio
    async def test_unverifiable_external_publication_is_strictly_missing(self):
        job = make_job(manifest=["repos/KurortEngine/docs/design/theme.md"])
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([])
        )
        assert report["missing"] == ["repos/KurortEngine/docs/design/theme.md"]
        assert report["unverified"] == []
        assert report["passed"] is False
        assert report["strict"] is True

    @pytest.mark.asyncio
    async def test_external_path_makes_the_whole_contract_strict(self):
        job = make_job(manifest=["repos/K/docs/a.md", "output/b.md"])
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea([])
        )
        assert report["missing"] == ["repos/K/docs/a.md"]
        assert report["unverified"] == []
        assert report["passed"] is False

    @pytest.mark.asyncio
    async def test_copy_in_job_tree_does_not_prove_external_publication(self):
        job = make_job(manifest=["repos/K/docs/a.md"])
        report = await evaluate_deliverable_gate(
            job, db=make_db(), gitea=make_gitea(["repos/K/docs/a.md"])
        )
        assert report["present"] == []
        assert report["missing"] == ["repos/K/docs/a.md"]
        assert report["passed"] is False


class TestCreationRefusesClonedRepoManifests:
    """Fix half (1): refuse the path where it is cheap, not at seal.

    A deliverable contract is a claim about the job's OWN output. For work
    delivered to an external repository the honest deliverable is the pull
    request, which the orchestrator persists itself. Letting a
    ``repos/...`` entry through means the job runs to completion and only
    then discovers the contract was unsatisfiable.
    """

    @pytest.mark.parametrize(
        "declared",
        [
            "repos/Widget/docs/design/theme.md",
            "./repos/Widget/docs/design/theme.md",
            "/repos/Widget/docs/design/theme.md",
            "  ./repos/Widget/docs/design/theme.md  ",
        ],
    )
    def test_a_cloned_repo_deliverable_is_rejected_after_binding(
        self, declared
    ) -> None:
        with pytest.raises(DeliveryContractConflict) as exc:
            prepare_delivery_contract(
                [declared],
                datasources=[repository_datasource()],
            )
        assert exc.value.code == "external_repository_requires_pr"
        assert exc.value.fields["required_pr_deliverables"] == ["pr:acme/widget"]

    def test_the_refusal_names_the_reason_and_the_alternative(self) -> None:
        """A 422 that does not say WHY just moves the confusion."""
        with pytest.raises(DeliveryContractConflict) as exc:
            prepare_delivery_contract(
                ["repos/Widget/a.md"],
                datasources=[repository_datasource()],
            )
        assert "pull request" in exc.value.message.lower()

    def test_ordinary_and_kb_deliverables_still_pass(self) -> None:
        body = job_create_module.JobCreate(
            description="ship it",
            required_deliverables=["output/a.md", "repo/output/b.md", "kb:note"],
        )
        assert body.required_deliverables == ["output/a.md", "output/b.md", "kb:note"]


class TestHistoricalExternalFailureIsReportedAccurately:
    """A fail-open the operator cannot read is a silent pass.

    The pass-path action line predated cloned-repo entries and called every
    unverified entry a ``kb`` entry. Saying "kb" about a repos/ path tells
    the reader the gate did something it did not do.
    """

    @pytest.mark.asyncio
    async def test_cloned_repo_refusal_is_not_described_as_success(self):
        job = make_job(manifest=["repos/K/docs/a.md"], repo_name="job-aaaaaaaa")
        db = make_db()
        _status, actions, _bounced = await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert _bounced is True
        line = " ".join(actions).lower()
        assert "bounced" in line
        assert "passed" not in line

    @pytest.mark.asyncio
    async def test_the_stamp_names_the_missing_external_paths(self):
        job = make_job(manifest=["repos/K/docs/a.md"], repo_name="job-aaaaaaaa")
        db = make_db()
        await run_deliverable_gate(
            job,
            completion_result(),
            "completed",
            db=db,
            gitea=make_gitea([]),
            queue_resume=AsyncMock(),
        )
        assert stamped(db)["missing"] == ["repos/K/docs/a.md"]
