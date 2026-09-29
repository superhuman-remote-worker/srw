"""Project self-improvement loop — kickoff assembly + job materialization.

Mirrors ``services/automations.py``: translate a ``project_loops`` control row
into a real job via ``db.create_job()``. The caller — the loop router on start,
and the ``_advance_project_loop`` completion hook on each step — is responsible
for repo provisioning and the dispatch nudge, exactly like the automation
run-now path.

Loop jobs run **bare**: the per-job lifecycle hooks (verification critic,
scholar pre-research, curator) are disabled via ``config_override`` so the
loop's explicit Scholar→Critic→Execution rotation IS the cycle and the
auto-hooks don't fight it. State is shared between iterations through the
project knowledge base (a blackboard), not this row.

Design: knowledge-base/knowledge/features/project_self_improvement_loop.md.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

from orchestrator.security.access import project_is_archived

# The legacy name remains the loop path's import surface while the safe
# migration lands. It now inserts a structured PostgreSQL record; it does not
# write a retro file into an agent-visible repository.
from orchestrator.services.job_records import write_loop_retro as write_loop_retro
from orchestrator.services.work_categories import category_block, role_to_category

from orchestrator.services.completion_effect_reconciliation import (
    CompletionEffectProbeError,
    completion_commit_message,
    completion_pr_body,
    completion_pr_title,
    probe_completion_commit,
    probe_completion_pull_request,
)
from orchestrator.services.datasource_policy import default_datasource_selection

logger = logging.getLogger(__name__)


TerminalMergeIntentReader = Callable[[], Awaitable[dict[str, Any] | None]]
TerminalMergeIntentWriter = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]

_S33_TERMINAL_PR_EFFECT = "s33-terminal-merge"
_S33_CURATED_COMMIT_EFFECT = "s33-curated-commit"
_S33_CURATED_AUDIT_PR_EFFECT = "s33-curated-audit-pr"


class TerminalMergeReconciliationError(RuntimeError):
    """A durable PR merge could not be reconciled without guessing."""


class LoopWorkspaceRefused(RuntimeError):
    """The workspace defaults chain refused a loop job (Slice A2b, spec §7).

    Carries only the refusal text, so the spawn paths that stop the loop put
    exactly that message in ``last_error``.
    """


# Every loop role gets an isolated job repository seeded from the project cloud
# folder. On completion, conflict-free changes under ``projects/<slug>/`` are
# applied back to cloud. Execution roles are expected to produce such a diff;
# analysis roles coordinate through the KB, so no file change is normal. The
# execution slot is swappable (developer / default / a future writer), so
# analysis is the closed set and everything else is treated as execution.
# See knowledge-base/knowledge/features/project_jobs_repo_retirement.md.
#
# product-qa audits the SHIPPED product (missing UI, broken setup, integration
# gaps) and files issue candidates as KB notes — it never touches `repo/`, so an
# `empty` merge is normal, not lost work. It coordinates through the KB exactly
# like scholar/critic. Wiring per knowledge-base/knowledge/features/loop_parallel_stages.md (Phase 0).
LOOP_ANALYSIS_ROLES: frozenset[str] = frozenset({"scholar", "critic", "product-qa"})

# Ceiling on how long a born-parked loop member may wait for a model-cooldown
# reset. A pathological upstream reset_at must not park a loop for a year; an
# early wake self-heals (the member re-hits the remaining cooldown → in-job
# pause if ≤12h, else fail-fast → the next member re-parks on a fresh reset).
# knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md
LOOP_COOLDOWN_PARK_CAP_SECONDS: int = 14 * 24 * 3600


def extract_cooldown_reset_at(
    job: dict[str, Any] | None, result: dict[str, Any] | None
) -> float | None:
    """Absolute epoch reset time if this member failed on a model cooldown, else None.

    Prefers the in-flight completion result's error dict; falls back to the
    row's persisted ``error_details`` (heal/resume re-drives call the advance
    with ``result={}``, and asyncpg hands JSONB back as raw JSON strings).
    """
    for cand in ((result or {}).get("error"), (job or {}).get("error_details")):
        if isinstance(cand, str):
            try:
                cand = json.loads(cand)
            except (ValueError, TypeError):
                continue
        if not isinstance(cand, dict) or cand.get("classification") != "cooldown":
            continue
        try:
            return float(cand["reset_at"])
        except (KeyError, TypeError, ValueError):
            continue
    return None


def is_loop_execution_role(role: str | None) -> bool:
    """True if a loop role is expected to change project-cloud files."""
    return bool(role) and role not in LOOP_ANALYSIS_ROLES


def job_loop_id(job: dict[str, Any]) -> str | None:
    """Return the project-loop id a job belongs to, or None if it isn't a loop job.

    Reads ``context.loop_id`` (the stamp ``create_loop_job`` writes), tolerating a
    JSON-string context. This is the same signal ``_advance_project_loop`` keys
    off; exposed so the completion handler can ask "is this a loop job?" before
    applying the loop-specific automatic cloud-delivery policy before its
    advance hook fires.
    """
    ctx = job.get("context") or {}
    if isinstance(ctx, str):
        try:
            ctx = json.loads(ctx)
        except (json.JSONDecodeError, ValueError):
            return None
    loop_id = ctx.get("loop_id") if isinstance(ctx, dict) else None
    return str(loop_id) if loop_id else None


def normalize_stage(entry: Any) -> list[str]:
    """Normalize one ``role_sequence`` entry to its list of concurrent roles.

    The loop grammar (knowledge-base/knowledge/features/loop_parallel_stages.md, Phase 1) lets an
    entry be either a single role name (``"scholar"`` — a one-job stage) or a
    list of role names (``["scholar", "product-qa"]`` — a fan-out stage whose
    jobs run concurrently and barrier before the loop rotates). This collapses
    both to a list so the spawn/advance code has one shape to handle. Raises
    ValueError on a malformed entry so bad grammar fails at the boundary.
    """
    if isinstance(entry, str):
        role = entry.strip()
        if not role:
            raise ValueError("role_sequence entry is an empty string")
        return [role]
    if isinstance(entry, (list, tuple)):
        roles = [r.strip() for r in entry if isinstance(r, str) and r.strip()]
        if not roles:
            raise ValueError(f"role_sequence stage {entry!r} has no valid roles")
        # De-dupe within a stage (a role twice in one fan-out = one job) while
        # preserving order — two jobs of the same role would collide on branch
        # name and race the same KB slot.
        seen: set[str] = set()
        deduped = [r for r in roles if not (r in seen or seen.add(r))]
        return deduped
    raise ValueError(f"role_sequence entry must be a string or list, got {type(entry)}")


def validate_role_sequence(role_sequence: list[Any]) -> None:
    """Validate loop grammar; raise ValueError on the first problem.

    Enforces: non-empty sequence; every entry normalizes to ≥1 role; a fan-out
    (multi-role) stage contains ONLY analysis roles. The last rule keeps the
    single-writer-per-artifact invariant — two execution roles applying to the
    same cloud folder concurrently would race — and matches the design:
    parallel stages are additive *producers* (scholar ∥ product-qa) feeding a
    single downstream consumer, never concurrent executors.
    """
    if not role_sequence:
        raise ValueError("role_sequence must be non-empty")
    for entry in role_sequence:
        roles = normalize_stage(entry)
        if len(roles) > 1:
            execution = [r for r in roles if is_loop_execution_role(r)]
            if execution:
                raise ValueError(
                    "parallel role_sequence stages may contain analysis roles "
                    f"only (got execution role(s) {execution} in stage {roles}); "
                    "a fan-out stage is additive producers feeding one consumer, "
                    "not concurrent executors racing cloud delivery."
                )


# --- Campaign scheduling (knowledge-base/knowledge/features/loop_campaign_scheduling.md, P0) ---
#
# Guardrail defaults for planner-mode loops. Per-loop overrides live in the
# loop row's `campaign_caps` and are validated against the hard ceilings at
# loop start — the ceilings are the non-negotiable runaway floor.
LOOP_CAMPAIGN_DEFAULT_CAPS: dict[str, int] = {
    "max_stages": 5,  # stages one plan may schedule
    "max_extensions": 2,  # times one campaign may be extended
    "abort_failures": 2,  # consecutive member failures before early abort
}
LOOP_CAMPAIGN_CAPS_CEILING: dict[str, int] = {
    "max_stages": 10,
    "max_extensions": 5,
    "abort_failures": 5,
}
# A plan may never spend the loop's whole remaining budget — room must remain
# for the closing analysis stage + critic checkpoint after the campaign.
LOOP_CAMPAIGN_BUDGET_RESERVE = 2
# Bounded archive of disposed campaigns on the loop row (newest last).
LOOP_CAMPAIGN_HISTORY_LIMIT = 20

_PLAN_OUTCOMES = frozenset({"ship", "extend", "kill"})


def resolve_campaign_caps(loop: dict[str, Any]) -> dict[str, int]:
    """Effective campaign guardrails for a loop: defaults + per-loop overrides.

    Overrides come pre-validated from loop start (``validate_campaign_caps``),
    but re-clamp against the ceilings anyway — the caps gate spawning, so a
    hand-edited row must not be able to exceed the runaway floor.
    """
    caps = dict(LOOP_CAMPAIGN_DEFAULT_CAPS)
    overrides = loop.get("campaign_caps") or {}
    if isinstance(overrides, dict):
        for key in caps:
            val = overrides.get(key)
            if isinstance(val, int) and val >= 1:
                caps[key] = min(val, LOOP_CAMPAIGN_CAPS_CEILING[key])
    return caps


def validate_campaign_caps(overrides: dict[str, Any]) -> dict[str, int]:
    """Validate per-loop cap overrides at loop start; raise ValueError.

    Explicit rejection beats silent clamping at the API boundary: a caller who
    asks for max_stages=50 should learn the ceiling, not silently get 10.
    """
    if not isinstance(overrides, dict):
        raise ValueError("campaign_caps must be an object")
    unknown = set(overrides) - set(LOOP_CAMPAIGN_DEFAULT_CAPS)
    if unknown:
        raise ValueError(
            f"unknown campaign_caps key(s) {sorted(unknown)}; "
            f"allowed: {sorted(LOOP_CAMPAIGN_DEFAULT_CAPS)}"
        )
    out: dict[str, int] = {}
    for key, val in overrides.items():
        if not isinstance(val, int) or isinstance(val, bool) or val < 1:
            raise ValueError(f"campaign_caps.{key} must be an integer >= 1")
        ceiling = LOOP_CAMPAIGN_CAPS_CEILING[key]
        if val > ceiling:
            raise ValueError(
                f"campaign_caps.{key}={val} exceeds the hard ceiling {ceiling}"
            )
        out[key] = val
    return out


def next_stage_index(
    *, seq_index_completed: int, stage_count: int, turn_all_failed: bool
) -> tuple[int, bool]:
    """Which stage runs next, and whether the cycle wrapped.

    A turn in which EVERY member failed produced nothing for the next role
    to work from, so it re-runs its own stage instead of advancing. The
    canonical harm is a failed critic: rotation would hand the developer its
    slot anyway, and the developer would build on the PREVIOUS iteration's
    verdict as though it were fresh — the engine deliberately cannot read
    verdicts to notice (they live in KB notes, see ``validate_loop_plan``).

    A partly-failed turn still advances: something landed, and the next role
    has real input.

    This also un-masks the failure. ``consecutive_failures`` resets to 0 on
    any turn that is not wholly failed, so under blind rotation a critic
    could fail on EVERY cycle and never trip ``max_consecutive_failures`` —
    the successful developer that followed it always reset the counter. The
    loop would run its full budget on unjudged work and stop with
    ``stop_reason='budget'``, reporting no failures at all. Re-running the
    failed stage puts consecutive failures back-to-back, so the existing
    stop (evaluated before rotation) actually trips and halts a permanently
    broken stage instead of letting it spin invisibly.

    A retry costs an iteration of budget, which is correct: it is a real
    job that really runs.

    The second element is ``cycle_wrapped``. A retry is never a wrap: the
    KB convergence TTL must not age notes toward re-verification on the
    strength of a cycle that failed to complete.
    """
    if stage_count <= 0:
        # A malformed role_sequence must not take the advance down — the
        # rotate is the only thing keeping a running loop moving.
        return 0, False
    if turn_all_failed:
        return seq_index_completed % stage_count, False
    next_index = (seq_index_completed + 1) % stage_count
    return next_index, next_index == 0


def planner_slots(role_sequence: list[Any]) -> tuple[int, int]:
    """Locate a planner loop's (critic_slot, execution_slot) in the template.

    Planner grammar on top of ``validate_role_sequence``: the critic must
    appear exactly once, as a single-role stage (the checkpoint — a fan-out
    member could never own plan-filing), and the stage after it (cyclically)
    must be single-role — that stage is the execution slot a filed plan
    expands into a campaign. Raises ValueError with the first problem.
    """
    critic_slots = [
        i
        for i, entry in enumerate(role_sequence)
        if normalize_stage(entry) == ["critic"]
    ]
    for i, entry in enumerate(role_sequence):
        roles = normalize_stage(entry)
        if "critic" in roles and len(roles) > 1:
            raise ValueError(
                "campaign scheduling requires the critic to be a single-role "
                f"checkpoint stage, not a fan-out member (stage {i}: {roles})"
            )
    if len(critic_slots) != 1:
        raise ValueError(
            "campaign scheduling requires exactly one 'critic' stage in "
            f"role_sequence (found {len(critic_slots)})"
        )
    if len(role_sequence) < 2:
        raise ValueError(
            "campaign scheduling requires at least one non-critic stage — the "
            "execution slot after the critic checkpoint"
        )
    critic_slot = critic_slots[0]
    execution_slot = (critic_slot + 1) % len(role_sequence)
    if len(normalize_stage(role_sequence[execution_slot])) > 1:
        raise ValueError(
            "campaign scheduling requires the stage after the critic (the "
            "execution slot a plan expands) to be single-role, got fan-out "
            f"stage {role_sequence[execution_slot]!r}"
        )
    return critic_slot, execution_slot


def validate_loop_plan(plan: Any, loop: dict[str, Any]) -> dict[str, Any]:
    """Validate + normalize a Critic-filed campaign plan; raise ValueError.

    Shared by the intake endpoint (so the agent gets actionable feedback while
    it can still fix the plan) and the advance path (re-validated at apply time
    — never trust stored input). Roster laxity matches ``role_sequence``: any
    non-empty role string is accepted, the same contract rotation mode has for
    its entries (unknown roles fall to the default role block downstream).

    Returns the normalized plan:
    ``{initiative: {kb_note_id, title}, stages: [{role}...],
    acceptance: [str...], disposition: {outcome, notes} | None}``.
    """
    if not isinstance(plan, dict):
        raise ValueError("plan must be a JSON object")
    caps = resolve_campaign_caps(loop)

    campaign = loop.get("campaign") or None
    pending_review = bool(campaign) and campaign.get("status") in ("review", "aborted")

    def _normalize_disposition(raw: Any) -> dict[str, Any]:
        if not isinstance(raw, dict):
            raise ValueError("plan.disposition must be an object")
        outcome = str(raw.get("outcome") or "").strip()
        if outcome not in _PLAN_OUTCOMES:
            raise ValueError(
                f"plan.disposition.outcome must be one of {sorted(_PLAN_OUTCOMES)}"
            )
        if not pending_review:
            raise ValueError(
                "plan.disposition given but no campaign is awaiting review — "
                "omit disposition"
            )
        return {
            "outcome": outcome,
            "notes": str(raw.get("notes") or "").strip()[:2000],
        }

    # Dispose-only filing: close the reviewed campaign without opening a new
    # one. Without this shape, a critic whose verdict is ship/kill with no
    # successor campaign has no legal way to deliver it — the observed failure
    # mode is the verdict landing in a KB note the engine cannot read while
    # the campaign stays parked in review.
    if (
        plan.get("disposition") is not None
        and plan.get("initiative") is None
        and not plan.get("stages")
    ):
        disposition = _normalize_disposition(plan.get("disposition"))
        if disposition["outcome"] == "extend":
            raise ValueError(
                "extend requires stages — an extension continues the same "
                "initiative; include plan.initiative (same kb_note_id) and "
                "plan.stages, or dispose with ship|kill"
            )
        return {
            "initiative": None,
            "stages": [],
            "acceptance": [],
            "disposition": disposition,
        }

    initiative = plan.get("initiative")
    if not isinstance(initiative, dict):
        raise ValueError("plan.initiative must be an object with kb_note_id")
    kb_note_id = str(initiative.get("kb_note_id") or "").strip()
    if not kb_note_id:
        raise ValueError("plan.initiative.kb_note_id is required")
    if len(kb_note_id) > 100:
        raise ValueError("plan.initiative.kb_note_id exceeds 100 chars")
    title = str(initiative.get("title") or "").strip()[:300]

    raw_stages = plan.get("stages")
    if not isinstance(raw_stages, list) or not raw_stages:
        raise ValueError("plan.stages must be a non-empty list")
    if len(raw_stages) > caps["max_stages"]:
        raise ValueError(
            f"plan.stages has {len(raw_stages)} stages; this loop's cap is "
            f"{caps['max_stages']}"
        )
    stages: list[dict[str, str]] = []
    for i, entry in enumerate(raw_stages):
        role = entry.get("role") if isinstance(entry, dict) else entry
        role = str(role or "").strip()
        if not role:
            raise ValueError(f"plan.stages[{i}] has no role")
        if len(role) > 100:
            raise ValueError(f"plan.stages[{i}].role exceeds 100 chars")
        stages.append({"role": role})

    remaining = loop.get("remaining_iterations")
    if remaining is not None:
        affordable = int(remaining) - LOOP_CAMPAIGN_BUDGET_RESERVE
        if len(stages) > affordable:
            raise ValueError(
                f"plan costs {len(stages)} iterations but the loop can only "
                f"afford {max(0, affordable)} (remaining {remaining} minus a "
                f"reserve of {LOOP_CAMPAIGN_BUDGET_RESERVE} for the closing "
                "analysis + critic stages) — file a shorter plan"
            )

    raw_acceptance = plan.get("acceptance") or []
    if not isinstance(raw_acceptance, list):
        raise ValueError("plan.acceptance must be a list of strings")
    if len(raw_acceptance) > 10:
        raise ValueError("plan.acceptance is capped at 10 entries")
    acceptance = [str(a).strip()[:2000] for a in raw_acceptance if str(a).strip()]

    raw_disposition = plan.get("disposition")
    disposition: dict[str, Any] | None = None
    if raw_disposition is not None:
        disposition = _normalize_disposition(raw_disposition)
    elif pending_review:
        raise ValueError(
            f"campaign '{campaign.get('title') or campaign.get('id')}' is in "
            f"status {campaign.get('status')} and must be disposed first — "
            "include plan.disposition with outcome ship|extend|kill"
        )

    if disposition and disposition["outcome"] == "extend":
        if int(campaign.get("extensions_used") or 0) >= caps["max_extensions"]:
            raise ValueError(
                "campaign has already used its "
                f"{caps['max_extensions']} extension(s) — outcome must be "
                "ship or kill"
            )
        if kb_note_id != str(campaign.get("initiative_note_id") or ""):
            raise ValueError(
                "extend must continue the same initiative "
                f"({campaign.get('initiative_note_id')!r}); to switch "
                "initiatives, dispose with ship or kill"
            )

    return {
        "initiative": {"kb_note_id": kb_note_id, "title": title},
        "stages": stages,
        "acceptance": acceptance,
        "disposition": disposition,
    }


# Loop roles that get a WORK-CATEGORY contract prepended to their identity block
# (knowledge-base/knowledge/features/officer_backlog_pools.md §7). The contract says what shape the
# deliverable takes and what counts as evidence; the identity block below says
# who this agent is within the loop's rotation.
#
# The critic is deliberately absent, and the omission is the interesting part.
# ``role_to_category("critic")`` is ``tester`` — correct for the *expert*, which
# can work critique tickets — but the loop's critic does something no category
# describes: it SELECTS from the pool. Prepending the tester contract would tell
# it to file 3-7 issue tickets, contradicting its actual duty on the very same
# screen. Categories describe work; selection is orchestration.
_ROLE_CONTRACT_EXEMPT: frozenset[str] = frozenset({"critic"})


def _role_contract_category(role: str | None) -> str | None:
    """The category contract a loop role carries, or None for the selector."""
    if not role or role.strip().lower() in _ROLE_CONTRACT_EXEMPT:
        return None
    return role_to_category(role)


# Role-specific IDENTITY blocks — who this agent is in the rotation and what it
# hands to the next one. Keyed by expert config name; unknown roles fall to
# _ROLE_BLOCK_DEFAULT so the loop stays domain-agnostic (swap `developer` for a
# `writer` / `default` execution role without code changes).
#
# Deliverable shape, evidence class and stopping rules used to live here too and
# now come from the category contract, so these are the loop-specific half only:
# Scholar self-grounds then generates diverse candidates and does NOT
# self-filter; Product-QA is Scholar's inward-looking counterpart feeding the
# same pool; Critic verifies at the GOAL level against the Definition of Done
# and always selects the next improvement (the loop is unconditional — there is
# no goal-met stop); the executor implements what the Critic chose.
_ROLE_BLOCKS: dict[str, str] = {
    "scholar": (
        "FIRST ground yourself: you MUST research the target/competitor system "
        "and the domain with your research tools (what it actually does, what "
        "comparable products offer, what users need) and record concrete, named "
        "findings as durable KB notes so later iterations reuse them instead of "
        "re-researching. Delegate reading to `reader` children with bounded "
        "returns: one question each, a compact evidence-backed answer back; "
        "you synthesize and author every artifact and note. Treat a thin or "
        "empty return as a gap to re-spawn narrower or mark uncovered. THEN propose "
        "several GENUINELY DISTINCT approaches toward "
        "the goal — not variations on one idea — each anchored in the specifics "
        "you found, not generic boilerplate. Check the KB's tried/rejected record "
        "first so you don't re-propose a dead end. Write each candidate to the KB "
        "as a `plan` note tagged `proposal` (a one-line thesis and why it differs "
        "from the others). Do NOT self-filter — selecting is the Critic's job. "
        "Default to foraging "
        "widely rather than waiting to be told what to look at — file what you "
        "find as `idea` notes; that is how the backlog grows."
    ),
    "critic": (
        "You are the OVERSEER: select the next ticket to work, judged AGAINST "
        "THE DEFINITION OF DONE — not your own confidence. Your candidate set "
        "is the PROJECT BACKLOG pool shown above (the `feature`/`issue`/`idea` "
        "tickets) — it is given to you, do not go searching for one. Every "
        "ticket type competes on ONE rubric: choosing a fix over a feature is "
        "a first-class outcome — a shipped module no user can reach, a broken "
        "setup, or a missing product surface can outweigh yet another new "
        "backend slice. Judge every ticket on user-visible value, "
        "product-stability risk of ignoring it, leverage of already-shipped "
        "work, implementation size, and evidence APPROPRIATE TO THE CLAIM — "
        "tests for logic, screenshots for UI, a running deployment for "
        "hosting, citations for research. A ticket is not weaker because its "
        "evidence would be a screenshot rather than a test. Verify any claimed "
        "progress at the "
        "GOAL level, not surface checks (do not approve merely because code "
        "compiles or has no leftover TODOs). Delegate evidence gathering to "
        "`verifier` children — one per independent stream, in one turn. They "
        "return per-criterion evidence with file:line and exact output; they "
        "never approve or reject. The verdict is yours. If "
        "the pool is empty, fall back to self-directed selection from the "
        "PROJECT GOAL — but your pick must always be a real ticket note_id, "
        "so kb_write the action yourself as a `feature`/`issue`/`idea` "
        "ticket FIRST, then select it. "
        "Write a `decision` note tagged "
        "`verdict`: your rationale for the pick and how it will be checked — "
        "this note records WHY you chose it; it is NOT the ticket itself and "
        "never stands in for one (on a campaign-scheduled loop, the "
        "campaign's initiative is the CHOSEN TICKET's own note_id — see "
        "your planner duties below, when this loop runs campaign "
        "scheduling). Leave every non-selected "
        "ticket exactly as it is: do NOT flip it to `superseded` just because "
        "you didn't pick it this turn "
        "— the backlog pool exists so ideas stay queued instead of "
        "evaporating; reserve `superseded` (kb_update, with `superseded_by`) "
        "for a ticket that turns out to be a genuine duplicate of another "
        "one. The loop is "
        "UNCONDITIONAL — it does not stop on 'done'; if the system already meets "
        "the bar in an area, select the next most valuable ticket instead of "
        "declaring completion. Do NOT modify "
        "project files — only read, evaluate, and write your verdict to the KB. "
        "The repository attached to this job is isolated, not shared history. "
        "Use the PROJECT JOB HISTORY in this kickoff for the orchestrator's "
        "mechanical delivery record, and trust it over KB self-reports."
    ),
    "developer": (
        "Implement the Critic's chosen action. You own the code: write the spec "
        "and failing test, then delegate ONE bounded implementation at a time to "
        "`implementer` (never parallel writers), the suite to `tester`, and an "
        "independent diff review to `reviewer`; read the diff yourself before "
        "completion. Write directly when the change is small or cross-cutting. "
        "The current durable project "
        "files are seeded under `projects/<project-slug>/` in this isolated "
        "workspace. When you finish, the "
        "orchestrator applies a conflict-free diff to the project cloud folder; "
        "a conflict pauses the loop for review instead of overwriting newer "
        "cloud state. Job-scoped scratch stays outside that folder. "
        "Record in the KB what you shipped and any follow-ups."
    ),
    "product-qa": (
        "Audit the CURRENT application as a product a real user must operate — "
        "not as a codebase. You are Scholar's counterpart: Scholar looks OUTWARD "
        "for new opportunities; you look INWARD for what is broken, missing, or "
        "unusable in what already exists. Exercise the product: run setup from a "
        "fresh checkout, launch any UI/CLI/API/demo path, check that shipped "
        "modules are reachable in one workflow, look for regressions, setup "
        "failures, integration gaps, and documentation holes. First check the KB "
        "(and the backlog pool above) for "
        "findings already filed — UPDATE them (kb_update), don't re-file "
        "duplicates — and consult the PROJECT JOB HISTORY in this kickoff for "
        "what prior iterations actually delivered. Each `issue` note also "
        "carries acceptance criteria and a priority (high/normal/low — weigh "
        "user-visible value, risk of leaving it unfixed, leverage on "
        "already-shipped work), and is presented as "
        "evidence, not an argument against Scholar. An issue that exists only "
        "in your report is invisible to the next iteration: the `issue` note "
        "IS the handoff, and it is what puts the defect in the project "
        "backlog pool — a repro script or audit transcript is a fine "
        "attachment, never a substitute. Repairing is the Developer's job, new "
        "ideas are Scholar's lane. You and Scholar are peers "
        "feeding the same backlog pool; you do not rank against each "
        "other — the Critic reads the pool and decides what to do next."
    ),
}

_ROLE_BLOCK_DEFAULT = (
    "Advance the goal acting as '{role}'. Build on the KB, validate your work "
    "against the Definition of Done before declaring done, and put durable "
    "project files under `projects/<project-slug>/` so a conflict-free diff can "
    "be applied to the project cloud folder. Record what you did and what the "
    "next agent should do."
)

# Concise verb phrases for the job *description* (the UI title + task_brief
# "## Description"). The full protocol lives in the kickoff message; this is just
# the scannable one-liner so a glance at the jobs list shows role + iteration +
# goal instead of the identical multi-paragraph preamble.
_ROLE_TASKS: dict[str, str] = {
    "scholar": "research the domain & propose distinct improvements",
    "product-qa": "audit the current product & file issue candidates",
    "critic": "select & prioritise the next improvement",
    "developer": "implement the chosen action",
}
_ROLE_TASK_DEFAULT = "advance the goal"


def _goal_snippet(goal: str, limit: int = 80) -> str:
    """First line of the goal, collapsed + truncated for a title."""
    snippet = " ".join(goal.split())  # collapse newlines/runs of whitespace
    return snippet if len(snippet) <= limit else snippet[: limit - 1].rstrip() + "…"


def build_loop_description(loop: dict[str, Any], *, role: str, iteration: int) -> str:
    """Concise job title/task — what the agent accomplishes THIS iteration.

    This becomes ``jobs.description`` (the cockpit row title and the task_brief
    "## Description"). The full loop protocol is delivered separately as the
    kickoff message (``build_loop_kickoff``), so the title stays legible.
    """
    task = _ROLE_TASKS.get(role) or _ROLE_TASK_DEFAULT.format(role=role)
    goal = (loop.get("goal") or "").strip()
    base = f"Loop iter {iteration} · {role.upper()}: {task}"
    return f"{base} — toward: {_goal_snippet(goal)}" if goal else base


def _record_pull_request(record: dict[str, Any]) -> dict[str, Any] | None:
    """The orchestrator-verified pull request a past job delivered, if any.

    Only ``verified: true`` entries count. An agent-declared claim carries
    ``verified: false`` by construction (job_records §5.1), and rendering a
    claim here would let a job talk the loop into believing it delivered.
    """
    changes = record.get("changes")
    if isinstance(changes, str):  # asyncpg hands JSONB back as text
        try:
            changes = json.loads(changes)
        except (json.JSONDecodeError, ValueError):
            return None
    if not isinstance(changes, list):
        return None
    for entry in changes:
        if (
            isinstance(entry, dict)
            and entry.get("kind") == "pull_request"
            and entry.get("verified") is True
        ):
            return entry
    return None


def render_loop_job_history(records: list[dict[str, Any]]) -> str:
    """Compact, orchestrator-owned history block for the next loop kickoff.

    This block is the loop's memory of itself, so a delivery it cannot see
    is a delivery that never happened as far as the next iteration is
    concerned. When code compounds into a source repository every record
    reads ``delivery=no-changes`` — truthfully, because nothing goes to the
    project cloud folder — so the pull request is named alongside it. Left
    unexplained, that field alone testifies that the loop has never shipped.
    """
    lines = ["PROJECT JOB HISTORY (structured database records, newest first):"]
    if not records:
        lines.append("- No prior loop job records.")
        return "\n".join(lines)
    for record in records:
        iteration = record.get("iteration")
        iteration_label = f"iter {iteration}" if iteration is not None else "job"
        role = record.get("role") or "unknown"
        job_id = str(record.get("job_id") or "")[:8]
        status = record.get("status") or "unknown"
        delivery = record.get("delivery_status") or "none"
        line = (
            f"- {iteration_label} · {role} · {job_id}: "
            f"status={status}, delivery={delivery}"
        )
        pull_request = _record_pull_request(record)
        if pull_request is not None:
            detail = " ".join(str(pull_request.get("summary") or "").split())
            ref = str(pull_request.get("ref") or "").strip()
            inner = ", ".join(bit for bit in (detail, ref) if bit)
            line += f" (source repo: {inner})"
        summary = " ".join(str(record.get("completion_notes") or "").split())
        if summary and summary != "(none recorded)":
            line += f" — {summary[:240]}"
        if record.get("error"):
            line += f" — error: {' '.join(str(record['error']).split())[:160]}"
        lines.append(line)
    return "\n".join(lines)


def _format_budget(remaining: int | None, run_until: Any) -> str:
    """One-line budget summary for the kickoff (termination awareness)."""
    bits: list[str] = []
    if remaining is not None:
        bits.append(f"{remaining} iteration(s) left")
    if run_until is not None:
        until = (
            run_until.isoformat() if hasattr(run_until, "isoformat") else str(run_until)
        )
        bits.append(f"runs until {until}")
    return ("Budget: " + " / ".join(bits) + ".") if bits else "Budget: bounded."


def _planner_critic_block(loop: dict[str, Any], budget_line: str) -> str:
    """Planner duties appended to a checkpoint critic's kickoff.

    knowledge-base/knowledge/features/loop_campaign_scheduling.md (P1). Covers: the loop_plan verb
    + budget arithmetic, the disposition duty when a campaign awaits review,
    why campaigns exist (multi-job investments beat horizon-1 selection), and
    the no-fictional-stakeholders rule the Better Resavio audit earned.
    """
    caps = resolve_campaign_caps(loop)
    campaign = loop.get("campaign") or {}
    lines = [
        "PLANNER DUTIES (this loop runs campaign scheduling):",
        "- You are the loop's PLANNER as well as its judge. After selecting "
        "the next ticket, you MAY file a campaign plan with the "
        f"`loop_plan` tool: 1..{caps['max_stages']} execution stages (each "
        "stage = one job = one iteration of budget) toward ONE initiative — "
        "the SELECTED TICKET's own note_id (from the backlog pool above, or "
        "one you kb_write'd yourself if the pool was empty). Your `decision`/"
        "verdict note is a separate rationale record, never the initiative "
        "itself — plus the acceptance evidence the closing critic will check. "
        "If you "
        "file no plan, the loop falls back to a single follow-up job — but "
        "filing nothing NEVER discharges a pending disposition duty.",
        f"- Budget arithmetic: {budget_line} A k-stage campaign costs k "
        "iterations — spend accordingly.",
        "- A multi-job campaign is how high-value work that cannot be proven "
        "in one job (a UI, an integration, a migration) beats yet another "
        "small provable increment: early stages may be honest scaffolding; "
        "the campaign is judged at its END against the acceptance evidence, "
        "not per-job.",
        "- Constraints must be REAL: the only human stakeholder is the "
        "operator. If something seems to need a human (legal review, budget, "
        "third-party access), file a KB note tagged `user-question` and "
        "proceed on the best assumption — NEVER park or defer work on a "
        "trigger no real person will ever pull.",
    ]
    if campaign.get("status") in ("review", "aborted"):
        acceptance = campaign.get("acceptance") or []
        checks = (
            "\n".join(f"    {c}" for c in acceptance)
            if acceptance
            else "    (none pre-registered — judge against the KB verdict "
            "that selected it)"
        )
        lines.insert(
            1,
            f"- DISPOSITION DUTY FIRST: campaign "
            f"'{campaign.get('title') or campaign.get('initiative_note_id')}' "
            f"({campaign.get('stages_done', '?')} of "
            f"{len(campaign.get('stages') or [])} stages, status "
            f"{campaign.get('status')}) awaits your verdict. Run its "
            f"pre-registered acceptance checks:\n{checks}\n  Then dispose via "
            "loop_plan's disposition fields: ship (evidence passes), extend "
            "(alive but unfinished — your stages continue the SAME "
            "initiative), or kill (dead end — record why in the KB). A plan "
            "without a disposition will be rejected while this campaign is "
            "pending. If you have no new campaign to open, file ONLY the "
            'disposition: `{"disposition": {"outcome": "ship|kill", "notes": '
            '"..."}}` — a KB note is NOT a disposition, and filing nothing '
            "leaves this campaign parked.",
        )
    return "\n".join(lines)


def _campaign_member_block(campaign: dict[str, Any], stage_index: int) -> str:
    """Campaign context appended to a campaign member's kickoff.

    The verification-repricing contract: mid-campaign scaffolding is licensed,
    dishonest retros are not — the campaign is judged at its end against the
    pre-registered evidence, so truthfulness (not per-job provability) is what
    keeps the loop's shared reality intact.
    """
    stages = campaign.get("stages") or []
    acceptance = campaign.get("acceptance") or []
    checks = (
        "\n".join(f"    {c}" for c in acceptance)
        if acceptance
        else "    (none pre-registered)"
    )
    return (
        f"CAMPAIGN CONTEXT: you are stage {stage_index + 1} of {len(stages)} "
        f"of campaign '{campaign.get('title') or campaign.get('initiative_note_id')}' "
        f"(initiative KB note: {campaign.get('initiative_note_id')}).\n"
        "- Prior stages' actual state: use the current project-cloud files, "
        "PROJECT JOB HISTORY, and KB — build on what was delivered, don't "
        "restart.\n"
        "- The campaign is judged at its END against this pre-registered "
        f"acceptance evidence:\n{checks}\n"
        "- You do NOT have to reach a fully provable state THIS job — "
        "mid-campaign scaffolding is fine — but your completion report MUST be honest "
        "about actual state: never claim working what isn't. The closing "
        "critic runs the checks above; honesty is the contract that keeps "
        "the loop sane."
    )


def build_loop_kickoff(
    loop: dict[str, Any],
    *,
    role: str,
    iteration: int,
    extra_context: dict[str, Any] | None = None,
    backlog_block: str | None = None,
    history_block: str | None = None,
) -> str:
    """Assemble the loop-aware kickoff *message* for one job.

    Delivered via ``context["kickoff_message"]`` (the "Opening Message" channel),
    NOT as the job description — so the full protocol reaches the agent's
    task_brief "## Kickoff Message" while the cockpit row title stays the concise
    ``build_loop_description`` line.

    Part system-preamble, part project goal + Definition of Done, part
    role-specific task, part optional user steering. Research-tuned: anchors
    "done" to external acceptance criteria, makes the agent restate the goal,
    states the remaining budget (termination awareness), and tells it to read
    the tried/rejected record before proposing (repetition guard).

    Planner-scheduled loops add a block: the checkpoint critic gets its
    planner duties (loop_plan / disposition / budget arithmetic), a campaign
    member gets its campaign context (stage N of M + the honesty contract).
    ``extra_context`` is the spawn-time context stamp dict — a present
    ``loop_campaign_id`` marks a campaign member.

    ``backlog_block`` is the pre-rendered work pool (see
    ``services/project_backlog.render_backlog_block``). It is injected
    VERBATIM: this function stays pure and does no I/O, so the caller fetches
    it. Passing None (start-up paths, tests) simply omits the section.
    ``history_block`` follows the same pattern for structured job records.
    """
    goal = (loop.get("goal") or "").strip() or (
        "(no explicit goal set — make useful, self-directed progress and record "
        "in the KB what you chose to pursue and why)"
    )
    criteria = (loop.get("acceptance_criteria") or "").strip() or (
        "(no explicit acceptance criteria — infer reasonable ones from the goal "
        "and record them in the KB as a `goal` note tagged `definition_of_done` "
        "for later iterations to steer by)"
    )
    budget_line = _format_budget(
        loop.get("remaining_iterations"), loop.get("run_until")
    )
    # Category contract first, loop identity second (§7). The contract answers
    # "what shape is the deliverable and what counts as evidence"; the identity
    # answers "who are you in this rotation". Composing in that order means a
    # loop with no officer still gets the doctrine the pools were built to
    # carry — an answer is a deliverable, a screenshot is evidence, tests are
    # regression rails and never the score.
    identity_block = _ROLE_BLOCKS.get(role) or _ROLE_BLOCK_DEFAULT.format(role=role)
    contract_category = _role_contract_category(role)
    role_block = (
        f"{category_block(contract_category)}\n\n{identity_block}"
        if contract_category
        else identity_block
    )
    user_prompt = (loop.get("user_prompt") or "").strip()

    parts = [
        "You are ONE step in a CONTINUOUS, UNATTENDED improvement loop on this "
        "project. Other agents run before and after you. Coordinate through the "
        "project knowledge base and current project-cloud files; this job's "
        "isolated repository is not shared history. READ THE KB FIRST; WRITE "
        "BACK what matters before you finish.",
        f"PROJECT GOAL:\n{goal}",
        "DEFINITION OF DONE — the quality bar you STEER TOWARD (the loop keeps "
        f"improving past it; it does not stop when it's 'met'):\n{criteria}",
        f"LOOP STATUS: iteration {iteration}. {budget_line} Do NOT try to finish "
        "the whole goal in one job — make ONE solid increment with EVIDENCE "
        "APPROPRIATE TO THE WORK (tests for logic, screenshots for UI, a "
        "running deployment for hosting, citations for research) and hand off "
        "through the KB. Do not pick the work whose evidence is easiest to "
        "produce; pick the work that moves the goal.",
    ]

    # The work pool, handed over rather than searched for. Placed before the
    # role block so selection duty reads it in order.
    if backlog_block:
        parts.append(backlog_block)
        backlog_note = (
            "The open backlog is listed above — it is given to you, do not go "
            "searching for it."
        )
    else:
        # Fix round 1, Finding 1: backlog_block=None in production means the
        # fetch failed (a KB outage) -- project_id is NOT NULL and vector_db is
        # assigned unconditionally, so this is the only realistic route. Saying
        # "listed above" here would assert a block that isn't there AND forbid
        # the agent from doing anything about the gap -- worse than the old
        # fiction it replaced. Tell the truth instead.
        backlog_note = (
            "The project backlog could not be read this turn (KB unavailable) "
            "— proceed without it rather than hunting for a substitute; note "
            "the gap in your write-back."
        )

    if history_block:
        parts.append(history_block)

    parts += [
        "BEFORE you act: restate the goal in one line, then check the KB for "
        "(a) what's already done and (b) what's been TRIED AND REJECTED (do "
        f"not re-propose it). {backlog_note}",
        f"YOUR ROLE THIS ITERATION — {role.upper()}:\n{role_block}",
        "WHEN DONE: write to the KB what you did, what you learned, and what the "
        "next agent should do. If you closed or abandoned an approach, record it "
        "as tried/rejected so nobody repeats it. File any new work you spotted "
        "but did not do as a `feature`, `issue` or `idea` note (kb_write) — that "
        "is the project backlog, and it is the only place future iterations will "
        "look for it.",
    ]

    # Campaign-scheduled loops (knowledge-base/knowledge/features/loop_campaign_scheduling.md):
    # campaign context for members, planner duties for the checkpoint critic.
    if (loop.get("scheduling") or "standard") == "campaign":
        stamps = extra_context or {}
        if stamps.get("loop_campaign_id") is not None:
            campaign = loop.get("campaign") or {}
            try:
                stage_index = int(stamps.get("loop_campaign_index") or 0)
            except (TypeError, ValueError):
                stage_index = 0
            parts.append(_campaign_member_block(campaign, stage_index))
        elif role == "critic":
            parts.append(_planner_critic_block(loop, budget_line))

    if user_prompt:
        parts.append(f"ADDITIONAL STEERING FROM THE USER:\n{user_prompt}")
    return "\n\n".join(parts)


async def create_loop_job(
    db: Any,
    loop: dict[str, Any],
    *,
    role: str,
    iteration: int,
    seq_index: int | None = None,
    remaining_iterations: int | None = None,
    disable_memory_assembler: bool = False,
    extra_context: dict[str, Any] | None = None,
    backlog_block: str | None = None,
    history_block: str | None = None,
    park_until: datetime | None = None,
) -> dict[str, Any] | None:
    """Materialize ONE bare loop job for the given role + iteration.

    Returns ``None`` when the loop's project is archived — see the archived
    check below.

    DB-only (job row + materialized datasource links), mirroring
    ``create_job_from_automation``. The caller provisions the Gitea repo and
    nudges the dispatcher afterwards.

    Stamps ``context.loop_id`` (the join key for ``list_project_loop_jobs`` and
    the ``_advance_project_loop`` hook) plus the role + iteration. Connector
    defaults are resolved live for the loop owner and project at each spawn,
    then atomically materialized with the job. Project-linked connectors that
    are not automatic remain available for manual work but are not attached to
    unattended loop iterations.

    When ``seq_index`` is given (always, when spawned through a stage), the
    stage index and post-advance ``remaining_iterations`` are ALSO stamped into
    context (``loop_seq_index`` / ``loop_remaining``). These are spawn-time
    truth the torn-advance sweeper reads back directly to reconstruct the loop's
    counters — robust to variable-width parallel stages, where the old
    ``(iteration-1) % len(roles)`` modulo no longer maps job-count to stage.

    ``disable_memory_assembler`` turns off the RecallStore TTL-curation
    assembler for this job (set by the caller for members of a *fan-out* stage).
    The assembler is the one memory writer that does a read-modify-write over the
    project-scoped shared store (it retires/re-TTLs existing memories); running N
    of them concurrently over one project would race that curation slot. It is
    disabled via the existing ``auxiliary.tasks.assemble_memories.enabled`` flag
    (the same lever ``session_base.yaml`` uses) rather than by editing the
    ``memory.pipeline.writers`` list — a scalar deep-merge that leaves the
    append-only extractors, the KB curator, and the writers list untouched. See
    knowledge-base/knowledge/features/loop_parallel_stages.md and [[project_kb_convergence_f13]].

    ``backlog_block`` is threaded straight through to ``build_loop_kickoff`` —
    this function does no fetching itself, it just carries the caller's
    pre-rendered pool string (or None) into the kickoff message.
    """
    loop_id = str(loop["id"])
    project_id = str(loop["project_id"]) if loop.get("project_id") else None

    # An archived project takes no new work (§4.3 of
    # knowledge-base/knowledge/features/project_and_job_list_filtering.md).
    # This is a pure service path with no HTTP caller, so it skips and logs
    # rather than raising. It is a backstop, not the primary control: archiving
    # pauses the loop (§4.5) and the loop start/resume/scheduling endpoints
    # already 409 at the guard, so reaching here means the archive landed
    # mid-advance.
    if project_id:
        project = await db.get_project(project_id)
        if project_is_archived(project):
            logger.warning(
                "loop %s: project %s is archived — not materializing the %s "
                "job for iteration %s",
                loop_id[:8],
                project_id,
                role,
                iteration,
            )
            return None

    # Bare config: the loop is the orchestration, so disable the per-job
    # lifecycle hooks that would otherwise fight it — a verification critic that
    # resumes the job, and a scholar pre-research that doubles the scholar role.
    # Curation is the exception: it is the inline KB extractor/assembler aux pass
    # (not a competing job rotation), and it is what makes the loop's knowledge
    # compound and converge across cycles, so the loop turns it ON.
    # See knowledge-base/knowledge/features/kb_convergence_ttl_reverification.md.
    config_override: dict[str, Any] = {
        "verification": {"enabled": False},
        "scholar": {"enabled": False},
        "curator": {"enabled": True},
        "autonomy": "full",
        # Loop reasoning coordinates through the project knowledge base + shared
        # memory, while durable files hand off through project cloud delivery.
        # A step that loses its embedding-backed stores must pause for
        # re-dispatch rather than run blind (see
        # knowledge-history/done/embedding_key_missing_silently_disables_memory_and_kb.md).
        "memory": {"required": True},
    }
    model = loop.get("model")
    if model:
        config_override["llm"] = {"model": model}

    # Per-loop workspace tier override (mirrors `model`). When set, every job
    # the loop spawns boots on this backend — e.g. `vm` gives every role a root
    # VM instead of the default sandbox container. The dispatcher reads
    # config_override.workspace.backend (main._job_needs_vm / _job_needs_sandbox)
    # to decide provisioning; VM sizing falls back to its 8c/16Gi default.
    workspace_backend = loop.get("workspace_backend")
    if workspace_backend:
        config_override["workspace"] = {"backend": workspace_backend}

    # Fan-out members: silence the TTL-curation assembler so N concurrent
    # analysis roles don't race the shared project RecallStore's read-modify-
    # write curation slot (see docstring). Scalar override on the existing aux
    # gate — no list surgery, extractors/curator keep running.
    if disable_memory_assembler:
        config_override["auxiliary"] = {
            "tasks": {"assemble_memories": {"enabled": False}}
        }

    # Split the synthesized prompt the way the manual create-job form does:
    # a concise `description` (the cockpit row title + task_brief "## Description")
    # and the full loop protocol as the kickoff message, carried through the
    # "Opening Message" channel (context["kickoff_message"]). Both land together
    # in the agent's task_brief.md; only the description shows as the job title.
    # Campaign loops: the checkpoint critic gets the campaign-filing tool via
    # an additive category override (tools.loop doesn't exist in any bundled
    # config, so the deep-merge adds it without touching the expert's other
    # tool lists). Campaign members — even critic-flavored sub-critics — and
    # standard loops never get it; the intake endpoint re-gates server-side
    # (defense in depth, mirroring phase-restricted tools).
    is_campaign_member = bool((extra_context or {}).get("loop_campaign_id"))
    if (
        (loop.get("scheduling") or "standard") == "campaign"
        and role == "critic"
        and not is_campaign_member
    ):
        config_override["tools"] = {"loop": ["loop_plan"]}

    description = build_loop_description(loop, role=role, iteration=iteration)
    kickoff = build_loop_kickoff(
        loop,
        role=role,
        iteration=iteration,
        extra_context=extra_context,
        backlog_block=backlog_block,
        history_block=history_block,
    )
    context = {
        "loop_id": loop_id,
        "loop_role": role,
        "loop_iteration": iteration,
        "kickoff_message": kickoff,
        # Blocks dispatch from the instant the row exists. Loop provisioning
        # replaces this with ready only after the project-cloud baseline has
        # been seeded synchronously.
        "cloud_baseline": {"state": "seeding"},
    }
    # Spawn-time counter stamps for the torn-advance heal (see docstring). Gated
    # on seq_index so legacy single-job callers stay stamp-free and fall through
    # to the sweeper's modulo derivation; loop_remaining is stamped alongside
    # (None is authoritative — a deadline-only loop genuinely has no budget).
    if seq_index is not None:
        context["loop_seq_index"] = int(seq_index)
        context["loop_remaining"] = remaining_iterations

    # Born parked (knowledge-base/knowledge/issues/loop_advances_into_active_model_cooldown.md): the
    # previous turn failed on a model cooldown that outlives the pause budget, so
    # this member is created paused-with-freeze instead of dispatched —
    # freeze_data non-NULL hides it from the dispatcher and the existing
    # llm_outage sweeper wakes it at next_retry_at. context.llm_outage carries NO
    # first_failed_at: evaluate_llm_outage would read creation time as a
    # multi-day elapsed outage and ceiling-fail the member AT WAKE; absent, it
    # defaults to wake-time now → elapsed≈0 → survives (completion.py).
    status = "created"
    freeze_data: dict[str, Any] | None = None
    if park_until is not None:
        park_iso = park_until.isoformat()
        status = "paused"
        freeze_data = {
            "freeze_type": "llm_unavailable",
            "classification": "cooldown",
            "next_retry_at": park_iso,
            "attempt": 0,
            "model": loop.get("model"),
            "origin": "loop_cooldown_park",
            "error_summary": (
                f"Created parked: model '{loop.get('model') or 'pinned model'}' "
                f"in quota cooldown until {park_iso} (inherited from the "
                f"previous loop turn)"
            )[:500],
        }
        context["llm_outage"] = {"attempt": 0, "next_retry_at": park_iso}

    # Campaign member stamps (loop_campaign_id / loop_campaign_index) and any
    # other spawn-time truth the caller needs read back by the advance/heal.
    # Reserved keys above win — extra context can never shadow the loop join
    # key, the counter stamps, or the born-parked llm_outage state.
    if extra_context:
        for key, value in extra_context.items():
            context.setdefault(key, value)

    # Resolve the role NAME to a DB expert_id when DB-backed experts are on, so a
    # custom expert in the rotation pulls its OWN overlay (model, prompts, tools)
    # rather than just a bundled disk config. Mirrors the automations
    # name-resolution path (services/automations.py); falls through to the
    # bundled config_name when nothing matches or the flag is off. A DB winner
    # resolves on worker_base; keeping the role slug in config_name as well would
    # accidentally merge the bundled role and the DB expert into one profile.
    expert_id: str | None = None
    config_name = role
    owner_id = str(loop["owner_id"]) if loop.get("owner_id") else None
    if os.getenv("EXPERTS_DB_ENABLED", "true").lower().strip() in (
        "true",
        "1",
        "yes",
    ):
        from shared.runtime.core.expert_resolution import pick_expert_by_name

        pids = [project_id] if project_id else []
        try:
            candidates = await db.list_experts_visible(
                user_id=owner_id, project_ids=pids
            )
            matches = [c for c in candidates if c.get("name") == role]
            winner = pick_expert_by_name(matches, owner_id, set(pids))
            if winner:
                expert_id = str(winner["id"])
                config_name = "worker_base"
        except Exception as e:
            logger.warning(
                "loop %s: expert resolution for role %s failed: %s", loop_id, role, e
            )

    target_project_ids = [project_id] if project_id else []

    from orchestrator.services.manifest_workspace_selection import (
        select_project_workspace_default,
    )

    from fastapi import HTTPException

    try:
        (
            config_override,
            workspace_selection,
        ) = await select_project_workspace_default(
            db,
            owner_id,
            project_id,
            config_override,
        )
    except HTTPException as exc:
        raise LoopWorkspaceRefused(str(exc.detail)) from exc
    if workspace_selection is not None:
        workspace_backend = config_override["workspace"]["backend"]

    # Resolve immediately before materialization so every loop iteration uses
    # the owner's current policy. The policy service applies scope/membership
    # checks and silently filters repository defaults for lite tiers. A revoked
    # owner or project membership raises before the job exists, preventing an
    # unattended loop from continuing with a partial credential contract.
    if owner_id:
        (
            datasource_ids,
            datasource_policy_revisions,
        ) = await default_datasource_selection(
            db,
            owner_id,
            target_project_ids,
            str(workspace_backend) if workspace_backend else None,
        )
        datasource_origin = "default"
    else:
        # Historical/userless internal rows have no authoritative principal
        # from whom ambient preferences may be borrowed. Their complete,
        # explicit selection is therefore empty.
        datasource_ids = []
        datasource_policy_revisions = {}
        datasource_origin = "explicit"

    job = await db.create_job(
        **(
            {"workspace_selection": workspace_selection}
            if workspace_selection is not None
            else {}
        ),
        origin="loop",
        description=description,
        config_name=config_name,
        config_override=config_override,
        context=context,
        user_id=owner_id,
        project_id=project_id,
        priority=5,
        expert_id=expert_id,
        status=status,
        freeze_data=freeze_data,
        datasource_ids=datasource_ids,
        datasource_selection_provenance={
            "origin": datasource_origin,
            "creation_path": "project_loop",
            "effective_work_owner_id": owner_id,
            "target_project_ids": target_project_ids,
            "datasource_ids": datasource_ids,
            "policy_revisions": datasource_policy_revisions,
            "resolved_at": datetime.now(timezone.utc).isoformat(),
        },
        datasource_policy_revisions=datasource_policy_revisions,
        authority_user_id=owner_id,
        authority_project_ids=(target_project_ids if owner_id else None),
    )
    job_id = str(job["id"])

    logger.info(
        "Project loop %s → %s job %s (iteration %s, datasource_defaults=%s)",
        loop_id,
        role,
        job_id,
        iteration,
        len(datasource_ids),
    )
    return job


async def merge_loop_job_branch(
    gitea_client: Any,
    job: dict[str, Any],
    *,
    load_merge_intent: TerminalMergeIntentReader | None = None,
    store_merge_intent: TerminalMergeIntentWriter | None = None,
    completion_command_id: str | None = None,
) -> tuple[str, str | None]:
    """Squash-merge a completed loop job's branch into ``main``.

    One clean commit per job lands on ``main``; the ``job/<id>`` branch is
    KEPT as the per-iteration audit log. The returned status is literal —
    the loop's artifact-integrity signal (replaces the v1 SHA-compare no-op
    guard, which inferred what this step *knows*):

    - ``merged``       — squash commit created; second element is its SHA.
    - ``empty``        — branch has no commits vs ``main``: the job landed
                         nothing (F29-family signal for execution roles;
                         expected for analysis roles while the KB is a DB).
    - ``merge-failed`` — compare/PR/merge errored. Sequential loops are
                         conflict-free by construction, so this is loud-flag
                         territory, never silent.
    - ``skipped``      — nothing to merge structurally: no repo, or a v1-era
                         job that worked directly on ``main`` (its push
                         already landed) completing after the v2 deploy.

    Design: knowledge-base/knowledge/features/loop_repo_compounding_v2.md.
    """
    repo_name = job.get("repo_name")
    branch = job.get("branch_name")
    if not repo_name or not branch or branch == "main":
        return "skipped", None

    durable = (
        load_merge_intent is not None
        or store_merge_intent is not None
        or completion_command_id is not None
    )
    if (load_merge_intent is None) != (store_merge_intent is None):
        raise ValueError(
            "durable terminal merge requires both intent reader and writer"
        )
    if durable and (
        load_merge_intent is None
        or store_merge_intent is None
        or completion_command_id is None
    ):
        raise ValueError(
            "durable terminal merge requires intent callbacks and command id"
        )

    async def _finish_intent(intent: dict[str, Any]) -> tuple[str, str | None]:
        pr_index = intent.get("pr_index")
        valid = (
            intent.get("kind") == "gitea_pr_merge"
            and intent.get("repo_name") == repo_name
            and intent.get("head") == branch
            and intent.get("base") == "main"
            and isinstance(pr_index, int)
            and not isinstance(pr_index, bool)
            and pr_index > 0
        )
        if not valid:
            raise TerminalMergeReconciliationError(
                "persisted terminal-merge intent does not match this job"
            )

        merged = await gitea_client.probe_pr_merged(repo_name, pr_index)
        if merged is None:
            raise TerminalMergeReconciliationError(
                f"merge state for PR #{pr_index} is ambiguous"
            )
        if not merged:
            merged = await gitea_client.merge_pr(
                repo_name,
                pr_index,
                merge_strategy="squash",
                delete_branch_after_merge=False,
            )
            if not merged:
                raise TerminalMergeReconciliationError(
                    f"merge of persisted PR #{pr_index} was refused"
                )
        sha = await gitea_client.get_branch_head_sha(repo_name, "main")
        return "merged", sha

    # Reconciliation MUST precede the compare.  After a successful squash or
    # rebase merge the branch comparison can look empty or otherwise cannot
    # identify the original commit.  The persisted PR number is the exact key.
    if durable:
        assert load_merge_intent is not None
        prior_intent = await load_merge_intent()
        if prior_intent is not None:
            return await _finish_intent(prior_intent)

        # ``create_pr`` is external and the response can be lost.  Search all
        # PR states for the exact command marker before the compare or another
        # create.  This is legal only in the durable finalizer arm, whose
        # complete_by/lease discipline proves the predecessor is gone.
        assert completion_command_id is not None
        try:
            prior_pr = await probe_completion_pull_request(
                gitea_client,
                repo_name=repo_name,
                head=branch,
                base="main",
                command_id=completion_command_id,
                effect_kind=_S33_TERMINAL_PR_EFFECT,
            )
        except CompletionEffectProbeError as exc:
            raise TerminalMergeReconciliationError(str(exc)) from exc
        if prior_pr is not None:
            recovered_intent = {
                "kind": "gitea_pr_merge",
                "repo_name": repo_name,
                "head": branch,
                "base": "main",
                "pr_index": prior_pr.pr_index,
            }
            assert store_merge_intent is not None
            persisted = await store_merge_intent(recovered_intent)
            if persisted != recovered_intent:
                raise TerminalMergeReconciliationError(
                    "persisted terminal-merge intent changed identity"
                )
            return await _finish_intent(recovered_intent)

    compare = await gitea_client.get_compare(repo_name, "main", branch)
    if compare is None:
        if durable:
            raise TerminalMergeReconciliationError(
                "branch comparison failed before terminal PR creation"
            )
        return "merge-failed", None
    if not compare.get("total_commits"):
        return "empty", None

    job_id = str(job.get("id"))
    title = (job.get("description") or "").splitlines()[0].strip()[:200] or (
        f"Loop job {job_id[:8]}"
    )
    body = f"job: {job_id}\nbranch: {branch}"
    if durable:
        assert completion_command_id is not None
        title = completion_pr_title(
            title,
            command_id=completion_command_id,
            effect_kind=_S33_TERMINAL_PR_EFFECT,
        )
        body = completion_pr_body(
            body,
            command_id=completion_command_id,
            effect_kind=_S33_TERMINAL_PR_EFFECT,
        )
    pr = await gitea_client.create_pr(
        repo_name,
        title=title,
        head=branch,
        base="main",
        body=body,
    )
    if not pr:
        if durable:
            raise TerminalMergeReconciliationError(
                "terminal pull request could not be created"
            )
        return "merge-failed", None

    if durable:
        pr_index = pr.get("number")
        if not isinstance(pr_index, int) or isinstance(pr_index, bool) or pr_index <= 0:
            raise TerminalMergeReconciliationError(
                "terminal pull request has no usable numeric index"
            )
        intent = {
            "kind": "gitea_pr_merge",
            "repo_name": repo_name,
            "head": branch,
            "base": "main",
            "pr_index": pr_index,
        }
        assert store_merge_intent is not None
        persisted = await store_merge_intent(intent)
        if persisted != intent:
            raise TerminalMergeReconciliationError(
                "persisted terminal-merge intent changed identity"
            )
    merged = await gitea_client.merge_pr(
        repo_name,
        pr["number"],
        merge_strategy="squash",
        # The branch is the audit log — never deleted on merge.
        delete_branch_after_merge=False,
    )
    if not merged:
        if durable:
            raise TerminalMergeReconciliationError(
                f"merge of persisted PR #{pr['number']} was refused"
            )
        return "merge-failed", None
    sha = await gitea_client.get_branch_head_sha(repo_name, "main")
    return "merged", sha


def contracted_file_deliverables(job: dict[str, Any]) -> list[str]:
    """The job's FILE deliverables (canonical, ``kb:`` entries dropped).

    Same manifest source and normalization as the seal-side gate
    (``services.deliverable_gate``): ``context.required_deliverables``,
    ``repo/`` prefix and ``./`` stripped. ``kb:`` entries are store-backed,
    never files — a contract of only those curates nothing.

    Public because it is the predicate :func:`merge_loop_job_contribution`
    dispatches on (curated vs full merge), and §6.6's terminal-transition
    gate MUST ask the same question before calling in — see
    ``services.completion.job_has_file_contract``.
    """
    from orchestrator.services.deliverable_gate import (
        KB_DELIVERABLE_PREFIX,
        parse_required_deliverables,
    )

    manifest = parse_required_deliverables(job.get("context"))
    return [p for p in manifest if not p.startswith(KB_DELIVERABLE_PREFIX)]


async def merge_loop_job_contribution(
    gitea_client: Any,
    job: dict[str, Any],
    *,
    ctx: dict[str, Any] | None = None,
    load_merge_intent: TerminalMergeIntentReader | None = None,
    store_merge_intent: TerminalMergeIntentWriter | None = None,
    completion_command_id: str | None = None,
) -> tuple[str, str | None, list[str]]:
    """Land a completed loop job's contribution on ``main`` — curated when
    the job carries a file-deliverable contract, full squash-merge otherwise.

    The §6.4 change (knowledge-base/knowledge/features/workspace_and_change_records.md): a job
    with ``required_deliverables`` naming at least one FILE gets a **curated
    merge** — the contracted files are copied from the branch head onto
    ``main`` as ONE commit and *nothing else merges*. The branch becomes a
    genuine scratchpad: stray ``.venv``s and scratch files stop accumulating
    into permanent shared history. The audit PR is still created, then
    closed UNMERGED with a comment naming the retro record and the curated
    commit. Jobs with no contract (or a ``kb:``-only one) take the exact
    ``merge_loop_job_branch`` path — behaviour byte-identical to today.

    Returns ``(merge_status, merged_sha, merge_notes)``. ``merge_status``
    adds ``curated`` to the existing vocabulary (``merged`` / ``empty`` /
    ``merge-failed`` / ``skipped``); for ``curated`` the SHA is the curated
    commit on ``main``. ``merge_notes`` are orchestrator observations for
    the retro's ``## Merge notes`` section: what was curated (and at which
    branch spelling), contracted paths missing from the branch, and any
    fallback warning.

    Fallback rules — never lose work to a curation bug:

    * Mechanical failure mid-curation (unreadable tree/blob, refused commit,
      any unexpected error) → today's full squash-merge, with a warning
      note. Only reachable BEFORE the curated commit exists; afterwards a
      full merge would re-land the whole branch on top of it.
    * NONE of the contracted files on the branch → full merge + warning (an
      empty curation would silently discard whatever work exists; policing
      missing deliverables is the deliverable gate's job, not the merge's).
    * Some missing → curate what exists, list the missing paths.
    """
    files_contract = contracted_file_deliverables(job)
    durable = (
        load_merge_intent is not None
        or store_merge_intent is not None
        or completion_command_id is not None
    )
    if (load_merge_intent is None) != (store_merge_intent is None):
        raise ValueError(
            "durable terminal merge requires both intent reader and writer"
        )
    if durable and (
        load_merge_intent is None
        or store_merge_intent is None
        or completion_command_id is None
    ):
        raise ValueError(
            "durable terminal merge requires intent callbacks and command id"
        )
    if not files_contract:
        status, sha = await merge_loop_job_branch(
            gitea_client,
            job,
            load_merge_intent=load_merge_intent,
            store_merge_intent=store_merge_intent,
            completion_command_id=completion_command_id,
        )
        return status, sha, []

    # A prior curation attempt may already have entered its full-merge
    # fallback and captured a PR.  That exact PR now owns the effect: resume
    # it before reconsidering curation, otherwise a repaired read could land a
    # curated commit while the already-created PR remains unresolved.
    if load_merge_intent is not None and await load_merge_intent() is not None:
        status, sha = await merge_loop_job_branch(
            gitea_client,
            job,
            load_merge_intent=load_merge_intent,
            store_merge_intent=store_merge_intent,
            completion_command_id=completion_command_id,
        )
        return (
            status,
            sha,
            ["resumed persisted full-merge fallback pull request"],
        )

    # Structural guards — same vocabulary and same order as the full merge.
    repo_name = job.get("repo_name")
    branch = job.get("branch_name")
    if not repo_name or not branch or branch == "main":
        return "skipped", None, []
    compare = await gitea_client.get_compare(repo_name, "main", branch)
    if compare is None:
        if load_merge_intent is not None:
            raise TerminalMergeReconciliationError(
                "branch comparison failed before terminal curation"
            )
        return "merge-failed", None, []
    if not compare.get("total_commits"):
        return "empty", None, []

    from orchestrator.services.job_records import loop_retro_path, loop_role_iteration

    job_id = str(job.get("id"))
    role, _ = loop_role_iteration(job, ctx or {})

    async def _fallback(reason: str) -> tuple[str, str | None, list[str]]:
        logger.warning(
            "curated merge for job %s fell back to full squash-merge: %s",
            job_id[:8],
            reason,
        )
        try:
            status, sha = await merge_loop_job_branch(
                gitea_client,
                job,
                load_merge_intent=load_merge_intent,
                store_merge_intent=store_merge_intent,
                completion_command_id=completion_command_id,
            )
        except Exception as exc:
            if load_merge_intent is not None:
                raise TerminalMergeReconciliationError(
                    f"durable full-merge fallback did not complete: {exc}"
                ) from exc
            raise
        return (
            status,
            sha,
            [f"curated merge FELL BACK to full squash-merge: {reason}"],
        )

    # ------------------------------------------------------------------
    # Curation phase — every failure here falls back to the full merge.
    # ------------------------------------------------------------------
    try:
        branch_tree = await gitea_client.list_tree(repo_name, branch)
        if branch_tree is None:
            return await _fallback(f"branch tree unreadable at {repo_name}@{branch}")
        branch_paths = {
            str(entry.get("path"))
            for entry in branch_tree
            if entry.get("type") == "blob" and entry.get("path")
        }

        # Variant resolution (F14 both-spellings rule, mirroring
        # src/core/deliverables.deliverable_path_variants): the canonical
        # path first, then the ``repo/``-prefixed checkout spelling. The
        # blob is written back to the SAME path it was found at.
        resolved: list[tuple[str, str]] = []  # (canonical, path on branch)
        missing: list[str] = []
        for canonical in files_contract:
            for variant in (canonical, f"repo/{canonical}"):
                if variant in branch_paths:
                    resolved.append((canonical, variant))
                    break
            else:
                missing.append(canonical)

        if not resolved:
            return await _fallback(
                f"none of the {len(files_contract)} contracted file "
                f"deliverable(s) exist on the branch — an empty curation "
                f"would discard whatever work exists"
            )

        main_tree = await gitea_client.list_tree(repo_name, "main")
        if main_tree is None:
            return await _fallback(f"main tree unreadable at {repo_name}")
        main_paths = {
            str(entry.get("path"))
            for entry in main_tree
            if entry.get("type") == "blob" and entry.get("path")
        }

        files: list[dict[str, str]] = []
        for canonical, path in resolved:
            blob = await gitea_client.get_file_bytes(repo_name, path, ref=branch)
            if blob is None:
                return await _fallback(f"blob unreadable at {path}@{branch}")
            files.append(
                {
                    "path": path,
                    "content_b64": base64.b64encode(blob).decode(),
                    # ``create`` on an existing path is a Gitea 422; pick per
                    # file against main's tree.
                    "operation": "update" if path in main_paths else "create",
                }
            )
    except TerminalMergeReconciliationError:
        raise
    except Exception as e:  # noqa: BLE001 — a curation bug must not lose work
        return await _fallback(f"unexpected curation error: {e!r}")

    message = f"curated merge: {role} ({job_id[:8]}) — {len(files)} deliverable(s)"
    reconciled_sha: str | None = None
    if durable:
        assert completion_command_id is not None
        message = completion_commit_message(
            message,
            command_id=completion_command_id,
            effect_kind=_S33_CURATED_COMMIT_EFFECT,
        )
        try:
            prior_commit = await probe_completion_commit(
                gitea_client,
                repo_name=repo_name,
                branch="main",
                command_id=completion_command_id,
                effect_kind=_S33_CURATED_COMMIT_EFFECT,
            )
        except CompletionEffectProbeError as exc:
            raise TerminalMergeReconciliationError(str(exc)) from exc
        if prior_commit is not None:
            reconciled_sha = prior_commit.commit_sha

    if reconciled_sha is None:
        try:
            committed = await gitea_client.change_files(
                repo_name, "main", files, message=message
            )
        except Exception as e:  # noqa: BLE001
            if durable:
                raise TerminalMergeReconciliationError(
                    f"curated commit response was ambiguous: {e!r}"
                ) from e
            return await _fallback(f"curated commit raised: {e!r}")
        if not committed:
            if durable:
                raise TerminalMergeReconciliationError(
                    "curated commit response was ambiguous or refused"
                )
            return await _fallback("curated commit refused by change_files")

    # ------------------------------------------------------------------
    # The curated commit is on ``main`` — the point of no return. From here
    # every failure is a note, NEVER a fallback (a full merge now would
    # re-land the whole branch on top of the curated files).
    # ------------------------------------------------------------------
    notes: list[str] = []
    if reconciled_sha is not None:
        curated_sha = reconciled_sha
    else:
        try:
            curated_sha = await gitea_client.get_branch_head_sha(repo_name, "main")
        except Exception:  # noqa: BLE001 — sha is provenance, not load-bearing
            curated_sha = None
    sha8 = (curated_sha or "")[:8] or "?"
    notes.append(
        f"curated merge: {len(resolved)}/{len(files_contract)} contracted "
        f"deliverable(s) copied to main @ {sha8}"
    )
    for canonical, path in resolved:
        notes.append(
            f"merged {canonical}"
            + (f" (branch spelling: {path})" if path != canonical else "")
        )
    for canonical in missing:
        notes.append(
            f"contracted deliverable NOT on the branch (not curated): {canonical}"
        )

    # Audit-PR ceremony: the numbered PR documents the branch's full diff,
    # then closes unmerged — the branch stays behind as the scratchpad's
    # audit log. Legacy calls retain the best-effort behavior. Durable calls
    # first reconcile the exact marker so a create-response loss cannot leave
    # duplicate audit PRs.
    try:
        record_path = loop_retro_path(job, ctx or {})
        title = (job.get("description") or "").splitlines()[0].strip()[:200] or (
            f"Loop job {job_id[:8]}"
        )
        body = f"job: {job_id}\nbranch: {branch}"
        pr = None
        if durable:
            assert completion_command_id is not None
            title = completion_pr_title(
                title,
                command_id=completion_command_id,
                effect_kind=_S33_CURATED_AUDIT_PR_EFFECT,
            )
            body = completion_pr_body(
                body,
                command_id=completion_command_id,
                effect_kind=_S33_CURATED_AUDIT_PR_EFFECT,
            )
            prior_audit_pr = await probe_completion_pull_request(
                gitea_client,
                repo_name=repo_name,
                head=branch,
                base="main",
                command_id=completion_command_id,
                effect_kind=_S33_CURATED_AUDIT_PR_EFFECT,
            )
            if prior_audit_pr is not None:
                pr = {
                    "number": prior_audit_pr.pr_index,
                    "state": prior_audit_pr.state,
                }
        if pr is None:
            pr = await gitea_client.create_pr(
                repo_name,
                title=title,
                head=branch,
                base="main",
                body=body,
            )
            if durable and not pr:
                raise TerminalMergeReconciliationError(
                    "command-keyed curated audit pull request could not be created"
                )
        if pr:
            if pr.get("state") != "closed":
                await gitea_client.comment_on_pr(
                    repo_name,
                    pr["number"],
                    (
                        f"Curated merge (§6.4): {len(resolved)} contracted "
                        f"deliverable(s) landed on `main` as `{curated_sha or '?'}`.\n"
                        f"Record: `{record_path}`\n\n"
                        f"This PR is the branch's audit trail and is closed "
                        f"WITHOUT merging — the branch is a scratchpad; only "
                        f"contracted deliverables merge."
                    ),
                )
                closed = await gitea_client.close_pr(repo_name, pr["number"])
                if not closed:
                    if durable:
                        raise TerminalMergeReconciliationError(
                            f"command-keyed audit PR #{pr['number']} could not be closed"
                        )
                    notes.append(
                        f"audit PR #{pr['number']} could not be closed "
                        f"(left open, unmerged)"
                    )
        else:
            notes.append("audit PR could not be created (curated commit landed)")
    except (CompletionEffectProbeError, TerminalMergeReconciliationError) as e:
        if durable:
            raise TerminalMergeReconciliationError(str(e)) from e
        notes.append(f"audit PR ceremony failed: {e!r}")
    except Exception as e:  # noqa: BLE001 — ceremony must not flip the outcome
        if durable:
            raise TerminalMergeReconciliationError(
                f"durable audit PR ceremony was interrupted: {e!r}"
            ) from e
        notes.append(f"audit PR ceremony failed: {e!r}")

    logger.info(
        "curated merge for job %s: %d/%d deliverable(s) → main (%s)",
        job_id[:8],
        len(resolved),
        len(files_contract),
        sha8,
    )
    return "curated", curated_sha, notes
