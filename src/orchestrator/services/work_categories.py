"""Work categories — the contract a ticket carries, independent of who runs it.

The loop spent a month producing tested Python and no UI. The cause was not a
gate rejecting design work; it was that no design ticket was ever *proposed*.
Roles were the vocabulary (scholar, critic, developer), each job inferred its own
definition of done, and the cheapest way to look finished was another verifiable
module. Nothing in the machinery said "an answer is a deliverable" or "a
screenshot is evidence", so nothing produced them.

This module supplies the missing half of the model
(knowledge-base/knowledge/features/officer_backlog_pools.md §2-§3):

    category = a property of the WORK  — what shape the deliverable takes,
                                          what counts as evidence, when to stop
    expert   = a property of the WORKER — what skills it brings

The two are many-to-many on purpose. A developer given "compare rendering A vs B"
is a *researcher* on that ticket: throwaway code, a decision note with numbers.
The same developer given "implement the winner" is an *executor*: files under
``projects/<slug>/``, evidence that fits the claim. One expert, two contracts,
because the contract belongs to the ticket.

Three categories, and the parallelism rule that falls out of them: researchers
and testers write to the knowledge base and can run concurrently; executors touch
shared project state and are serialized (§5.5). That generalizes the old
name-based ``LOOP_ANALYSIS_ROLES`` split without replacing it — legacy loops keep
their name-based checks, deliberately (§2).

**Effort ceilings are posture, not caps.** Each block states how big its job is
allowed to be and that bigger discoveries become follow-up tickets. No mechanical
per-ticket budget or wall-clock cap is set anywhere in this feature (§13.4): the
officer is the brake. The blocks therefore have to carry that discipline in
words, which is why they say so explicitly rather than leaving it implied.

Pure module: constants and string composition, no I/O, no database. The tick
(B3) and the kickoff builder (B5) are its callers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from shared.backlog_tags import (
    PARALLEL_SAFE_TAG,
    READY_TAG,
    category_values,
    expert_values,
    has_tag,
)

# Note types that can carry a backlog ticket. Keep the read query and manual
# admission on the same vocabulary without importing backlog storage/lifecycle.
BACKLOG_NOTE_TYPES: tuple[str, ...] = ("feature", "issue", "idea")

RESEARCHER = "researcher"
TESTER = "tester"
EXECUTOR = "executor"

# Ordered: the sequence a sitrep or capacity line reports them in — diverge,
# critique, converge.
WORK_CATEGORIES: tuple[str, ...] = (RESEARCHER, TESTER, EXECUTOR)

# Default membership. Warn-not-forbid: the officer may dispatch outside it, and
# the mismatch is named in the kickoff rather than refused (§6 precedence law).
# Every config here extends ``worker_base`` — session-only experts
# (designer-interactive, assistant, centurion) are correctly absent.
CATEGORY_EXPERTS: dict[str, frozenset[str]] = {
    RESEARCHER: frozenset(
        {"scholar", "designer", "developer", "engineer", "general-worker"}
    ),
    TESTER: frozenset({"product-qa", "bughunter", "critic"}),
    # ``writer`` is executor-only: user-facing prose is a shipped artifact under
    # ``projects/<slug>/``, not an answer written to the KB. A writer sent to
    # investigate would be a researcher whose deliverable happens to read well —
    # which is exactly the confusion the category/expert split exists to prevent.
    EXECUTOR: frozenset(
        {"developer", "engineer", "designer", "general-worker", "writer"}
    ),
}

# Used when a ticket carries no ``expert:`` pin. Executor default is the
# engineer since the dev-vs-eng-01 bench (2026-09-08, engineer_expert.md §6):
# equal completion at 0.1-0.2x the wall time; the developer stays the pick
# for briefs that ask for strict test-first TDD.
CATEGORY_DEFAULT_EXPERT: dict[str, str] = {
    RESEARCHER: "scholar",
    TESTER: "product-qa",
    EXECUTOR: "engineer",
}

# Every expert an ``expert:`` tag may name. The tick validates against this
# before dispatch: ``config_name`` is otherwise unvalidated until agent boot,
# where a typo would fail every job in the pool and chain-trip its breaker.
KNOWN_EXPERTS: frozenset[str] = frozenset().union(*CATEGORY_EXPERTS.values())

# Exhaustive policy map.  This is intentionally not expressed as "everything
# except executor": a newly introduced or malformed category must acquire an
# explicit serialization decision before it can be used.  Failing open here
# would make a typo parallel-safe.
_CATEGORY_ALLOWS_PARALLEL: dict[str, bool] = {
    RESEARCHER: True,
    TESTER: True,
    EXECUTOR: False,
}


class UnknownCategory(ValueError):
    """A category name outside :data:`WORK_CATEGORIES`."""


def normalize_category(value: str | None) -> str | None:
    """Fold a category name to its canonical form, or None if unrecognized.

    Returning None rather than raising is the right shape for the tick's hot
    path: an unrecognized ``category:`` tag is a triage problem to report, not
    an exception to handle.
    """
    if not value:
        return None
    candidate = value.strip().lower()
    return candidate if candidate in WORK_CATEGORIES else None


def allows_parallel(category: str | None) -> bool:
    """Whether a *recognized* category may run concurrently.

    Category-policy input is stored/machine data, not the legacy loop-role
    bridge below.  Unknown, blank, absent, or mixed values therefore raise
    :class:`UnknownCategory`; callers cannot accidentally interpret a falsey
    normalization result as the permissive researcher/tester policy.
    """
    normalized = normalize_category(category)
    if normalized is None:
        raise UnknownCategory(f"unknown work category: {category!r}")
    return _CATEGORY_ALLOWS_PARALLEL[normalized]


def default_expert(category: str) -> str:
    """The expert config to spawn when a ticket carries no ``expert:`` pin."""
    normalized = normalize_category(category)
    if normalized is None:
        raise UnknownCategory(f"unknown work category: {category!r}")
    return CATEGORY_DEFAULT_EXPERT[normalized]


def expert_fits_category(expert: str, category: str) -> bool:
    """True if ``expert`` is in the default membership map for ``category``.

    A False is a warning, never a refusal — the officer may deliberately send a
    scholar into an executor slot, and §6 says that mismatch gets named in the
    kickoff and logged, not blocked.
    """
    normalized = normalize_category(category)
    if normalized is None:
        return False
    return expert.strip().lower() in CATEGORY_EXPERTS[normalized]


# ---------------------------------------------------------------------------
# Legacy bridge
# ---------------------------------------------------------------------------

# Loop roles that map to an analysis category. Everything else — developer,
# designer, general-worker, writer, an unknown role — is execution.
# This mirrors ``is_loop_execution_role``'s law that execution is the open set
# and analysis is the closed one.
#
# Note the one deliberate divergence: ``bughunter`` maps to tester here but is
# NOT in ``project_loops.LOOP_ANALYSIS_ROLES``, so a loop running that role
# would still be treated as execution by the retro/delivery path. That is a
# pre-existing gap in the name-based list; this module does not paper over it,
# because [A3] warned specifically against generalizing the name-based checks in
# place. ``test_work_categories`` pins the divergence so it stays a known one.
_ROLE_CATEGORIES: dict[str, str] = {
    "scholar": RESEARCHER,
    "critic": TESTER,
    "product-qa": TESTER,
    "bughunter": TESTER,
}


def role_to_category(role: str | None) -> str:
    """Map a legacy loop role to its work category.

    The loop predates categories: its roles rotate by name and its kickoff picks
    a role block. B5 composes the same kickoff as category contract + expert
    identity, and this is the bridge that lets it do so without changing what a
    legacy loop dispatches.
    """
    return _ROLE_CATEGORIES.get((role or "").strip().lower(), EXECUTOR)


# ---------------------------------------------------------------------------
# Ticket classification
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TicketClassification:
    """What a ticket's tags say about how to dispatch it.

    ``problems`` is the point of this type. A ticket with two ``category:`` tags
    or a misspelled expert is ambiguous, and the tick must SKIP it with a sitrep
    line rather than resolve the ambiguity by guessing — picking the first tag
    would silently dispatch work into the wrong pool under the officer's name.
    """

    category: str | None
    expert: str | None
    ready: bool
    parallel_safe: bool
    problems: tuple[str, ...]

    @property
    def dispatchable(self) -> bool:
        """Ready, unambiguous, and categorized — safe for the tick to pull."""
        return self.ready and self.category is not None and not self.problems


def classify_ticket(tags: Iterable[str] | None) -> TicketClassification:
    """Read dispatch intent off a ticket's tags.

    Does not consult claim state, capacity, or the breaker — those are the
    tick's business. This answers only "what does this ticket say it is".
    """
    problems: list[str] = []

    categories = category_values(tags)
    category: str | None = None
    if len(categories) > 1:
        problems.append(
            "multiple category: tags (" + ", ".join(sorted(categories)) + ")"
        )
    elif categories:
        category = normalize_category(categories[0])
        if category is None:
            problems.append(f"unknown category: {categories[0]!r}")

    experts = expert_values(tags)
    expert: str | None = None
    if len(experts) > 1:
        problems.append("multiple expert: tags (" + ", ".join(sorted(experts)) + ")")
    elif experts:
        expert = experts[0]
        if expert not in KNOWN_EXPERTS:
            # Caught here rather than at agent boot, where the failure would look
            # like a job failure and count toward the pool's breaker.
            problems.append(f"unknown expert: {expert!r}")
            expert = None

    return TicketClassification(
        category=category,
        expert=expert,
        ready=has_tag(tags, READY_TAG),
        parallel_safe=has_tag(tags, PARALLEL_SAFE_TAG),
        problems=tuple(problems),
    )


def resolve_expert(classification: TicketClassification) -> str:
    """The expert config to spawn for a classified ticket.

    The ticket's validated pin wins; otherwise the category default. The
    officer's explicit argument beats both, but that resolution belongs to the
    dispatch helper — this is the ticket-side half.
    """
    if classification.category is None:
        raise UnknownCategory("cannot resolve an expert for an uncategorized ticket")
    return classification.expert or default_expert(classification.category)


# ---------------------------------------------------------------------------
# Contract blocks
# ---------------------------------------------------------------------------

# Binary close criteria. Binary is the operative word: yes/no items produce the
# highest agreement between judges, where "was this done well?" produces
# whatever mood the judge is in. The officer runs these at close; the worker
# sees them in its kickoff, so the bar is known in advance rather than
# discovered in review.
_CLOSE_CHECKLISTS: dict[str, tuple[str, ...]] = {
    RESEARCHER: (
        "decision note written at the declared slug",
        "residual unknowns named, not glossed",
        "recommendation is actionable as written",
        "the consumer of this answer is named",
    ),
    TESTER: (
        "each finding filed as its own issue note",
        "3-7 findings, or an explicit 'no blocking issues found'",
        "every finding carries severity, evidence, smallest remediation",
        "UI findings name a Nielsen heuristic or a WCAG SC by number",
        "no duplicates of findings already in the backlog",
    ),
    EXECUTOR: (
        "declared deliverables exist at their declared paths",
        "evidence matches the claim class, not the cheapest class available",
        "screenshots are reproducible captures (UI tickets)",
        "axe criticals are zero (UI tickets)",
        "the referenced style guide was followed (UI tickets)",
        "the completion report says what was NOT done",
    ),
}

_CATEGORY_BLOCKS: dict[str, str] = {
    RESEARCHER: (
        "Your deliverable is an ANSWER, not a product increment. This is a "
        "SPIKE: a bounded investigation that ends in a decision, not an "
        "open-ended study and not the first half of an implementation. Answer "
        "the question, then STOP. If the work opens bigger questions, they "
        "become follow-up tickets — extending a spike is not your call to "
        "make: file the follow-up and stop. "
        "DELIVERABLE: a decision note in the KB carrying your findings, the "
        "residual unknowns you did NOT resolve, and a recommendation someone "
        "can act on. Name its consumer — which decision or follow-up ticket "
        "this answer feeds — so research cannot drift into shelfware. "
        "EVIDENCE: citations, measured comparisons, named sources. Code you "
        "write here is disposable evidence: it lives and dies in this job's "
        "isolated repository and is never delivered to the project cloud "
        "folder. "
        "Design research is first-class work with firmer anchors than most "
        "people assume — current design systems (Material 3, Apple HIG), WCAG "
        "success criteria by number, type scales, spacing systems. Cite them "
        "the way you would cite a paper; you are not being asked to invent a "
        "visual language from nothing. "
        "For A-vs-B comparisons, gather evidence criterion by criterion BEFORE "
        "stating a verdict — a one-shot 'which is better' judgment flips on "
        "presentation order alone."
    ),
    TESTER: (
        "Your output is ISSUE TICKETS, not file commits. You audit what exists "
        "— the product a real user must operate, not the codebase. "
        "File 3-7 findings MAXIMUM: high-leverage beats a long list. Each one "
        "is its own `issue` note carrying severity, confidence, the target "
        "user or workflow, the evidence, and the smallest useful remediation. "
        "A finding that lives only in your report is invisible to the next "
        "iteration — the `issue` note IS the handoff. "
        "EVIDENCE: reproduce a defect at least twice before filing it. For an "
        "ABSENCE the evidence is the audit trail — paths searched, commands "
        "run, expected-versus-observed. 'No UI exists' is a HIGH finding even "
        "when every unit test passes; a tested module no user can reach is a "
        "product gap, not a success. "
        "For UI findings, name the anchor you judged against: a Nielsen "
        "usability heuristic, or a WCAG 2.2 success criterion by number — SC "
        "1.4.3 contrast, SC 2.4.7 focus visible, SC 2.5.8 target size at least "
        "24x24 CSS px (platform practice is 44pt on iOS, 48dp on Android). An "
        "axe-core scan is the cheap deterministic floor: it catches roughly "
        "half of accessibility problems, which is exactly why it layers UNDER "
        "your judgment instead of replacing it. Anchored critique survives "
        "review; vibes do not. "
        "Check the KB and the backlog above for findings already filed and "
        "UPDATE those rather than re-filing duplicates. Do NOT fix anything "
        "and do NOT propose new features. "
        "'No blocking issues found' is a valid outcome, not a failure to find "
        "work."
    ),
    EXECUTOR: (
        "Ship it, prove it appropriately, report it honestly. "
        "When this job carries a copy of the project's cloud folder, the task "
        "brief's Project Cloud Folder section names its directory and says "
        "what happens to your edits there; durable project files go there, and "
        "job scratch stays outside it. The declared deliverables are the one "
        "hard check on this work. "
        "EVIDENCE APPROPRIATE TO THE CLAIM: tests for logic, SCREENSHOTS for "
        "UI, a running deployment (bring it up, curl its health endpoint) for "
        "hosting, style-guide conformance for design. Tests are regression "
        "rails, never the score — forty green tests written by the same head "
        "that wrote the code prove that head was self-consistent and nothing "
        "more. Do not reach for the evidence that is easiest to produce; reach "
        "for the evidence that would convince someone who doubts you. "
        "A screenshot counts only as a REPRODUCIBLE capture: fixed viewport, "
        "settled fonts, seeded demo data, nothing mid-animation — so the "
        "officer comparing this iteration against the last one sees a real "
        "difference rather than a rendering accident. "
        "UI work must name the style guide it conforms to and follow it. "
        "Without that link every executor invents its own visual language and "
        "the product drifts apart one screen at a time. "
        "SCOPE: deliver THIS piece of work. Discoveries that deserve their own "
        "work become follow-up tickets. No budget or clock will stop you from "
        "expanding, which is precisely why the discipline has to come from "
        "you. "
        "Report truthfully — what you shipped, what you did not, what you are "
        "unsure of. An honest 'this did not work' is worth more than a "
        "confident summary that does not survive the officer's review."
    ),
}


def close_checklist(category: str) -> tuple[str, ...]:
    """The binary yes/no items the officer runs when closing this ticket."""
    normalized = normalize_category(category)
    if normalized is None:
        raise UnknownCategory(f"unknown work category: {category!r}")
    return _CLOSE_CHECKLISTS[normalized]


def category_block(category: str, *, include_checklist: bool = True) -> str:
    """The contract text for a category: deliverable shape, evidence, stopping rule.

    Composed into the kickoff ahead of the expert's identity block (§7). The
    close checklist rides along by default so the worker knows the bar before it
    starts, not after the officer applies it.
    """
    normalized = normalize_category(category)
    if normalized is None:
        raise UnknownCategory(f"unknown work category: {category!r}")
    block = _CATEGORY_BLOCKS[normalized]
    if not include_checklist:
        return block
    items = "; ".join(close_checklist(normalized))
    return (
        f"{block} "
        f"CLOSE CHECKLIST — the officer answers each of these yes or no when he "
        f"closes this ticket: {items}."
    )
