"""Deterministic deliverable-contract gate at the seal (P1-C).

knowledge-base/knowledge/issues/officer_blind_reads_and_worker_bureaucracy.md §4 P1-C / §7 annex E.

Jobs may declare ``context.required_deliverables`` — workspace-relative paths
(plus ``kb:<slug>`` knowledge-note slugs) that MUST exist before a completion
that claims done-ness is allowed to seal. Verifier #1's "26/27 todos done"
seal shipped 0/7 required deliverables and consumed a human-priced officer
review cycle; the check the officer ran by hand (blind, through broken tools)
is here a platform gate that cannot be argued with by narrative.

Semantics (annex E, each element proven elsewhere):

* Applies only when the agent's report CLAIMS completion (goal_achieved or a
  job_complete freeze) and the status would resolve to ``completed`` /
  ``pending_review`` / ``reviewing``. ``reviewing`` is included so a bounce
  fires BEFORE the critic spawn — cheap deterministic existence checks first,
  critic LLM second. An honest incomplete stop (no claim) is never gated.
* Validates END-STATE ARTIFACTS only, at the job branch HEAD in Gitea —
  never worker narrative, todo counts, or confidence.
* Path normalization (F14): ``repo/`` prefix and ``./`` are tolerated on
  BOTH sides — the manifest and the tree — so a complete job can never be
  bounced over a prefix.
* On failure: bounce, don't seal. The job re-enters the P1-A
  resume-with-feedback lane (``queued_feedback`` + ``queued_feedback_reason``
  → the worker's [FEEDBACK_RESUME] banner) with a PRECISE missing/present
  listing, resuming on its own checkpoint. Bounces are capped
  (:data:`DELIVERABLE_GATE_BOUNCE_CAP`). At the cap, an explicit external
  publication contract terminalizes as blocked/undelivered; ordinary in-repo
  contracts retain the historical review escalation. Never an infinite loop,
  and never a successful seal for an unproved PR.
* Gitea unavailable / repo unresolvable → fail-open for ordinary in-repo
  artifacts: skip, log, stamp ``{skipped: true, reason}``. ``kb:`` entries
  fail-open individually when the vector store can't answer.
* Explicit ``pr:`` publication and historical ``repos/`` publication promises
  never fail open. Missing proof bounces within the cap, then becomes the
  terminal blocked/undelivered outcome.
* A bounced seal spawns NO critic/curator subjobs (the /complete handler
  early-returns); a passed gate leaves that flow untouched.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Iterator

from orchestrator.services.deliverable_contracts import BLOCKED_UNDELIVERED_OUTCOME
from orchestrator.services.job_delivery import forge_repo_from_datasource
from shared.runtime.services.forge import get_pull_request_status
from shared.deliverable_contract import (
    KB_DELIVERABLE_PREFIX,
    PR_DELIVERABLE_PREFIX,
    cloned_repo_deliverables as _cloned_repo_deliverables,
    is_cloned_repo_deliverable as _is_cloned_repo_deliverable,
    normalize_deliverable,
    normalize_repository_identity,
    parse_required_deliverables as _parse_required_deliverables,
    pr_repositories,
)

logger = logging.getLogger(__name__)

# Maximum resume-with-feedback bounces before an ordinary manifest moves to
# review or an explicit publication contract ends blocked/undelivered.
DELIVERABLE_GATE_BOUNCE_CAP = 2

# Prefix marking a knowledge-note deliverable (checked against the KB index,
# not the Gitea tree). Mirrors src/core/deliverables.py agent-side.
# Statuses (as resolved by determine_job_status) whose seal the gate may
# intercept. ``reviewing`` is the critic-spawn lane — see module docstring.
_GATED_STATUSES: frozenset[str] = frozenset(
    {"completed", "pending_review", "reviewing"}
)


def _integer(value: Any, *, default: int = -1) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Manifest parsing + path normalization (F14)
# ---------------------------------------------------------------------------


def normalize_deliverable_path(path: Any) -> str | None:
    """Canonicalize a deliverable path; ``None`` when it isn't one.

    Canonical form is workspace-relative WITHOUT the ``repo/`` prefix and
    without ``./``. ``kb:<slug>`` entries keep their prefix.
    """
    return normalize_deliverable(path)


# Cloned repository datasources land here, and the platform gitignores the
# directory at seed time on purpose — src/core/workspace.py,
# src/core/datasource_setup.py and src/tools/orchestrator/repositories.py all
# write it ("working-tree only; never versioned"), guarding the
# contentless-gitlink bug b1758f38. Note the single character between this and
# the F14 ``repo/`` prefix normalized away above: ``repo/`` is the job's OWN
# tree, ``repos/`` is somebody else's repository, mounted and unversioned.
def is_cloned_repo_deliverable(path: Any) -> bool:
    """True for a manifest entry inside a cloned repository datasource.

    Such an entry can never be verified from the job's own versioned tree,
    because the platform refuses to version it. Treating it as missing is a
    false negative, and a costly one: it bounces a job that delivered, and
    teaches the agent that the way to pass is to defeat the .gitignore.
    See knowledge-base/knowledge/issues/deliverable_gate_cannot_see_cloned_repo_deliverables.md.
    """
    return _is_cloned_repo_deliverable(path)


def cloned_repo_deliverables(manifest: list[str]) -> list[str]:
    """The manifest entries this gate can never verify, in declared order.

    Job creation refuses these entries and records Officer-ticket publication
    provenance before admission can claim the ticket. This helper remains the
    single parser shared with compatibility handling for historical rows.
    """
    return _cloned_repo_deliverables(manifest)


def parse_required_deliverables(context: Any) -> list[str]:
    """The job's manifest as a clean, deduplicated list (possibly empty).

    Accepts a job ``context`` dict (or its JSON string form) or the raw
    list value. Entries that don't normalize are dropped.
    """
    return _parse_required_deliverables(context)


@dataclass(frozen=True, slots=True)
class DeliverableGateResult:
    """Backward-compatible three-value result plus server outcome metadata."""

    status: str | None
    actions: list[str]
    bounced: bool
    outcome_kind: str | None = None
    # Typed source continuity, independent of display actions and legacy tuples.
    preserves_human_wait: bool = False
    # Why the seal was held because its delivery could not be proven — the
    # completion authority records it as the job's error_message and names it
    # in the review notification, so the hold is never silent.
    hold_reason: str | None = None

    def __iter__(self) -> Iterator[Any]:
        # Existing collaborators/tests intentionally keep their historical
        # three-value unpacking; only the completion authority reads the
        # server-owned outcome attribute.
        yield self.status
        yield self.actions
        yield self.bounced


def _tree_has_path(tree_paths: set[str], canonical: str) -> bool:
    """Membership check tolerant of the ``repo/`` prefix on the TREE side.

    The manifest side is already canonicalized; a repository job's checkout
    may place the file under ``repo/`` in the pushed tree (and a manifest
    written with the prefix was canonicalized away), so both spellings count.
    """
    return canonical in tree_paths or f"repo/{canonical}" in tree_paths


# ---------------------------------------------------------------------------
# Trigger predicate
# ---------------------------------------------------------------------------


def gate_applies(job: dict[str, Any], result: dict[str, Any], new_status: Any) -> bool:
    """True when this completion must pass the deliverable gate.

    Requires: a gated status, a stop, a genuine COMPLETION CLAIM
    (goal_achieved or a job_complete freeze — an honest incomplete stop that
    merely lands on pending_review is not gated), and a non-empty manifest.
    """
    if new_status not in _GATED_STATUSES:
        return False
    if not result.get("should_stop", False):
        return False
    from orchestrator.services.completion import _parse_context, _parse_freeze_data

    fd = _parse_freeze_data(job)
    if not fd:
        fd = result.get("freeze_data")
        if isinstance(fd, str):
            try:
                fd = json.loads(fd)
            except (json.JSONDecodeError, ValueError):
                fd = {}
        if not isinstance(fd, dict):
            fd = {}
    manifest = parse_required_deliverables(_parse_context(job))
    if not manifest:
        return False
    # An explicit PR promise is an external-publication contract. An honest
    # worker saying it could not publish must reach the same bounded gate as a
    # success claim, otherwise ``should_stop`` can bypass proof merely by
    # avoiding goal_achieved.
    if pr_repositories(manifest) or cloned_repo_deliverables(manifest):
        return True
    claimed = (
        bool(result.get("goal_achieved"))
        or fd.get("freeze_type") == "job_complete"
        or fd.get("status") == "job_completed"
    )
    if not claimed:
        return False
    return claimed


# ---------------------------------------------------------------------------
# Evaluation (Gitea + KB existence)
# ---------------------------------------------------------------------------


async def _resolve_repo_ref(
    job: dict[str, Any], db: Any
) -> tuple[str | None, str | None]:
    """(repo_name, ref) for the job's pushed branch, or (None, None).

    Mirrors main.resolve_job_repo without the HTTP semantics: repo on the
    job row, else the parent's (subjob-on-branch model). Root jobs push to
    the repo default branch — ``main`` for per-job repos.
    """
    repo_name = job.get("repo_name")
    if not repo_name and job.get("parent_job_id") and db is not None:
        try:
            parent = await db.get_job(str(job["parent_job_id"]))
        except Exception:  # noqa: BLE001 — unresolvable → fail-open upstream
            parent = None
        if parent and parent.get("repo_name"):
            repo_name = parent["repo_name"]
    if not repo_name:
        return None, None
    ref = job.get("branch_name") or "main"
    return str(repo_name), str(ref)


def _seal_names_no_revision(job: dict[str, Any], freeze: dict[str, Any]) -> bool:
    """True for a completion seal whose worker could not read its own HEAD.

    Only an explicit ``head_commit: null`` counts — an ABSENT key comes from a
    writer that never carried it and is judged on the tree as before — and a
    ``delivered_commit`` (the agent's post-push proof) outranks it. Lite tiers
    have no git, so a null HEAD is their normal state, not a failure.
    """
    if "head_commit" not in freeze or freeze.get("head_commit") is not None:
        return False
    if freeze.get("delivered_commit"):
        return False
    if not (
        freeze.get("freeze_type") == "job_complete"
        or freeze.get("status") == "job_completed"
    ):
        return False
    from orchestrator.services.completion import _parse_resolved_config
    from shared.backend_kinds import LITE_BACKENDS
    from shared.workspace_contract import configured_workspace_backend

    for config in (job.get("config_override"), _parse_resolved_config(job)):
        try:
            backend = configured_workspace_backend(config)
        except Exception:  # noqa: BLE001 — an unreadable tier: cannot tell
            # Judge on the tree alone, the pre-existing behavior.
            return False
        if backend in LITE_BACKENDS:
            return False
    return True


async def _kb_note_exists(
    vector_db: Any, project_id: str | None, slug: str
) -> bool | None:
    """KB-note existence via the knowledge_index; ``None`` = unverifiable.

    Same store the MCP knowledge tools read. Any failure (no handle, no
    project, query error) returns None so the caller fails open on that
    entry rather than bouncing a job over infrastructure.
    """
    if vector_db is None or not project_id or not slug:
        return None
    try:
        async with vector_db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT 1 FROM knowledge_index WHERE project_id = $1 AND note_id = $2",
                str(project_id),
                slug,
            )
        return row is not None
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "Deliverable gate: KB lookup failed for note %r (project %s): %s",
            slug,
            project_id,
            exc,
        )
        return None


async def _evaluate_pr_deliverable(
    job: dict[str, Any],
    *,
    db: Any,
    contract: dict[str, Any] | None,
) -> dict[str, Any]:
    """Prove one exact immutable PR contract against current forge state."""

    job_id = str(job.get("id") or "")
    manifest_prs = pr_repositories(parse_required_deliverables(job.get("context")))
    contract_prs = [
        normalized
        for value in list((contract or {}).get("pr_repositories") or [])
        if (normalized := normalize_repository_identity(value)) is not None
    ]
    expected = manifest_prs[0] if len(manifest_prs) == 1 else None
    if expected is None or contract_prs != [expected]:
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected or 'invalid'}"],
            "present": [],
            "unverified": [],
            "reason": "immutable PR contract authority is missing or mismatched",
        }

    try:
        authority = await db.get_job_pull_request_authority(job_id)
    except Exception as exc:  # noqa: BLE001 - strict unavailable result
        logger.warning("PR authority read failed for %s: %s", job_id, exc)
        authority = None
    if not isinstance(authority, dict):
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "no valid server-recorded pull request exists",
        }
    recorded_repo = normalize_repository_identity(authority.get("repository"))
    if recorded_repo != expected:
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "the recorded pull request names a different repository",
        }

    try:
        datasources = await db.resolve_datasources_for_job(
            job_id,
            str(job.get("project_id")) if job.get("project_id") else None,
        )
    except Exception as exc:  # noqa: BLE001 - strict unavailable result
        logger.warning("PR deliverable datasource read failed for %s: %s", job_id, exc)
        datasources = []
    datasource = next(
        (
            item
            for item in datasources
            if str(item.get("id") or "") == str(authority.get("datasource_id") or "")
        ),
        None,
    )
    bindings = (contract or {}).get("pr_bindings")
    if isinstance(bindings, str):
        try:
            bindings = json.loads(bindings)
        except (TypeError, ValueError, json.JSONDecodeError):
            bindings = []
    bindings = bindings if isinstance(bindings, list) else []
    binding = (
        bindings[0] if len(bindings) == 1 and isinstance(bindings[0], dict) else {}
    )
    if (
        datasource is None
        or str(datasource.get("id") or "") != str(binding.get("datasource_id") or "")
        or normalize_repository_identity(binding.get("repository")) != expected
        or str(binding.get("forge") or "").strip().lower()
        != str(authority.get("forge") or "").strip().lower()
        or _integer(binding.get("policy_revision"))
        != _integer(datasource.get("policy_revision"), default=0)
        or _integer(authority.get("policy_revision"))
        != _integer(datasource.get("policy_revision"), default=0)
        or bool(datasource.get("read_only") or datasource.get("project_read_only"))
    ):
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "the exact writable repository connector is detached",
        }

    try:
        status = await get_pull_request_status(
            forge_repo_from_datasource(datasource), int(authority.get("number") or 0)
        )
    except Exception as exc:  # noqa: BLE001 - strict fail closed
        logger.warning("PR deliverable live proof failed for %s: %s", job_id, exc)
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "the pull request could not be verified at its forge",
        }

    state = str((status or {}).get("state") or "").strip().lower()
    live_head = str((status or {}).get("head") or "").strip()
    live_base = str((status or {}).get("base") or "").strip()
    live_revision = str((status or {}).get("head_sha") or "").strip().lower()
    if (
        state not in {"open", "merged", "closed"}
        or live_head != str(authority.get("head") or "")
        or live_base != str(authority.get("base") or "")
        or live_revision != str(authority.get("source_revision") or "").lower()
    ):
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "the forge returned a different or incomplete PR identity",
        }
    if not await db.mark_job_pr_deliverable_verified(
        job_id,
        datasource_id=str(datasource.get("id")),
        repository=expected,
        number=int(authority.get("number") or 0),
        record_id=str(authority.get("record_id") or ""),
        record_generation=_integer(authority.get("record_generation")),
        head=live_head,
        base=live_base,
        head_revision=live_revision,
        state=state,
    ):
        return {
            "passed": False,
            "strict": True,
            "missing": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
            "present": [],
            "unverified": [],
            "reason": "the PR evidence changed while proof was recorded",
        }
    return {
        "passed": True,
        "strict": True,
        "missing": [],
        "present": [f"{PR_DELIVERABLE_PREFIX}{expected}"],
        "unverified": [],
        "pr_state": state,
        "pr_number": int(authority.get("number") or 0),
    }


async def explicit_pr_delivery_block_reason(
    job: dict[str, Any], *, db: Any
) -> str | None:
    """Fail-closed human-approval proof for an explicit PR contract."""

    declared = pr_repositories(parse_required_deliverables(job.get("context")))
    if not declared:
        return None
    try:
        contract = await db.get_job_deliverable_contract(str(job.get("id")))
    except Exception as exc:  # noqa: BLE001
        logger.warning("PR contract read failed during approval: %s", exc)
        return "the immutable pull-request contract could not be read"
    report = await _evaluate_pr_deliverable(job, db=db, contract=contract)
    if report.get("passed"):
        return None
    return str(report.get("reason") or "the declared pull request is not proven")


async def evaluate_deliverable_gate(
    job: dict[str, Any],
    *,
    db: Any,
    gitea: Any,
    vector_db: Any = None,
) -> dict[str, Any]:
    """Check every manifest entry at the job branch HEAD in Gitea.

    Returns a report dict:
      * ``{"skipped": True, "reason": ...}`` — Gitea down / repo or tree
        unresolvable (fail-open; the caller must not block the seal);
      * otherwise ``{"passed": bool, "missing": [...], "present": [...],
        "unverified": [...], "commit_sha": str|None, "ref": str}`` —
        ``unverified`` entries (kb: without a working KB lookup) never count
        as missing.
    """
    from orchestrator.services.completion import _parse_context, _parse_freeze_data

    manifest = parse_required_deliverables(_parse_context(job))
    if not manifest:
        return {"skipped": True, "reason": "no manifest"}

    declared_prs = pr_repositories(manifest)
    pr_report: dict[str, Any] | None = None
    if declared_prs:
        try:
            contract = await db.get_job_deliverable_contract(str(job.get("id")))
        except Exception as exc:  # noqa: BLE001 - explicit PR fails closed
            logger.warning(
                "Deliverable gate: immutable contract read failed for %s: %s",
                job.get("id"),
                exc,
            )
            contract = None
        pr_report = await _evaluate_pr_deliverable(job, db=db, contract=contract)
        if not pr_report.get("passed"):
            return pr_report

    historical_external_paths = cloned_repo_deliverables(manifest)
    if historical_external_paths:
        # New admissions refuse this legacy contract and name the exact pr:
        # replacement. Rows admitted by an older replica remain possible
        # during rollout, so the seal independently fails closed rather than
        # repeating the historical "unverifiable therefore satisfied" bug.
        return {
            "passed": False,
            "strict": True,
            "missing": historical_external_paths,
            "present": [],
            "unverified": [],
            "reason": (
                "an external-repository path is not a verifiable publication "
                "contract; a matching pr:owner/repository contract is required"
            ),
        }

    # Nothing reached the repository, so "missing from the repository" says
    # nothing about what the agent produced. The agent sets this when its
    # job-ending push does not land, or its final commit did not
    # (src/agent/core/phase.py, _push_job_ending_state and
    # _verify_job_ending_delivery): the deliverables exist, on a workspace pod
    # about to be reclaimed, and Gitea is empty or stale.
    #
    # Without this the gate reads every manifest entry as missing and BOUNCES —
    # resuming the agent to produce files it already produced, onto a workspace
    # that may no longer exist (see run_deliverable_gate). That bounce also
    # early-returns in the caller, so it preempts the verification escalation
    # that would otherwise report the real reason.
    #
    # This belongs with the four skips below rather than beside them: it is the
    # same graceful-degradation rule ("never block a seal on infrastructure the
    # worker cannot fix"), and it is invisible to the others only because it is
    # the one infrastructure failure that looks like a CLEAN read — the tree is
    # readable, it is merely empty.
    # knowledge-history/done/git_push_fails_silently_via_workspace_backend.md
    #
    # Skipping the tree check is not accepting the delivery, though: when the
    # manifest names paths in the job's own tree, the report is also marked
    # ``undelivered`` and run_deliverable_gate holds a ``completed`` seal for
    # review rather than merging a branch the agent says lacks its work.
    freeze = _parse_freeze_data(job) or {}
    if not isinstance(freeze, dict):
        freeze = {}
    names_repo_paths = any(
        not path.startswith((PR_DELIVERABLE_PREFIX, KB_DELIVERABLE_PREFIX))
        for path in manifest
    )
    if freeze.get("delivery_failed"):
        report = {
            "skipped": True,
            "reason": str(
                freeze.get("delivery_error")
                or "the job-ending push failed; deliverables were not delivered"
            ),
        }
        if names_repo_paths:
            report["undelivered"] = True
        return report

    if gitea is None or not getattr(gitea, "is_initialized", False):
        return {"skipped": True, "reason": "gitea unavailable"}

    repo_name, ref = await _resolve_repo_ref(job, db)
    if not repo_name or not ref:
        return {"skipped": True, "reason": "job repo unresolvable"}

    # The same verdict for a seal that cannot name the revision it delivered.
    # Existence at the branch tip proves nothing when the worker could not read
    # its own HEAD: on VM job afa0004b the tip was a two-hour-old revision
    # holding the deliverable SCAFFOLDS, so every manifest entry was "present"
    # and the gate passed work nobody had delivered. Current agents prove
    # delivery with ``delivered_commit`` or refuse with ``delivery_failed``
    # (src/agent/core/phase.py, _verify_job_ending_delivery); this also covers
    # records from agents that predate that proof.
    # knowledge-base/knowledge/issues/worker_git_versioning_stops_midjob_seal_pins_stale_revision.md
    if names_repo_paths and _seal_names_no_revision(job, freeze):
        return {
            "skipped": True,
            "undelivered": True,
            "reason": (
                "the worker could not read its workspace HEAD when it sealed "
                "(head_commit is null), so the job branch cannot be shown to "
                "hold the delivered work — its tip may be a stale revision"
            ),
        }

    try:
        tree = await gitea.list_tree(repo_name, ref)
    except Exception as e:  # noqa: BLE001
        logger.warning(
            "Deliverable gate: tree listing failed for %s@%s: %s",
            repo_name,
            ref,
            e,
        )
        tree = None
    if tree is None:
        return {
            "skipped": True,
            "reason": f"tree unreadable at {repo_name}@{ref}",
        }
    tree_paths = {
        str(entry.get("path"))
        for entry in tree
        if entry.get("type") == "blob" and entry.get("path")
    }

    commit_sha: str | None = None
    try:
        commit_sha = await gitea.get_branch_head_sha(repo_name, ref)
    except Exception:  # noqa: BLE001 — sha is provenance, not load-bearing
        commit_sha = None

    project_id = str(job["project_id"]) if job.get("project_id") else None
    missing: list[str] = []
    present: list[str] = []
    unverified: list[str] = []
    for path in manifest:
        if path.startswith(PR_DELIVERABLE_PREFIX):
            present.append(path)
            continue
        if path.startswith(KB_DELIVERABLE_PREFIX):
            slug = path[len(KB_DELIVERABLE_PREFIX) :]
            found = await _kb_note_exists(vector_db, project_id, slug)
            if found is None:
                # Unverifiable ≠ missing: fail open on this entry, log it.
                logger.warning(
                    "Deliverable gate: kb entry %r unverifiable for job %s "
                    "— treating as satisfied",
                    path,
                    job.get("id"),
                )
                unverified.append(path)
            elif found:
                present.append(path)
            else:
                missing.append(path)
            continue
        if _tree_has_path(tree_paths, path):
            present.append(path)
        else:
            missing.append(path)

    return {
        "passed": not missing,
        "missing": missing,
        "present": present,
        "unverified": unverified,
        "commit_sha": commit_sha,
        "ref": ref,
    }


# ---------------------------------------------------------------------------
# Bounce feedback rendering
# ---------------------------------------------------------------------------


def _render_bounce_feedback(
    report: dict[str, Any], bounce: int, cap: int
) -> tuple[str, str]:
    """(full feedback, short banner reason) for a bounced seal.

    The full text rides ``queued_feedback`` into the worker's compacted
    context; the short reason renders verbatim in the [FEEDBACK_RESUME]
    banner (P1-A honest-cause contract).
    """
    sha = report.get("commit_sha")
    where = f"commit {sha[:12]}" if sha else "the branch HEAD"
    ref = report.get("ref")
    missing = report.get("missing") or []
    present = report.get("present") or []
    strict_pr = bool(report.get("strict"))
    lines = [
        "DELIVERABLE CONTRACT GATE: this completion was refused because "
        + (
            "the declared pull-request delivery could not be proven."
            if strict_pr
            else "required deliverables are missing from the job branch."
        ),
        "",
        f"Checked at {where}" + (f" on branch `{ref}`" if ref else "") + ":",
        "",
        f"MISSING ({len(missing)}):",
    ]
    lines += [f"  - {p}" for p in missing]
    lines.append("")
    lines.append(f"PRESENT ({len(present)}):")
    lines += [f"  - {p}" for p in present] if present else ["  - (none)"]
    instruction = (
        "Push your branch and use repo_open_pr for the exact attached "
        "repository named by the pr: contract. Do not replace the contract "
        "with a knowledge note."
        if strict_pr
        else "Produce the MISSING artifacts at exactly these workspace paths "
        "(`repo/` prefix is accepted either way), commit them, and call "
        "job_complete again. Do NOT redo work that is already present — "
        "only the missing deliverables block the seal."
    )
    lines += [
        "",
        instruction,
        f"(Deliverable-gate bounce {bounce}/{cap}: after {cap} bounces "
        + (
            "an unproved publication ends blocked/undelivered."
            if strict_pr
            else "the job is handed to a human with this report instead."
        )
        + ")",
    ]
    reason = (
        f"deliverable contract gate: {len(missing)} of "
        f"{len(missing) + len(present)} required deliverables missing at "
        f"{where} (bounce {bounce}/{cap})"
    )
    return "\n".join(lines), reason


# ---------------------------------------------------------------------------
# The gate — called from the /complete handler via services.completion
# ---------------------------------------------------------------------------


async def run_deliverable_gate(
    job: dict[str, Any],
    result: dict[str, Any],
    new_status: str | None,
    *,
    db: Any,
    gitea: Any,
    queue_resume: Callable[..., Any],
    vector_db: Any = None,
) -> DeliverableGateResult:
    """Apply the deliverable-contract gate to a resolved completion status.

    Returns ``(new_status, actions, bounced)``:

    * gate not applicable → status unchanged, no actions, ``bounced=False``;
    * pass / fail-open skip → status unchanged, action + stamp written;
    * missing under the cap → ``queue_resume`` invoked (P1-A feedback lane),
      ``bounced=True`` — the caller must EARLY-RETURN without writing the
      sealed status or spawning critic/curator subjobs;
    * missing at the cap → no bounce; ``completed`` demotes to
      ``pending_review`` for an ordinary in-repo contract, while an explicit
      publication contract becomes terminal blocked/undelivered. The report
      remains stamped for operator and Officer inspection.

    All context writes use the atomic top-level merge (never a context
    read-modify-write).
    """
    if not gate_applies(job, result, new_status):
        return DeliverableGateResult(new_status, [], False, preserves_human_wait=True)

    job_id = str(job.get("id"))
    from orchestrator.services.completion import _parse_context

    prior = _parse_context(job).get("deliverable_gate")
    prior = prior if isinstance(prior, dict) else {}
    bounces = int(prior.get("bounces", 0) or 0)
    checked_at = datetime.now(timezone.utc).isoformat()

    report = await evaluate_deliverable_gate(
        job, db=db, gitea=gitea, vector_db=vector_db
    )

    async def _stamp(stamp: dict[str, Any]) -> None:
        try:
            await db.merge_job_context(job_id, {"deliverable_gate": stamp})
        except Exception as e:  # noqa: BLE001 — the stamp is observability
            logger.warning(
                "Deliverable gate: failed to stamp context for %s: %s", job_id, e
            )

    if report.get("undelivered"):
        # The repository does not provably hold the work. Never a bounce:
        # resuming the worker cannot repair its transport, and a bounce
        # early-returns past the verification escalation that reports the real
        # reason. Never a pass either: a ``completed`` seal merges the stale
        # branch, so it is held the way the bounce cap holds an unmet manifest.
        # Review-bound seals keep their lane: verification escalates
        # ``delivery_failed`` to a human before any critic runs. A null
        # ``head_commit`` from an agent predating the delivery proof carries no
        # such flag, so its critic still reviews the branch tip; the hold reason
        # below (error_message, notification) is what tells a human why.
        reason = str(report.get("reason"))
        final_status = new_status
        if new_status == "completed":
            from orchestrator.services.project_loops import job_loop_id
            from orchestrator.services.verification_ledger import escalation_status

            final_status = escalation_status(is_loop_job=bool(job_loop_id(job)))
        logger.error(
            "Deliverable gate: delivery UNPROVEN for job %s: %s — sealing as %s",
            job_id,
            reason,
            final_status,
        )
        await _stamp(
            {
                "skipped": True,
                "undelivered": True,
                "reason": reason,
                "checked_at": checked_at,
                "bounces": bounces,
            }
        )
        return DeliverableGateResult(
            final_status,
            [
                f"deliverable gate: delivery unproven ({reason}) — not bounced, "
                f"sealing as {final_status}"
            ],
            False,
            hold_reason=reason,
        )

    if report.get("skipped"):
        # Fail-open (graceful-degradation house rule): never block a seal on
        # infrastructure the worker cannot fix.
        reason = str(report.get("reason"))
        logger.warning(
            "Deliverable gate SKIPPED for job %s: %s — sealing without the check",
            job_id,
            reason,
        )
        await _stamp(
            {
                "skipped": True,
                "reason": reason,
                "checked_at": checked_at,
                "bounces": bounces,
            }
        )
        return DeliverableGateResult(
            new_status, [f"deliverable gate skipped ({reason})"], False
        )

    sha = report.get("commit_sha")
    if report.get("passed"):
        await _stamp(
            {
                "passed": True,
                "checked_at": checked_at,
                "commit_sha": sha,
                "present": report.get("present"),
                "unverified": report.get("unverified"),
                "bounces": bounces,
            }
        )
        n_present = len(report.get("present") or [])
        n_unverified = len(report.get("unverified") or [])
        action = (
            f"deliverable gate passed ({n_present}/{n_present + n_unverified} present"
        )
        if n_unverified:
            # Name the KIND. This line predated cloned-repo entries and called
            # every fail-open a "kb" one; saying kb about a repos/ path
            # describes a check that never ran.
            unverified_paths = report.get("unverified") or []
            kb_n = sum(
                1 for p in unverified_paths if str(p).startswith(KB_DELIVERABLE_PREFIX)
            )
            repo_n = n_unverified - kb_n
            kinds = []
            if kb_n:
                kinds.append(f"{kb_n} kb")
            if repo_n:
                kinds.append(f"{repo_n} cloned-repo")
            noun = "entry" if n_unverified == 1 else "entries"
            action += (
                f", {n_unverified} unverifiable {noun} failed open ({', '.join(kinds)})"
            )
        action += ")"
        return DeliverableGateResult(
            new_status, [action], False, preserves_human_wait=not n_unverified
        )

    missing = report.get("missing") or []
    if bounces < DELIVERABLE_GATE_BOUNCE_CAP:
        bounce_n = bounces + 1
        feedback, reason = _render_bounce_feedback(
            report, bounce_n, DELIVERABLE_GATE_BOUNCE_CAP
        )
        # Stamp BEFORE the resume queue flips the row: both are atomic
        # top-level merges on different keys, so ordering is only about
        # observability if the second write fails.
        await _stamp(
            {
                "passed": False,
                "checked_at": checked_at,
                "commit_sha": sha,
                "missing": missing,
                "present": report.get("present"),
                "unverified": report.get("unverified"),
                "reason": report.get("reason"),
                "bounces": bounce_n,
            }
        )
        try:
            queued = await queue_resume(job_id, feedback, reason=reason)
            # Existing callbacks conventionally return None on success. An
            # explicit False is the durable queue CAS saying that no resume
            # was installed; treating it as success would early-return from
            # completion while leaving the job in its pre-gate state.
            if queued is False:
                raise RuntimeError("resume queue declined the job")
        except Exception as e:  # noqa: BLE001
            # The bounce could not be queued — do NOT strand the job on a
            # refused-but-unbounced seal. Strict publication contracts end
            # blocked/undelivered; ordinary contracts retain the historical
            # fall-through with the failed-bounce report stamped.
            logger.error(
                "Deliverable gate: bounce queue failed for %s (%s) — "
                "sealing with the gate report instead",
                job_id,
                e,
            )
            if report.get("strict"):
                return DeliverableGateResult(
                    "cancelled",
                    [
                        f"deliverable gate: {len(missing)} missing, bounce "
                        "failed; terminalized blocked/undelivered"
                    ],
                    False,
                    BLOCKED_UNDELIVERED_OUTCOME,
                )
            return DeliverableGateResult(
                new_status,
                [
                    f"deliverable gate: {len(missing)} missing, bounce "
                    f"FAILED to queue — sealed with report"
                ],
                False,
            )
        logger.warning(
            "Deliverable gate BOUNCED job %s (bounce %d/%d): %d missing at %s",
            job_id,
            bounce_n,
            DELIVERABLE_GATE_BOUNCE_CAP,
            len(missing),
            sha or "HEAD",
        )
        return DeliverableGateResult(
            None,
            [
                f"deliverable gate: bounced to feedback-resume "
                f"({bounce_n}/{DELIVERABLE_GATE_BOUNCE_CAP}) — "
                f"{len(missing)} missing"
            ],
            True,
        )

    # Cap reached: stop bouncing. Strict publication contracts become a
    # truthful terminal blocker; ordinary contracts retain the established
    # human-review escalation.
    await _stamp(
        {
            "passed": False,
            "cap_reached": True,
            "checked_at": checked_at,
            "commit_sha": sha,
            "missing": missing,
            "present": report.get("present"),
            "unverified": report.get("unverified"),
            "bounces": bounces,
        }
    )
    outcome_kind = None
    final_status = new_status
    if report.get("strict"):
        final_status = "cancelled"
        outcome_kind = BLOCKED_UNDELIVERED_OUTCOME
    elif new_status == "completed":
        from orchestrator.services.project_loops import job_loop_id
        from orchestrator.services.verification_ledger import escalation_status

        final_status = escalation_status(is_loop_job=bool(job_loop_id(job)))
    action = (
        f"deliverable gate: bounce cap reached ({bounces}) with "
        f"{len(missing)} still missing — sealing as {final_status} with report"
    )
    logger.error("Deliverable gate CAP for job %s: %s", job_id, action)
    return DeliverableGateResult(final_status, [action], False, outcome_kind)
