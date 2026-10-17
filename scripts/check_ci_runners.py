#!/usr/bin/env python3
"""Structural gate on which runners this repository's CI is allowed to use.

WHY THIS EXISTS
---------------
This is a PUBLIC repository whose CI runs on self-hosted runners inside the
homelab cluster. Anyone can open a pull request, and for a `pull_request` event
GitHub executes the workflow file *taken from that pull request*. If a job in it
names a self-hosted label, a stranger's code runs on our hardware.

So the rule is deliberately not "be careful when you edit runs-on". The rule is:
there are exactly three legal `runs-on` lines in this repository, and CI refuses
to go green if a fourth appears anywhere. A typo cannot produce a legal line. A
cleverly-equivalent rewrite cannot produce one either — that is the point, not a
limitation. Every routing decision has to be one of three strings a reviewer
recognises on sight, without evaluating an expression.

This is the MERGE-TIME half of the defence. The RUN-TIME half is
docker/ci-runner/job-started-guard.sh, wired up through
ACTIONS_RUNNER_HOOK_JOB_STARTED, which lives in the runner image and therefore
cannot be edited by a pull request at all. Neither half is sufficient alone:
this script cannot stop a fork PR that deletes it from RUNNING, and the hook
cannot stop a bad routing change from being MERGED. Check 8 below is what keeps
the two halves tied together.

See policy/ci_self_hosted_runners.md.

    python scripts/check_ci_runners.py     # 0 = policy holds, 1 = violated
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_DIR = REPO_ROOT / ".github" / "workflows"
RUNNER_DOC = REPO_ROOT / "policy" / "ci_self_hosted_runners.md"

# Strings the runner-image contract is recorded under. See check 8 for why this
# is a documentation check rather than a file check.
RUNNER_CONTRACT = (
    "job-started-guard.sh",
    "ARC_ALLOWED_REPOSITORY",
    "ghcr.io/knaeckebrothero/github-actions-runner",
)

# --- the three legal runs-on lines ------------------------------------------
# Compared after collapsing runs of whitespace, which cannot change what an
# expression means: reformatting is tolerated, rewording is not.
#
# The trailing `|| 'ubuntu-latest'` is load-bearing. Without it an unset
# CI_RUNNER_LABEL evaluates to '' and the job gets `runs-on: ''` — a config error
# that hangs the run. With it, `gh variable delete CI_RUNNER_LABEL` is a
# one-command kill-switch that fails safe back to GitHub-hosted runners, with no
# commit and no merge. That matters during an incident.
HOSTED = "runs-on: ubuntu-latest"
SELF_HOSTED = (
    "runs-on: ${{ github.event_name == 'pull_request'"
    " && 'ubuntu-latest' || vars.CI_RUNNER_LABEL || 'ubuntu-latest' }}"
)
SELF_HOSTED_KVM = (
    "runs-on: ${{ github.event_name == 'pull_request'"
    " && 'ubuntu-latest' || vars.CI_RUNNER_LABEL_KVM || 'ubuntu-latest' }}"
)
LEGAL_RUNS_ON = (HOSTED, SELF_HOSTED, SELF_HOSTED_KVM)

# The labels themselves are repository variables and must never be written into
# a workflow file. Check 4 already rejects any runs-on line naming one; catching
# them anywhere else (a strategy.matrix, a job output, a commented-out line
# somebody later uncomments) gives a much clearer error than "illegal runs-on".
FORBIDDEN_LABELS = ("srw-node4", "srw-node4-kvm", "self-hosted")

RUNS_ON_RE = re.compile(r"^\s*runs-on\s*:\s*(?P<value>.*?)\s*$")
# `pull_request_target` in YAML key position — i.e. actually used as a trigger,
# rather than merely named in a shell string. See check 2.
PR_TARGET_KEY_RE = re.compile(r"^\s*(?:-\s*)?['\"]?pull_request_target['\"]?\s*:")
# Conservative YAML comment strip: a line-leading `#`, or ` #` mid-line. Enough
# to let comments discuss self-hosted runners without tripping check 7, while
# still seeing a label that appears in an actual value. Check 4 backstops the
# only case that really matters.
COMMENT_RE = re.compile(r"(?:^\s*#.*$)|(?:\s+#.*$)")


@dataclass(frozen=True)
class Policy:
    """What a single workflow file is allowed to do."""

    triggers: frozenset[str]
    # Pinned to ubuntu-latest for every job, with no self-hosted option at all.
    hosted_only: bool = False
    why: str = ""


# Every file under .github/workflows/ must appear here. A new workflow that
# nobody registered fails the build rather than inheriting whatever routing its
# author happened to type. Routing is opt-in, never inherited.
WORKFLOWS: dict[str, Policy] = {
    "develop.yml": Policy(
        triggers=frozenset({"push", "pull_request", "workflow_dispatch"}),
    ),
    "main.yml": Policy(
        triggers=frozenset({"push", "pull_request", "workflow_dispatch"}),
    ),
    "db-migrations.yml": Policy(
        triggers=frozenset({"push", "pull_request", "workflow_dispatch"}),
    ),
    "stage1-rebuild.yml": Policy(
        triggers=frozenset({"schedule", "workflow_dispatch"}),
    ),
    "application-e2e.yml": Policy(
        triggers=frozenset({"push", "schedule", "workflow_dispatch"}),
        why=(
            "the full-stack observation builds an isolated k3d cluster and is "
            "restricted to trusted develop pushes and scheduled/manual events"
        ),
    ),
    "postgres-operand-check.yml": Policy(
        triggers=frozenset({"schedule", "workflow_dispatch"}),
        hosted_only=True,
        why=(
            "it only reads a values file and queries a public registry, so it "
            "has no reason to occupy a homelab runner"
        ),
    ),
    "mirror-nextcloud-images.yml": Policy(
        triggers=frozenset({"workflow_dispatch"}),
        hosted_only=True,
        why=(
            "it only copies two upstream images between public registries, on "
            "the owner's dispatch, so it has no reason to occupy a homelab runner"
        ),
    ),
    "docs.yml": Policy(
        triggers=frozenset({"push", "workflow_dispatch"}),
        hosted_only=True,
        why=(
            "it builds a static site and deploys it to GitHub Pages, so it has "
            "no reason to occupy a homelab runner"
        ),
    ),
    "ci-policy.yml": Policy(
        triggers=frozenset({"push", "pull_request", "workflow_dispatch"}),
        hosted_only=True,
        why=(
            "this workflow is what verifies nothing else is misrouted, so it can "
            "never be allowed to run on the machines it is protecting"
        ),
    ),
}
# NOTE: there is no ci-runner-image.yml entry. The runner image is built in
# Scripts-and-Notebooks (devops/github-actions-runner/), not here — see check 8.


class _StrictLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate mapping keys.

    PyYAML silently keeps the last duplicate. That is a parser differential we
    do not want: a file could read one way to this script and another to GitHub.
    Refuse the file instead of picking a winner.
    """


def _no_duplicate_keys(loader, node, deep=False):
    mapping = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_StrictLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _no_duplicate_keys
)


def _triggers(doc: dict) -> set[str]:
    """The `on:` keys.

    PyYAML follows YAML 1.1, where a bare `on:` is the BOOLEAN True, not the
    string "on". GitHub reads it as the string. Accept both so the check cannot
    be sidestepped by quoting.
    """
    node = doc.get("on", doc.get(True))
    if isinstance(node, dict):
        return set(node)
    if isinstance(node, list):
        return set(node)
    if isinstance(node, str):
        return {node}
    raise ValueError(f"unrecognised `on:` block: {node!r}")


@dataclass
class Report:
    errors: list[str] = field(default_factory=list)

    def add(self, msg: str) -> None:
        self.errors.append(msg)


def check() -> list[str]:
    """Return a list of policy violations; empty means the policy holds."""
    r = Report()

    if not WORKFLOW_DIR.is_dir():
        return [f"{WORKFLOW_DIR} does not exist — nothing to audit"]

    on_disk = {
        p.name
        for p in WORKFLOW_DIR.iterdir()
        if p.is_file() and p.suffix in {".yml", ".yaml"}
    }

    # 1. Registry <-> filesystem parity, both directions.
    for name in sorted(on_disk - WORKFLOWS.keys()):
        r.add(
            f"{name}: new workflow is not registered in scripts/check_ci_runners.py. "
            f"Add a Policy entry declaring its triggers and whether it may use "
            f"self-hosted runners. Routing is opt-in, never inherited."
        )
    for name in sorted(WORKFLOWS.keys() - on_disk):
        r.add(
            f"{name}: registered in scripts/check_ci_runners.py but missing from "
            f".github/workflows/. If it was deliberately removed, remove its Policy "
            f"entry in the same commit; ci-policy.yml in particular must never "
            f"disappear quietly."
        )

    for name in sorted(on_disk & WORKFLOWS.keys()):
        policy = WORKFLOWS[name]
        raw = (WORKFLOW_DIR / name).read_text(encoding="utf-8")
        lines = raw.splitlines()

        try:
            doc = yaml.load(raw, Loader=_StrictLoader)
        except yaml.YAMLError as exc:
            r.add(f"{name}: will not parse ({exc}); refusing to audit it")
            continue

        # 2. pull_request_target is banned outright. It runs with the BASE repo's
        #    token and secrets against a fork's code. This repository has never
        #    used it and must not start.
        #
        #    Anchored to key position rather than "anywhere in the file": the
        #    runner-image workflow has to name the event as a string in order to
        #    PROVE the fork guard refuses it, and a check that forbids testing
        #    the thing it forbids is a check people delete. Check 3 below is the
        #    semantic backstop — pull_request_target is in no Policy's trigger
        #    allowlist, so it fails there too even if written in a form this
        #    regex misses.
        for lineno, line in enumerate(lines, start=1):
            if PR_TARGET_KEY_RE.match(line):
                r.add(
                    f"{name}:{lineno}: pull_request_target is forbidden "
                    f"repository-wide. Use pull_request."
                )

        # 3. Trigger allowlist. A new event type must be reviewed against the
        #    routing rule before it inherits it — merge_group and workflow_run in
        #    particular have different trust properties from push.
        try:
            actual = _triggers(doc)
        except ValueError as exc:
            r.add(f"{name}: {exc}")
            actual = set()
        for extra in sorted(actual - policy.triggers):
            r.add(
                f"{name}: trigger '{extra}' is not in this workflow's allowlist "
                f"{sorted(policy.triggers)}. Decide how it should be routed, then "
                f"add it to the Policy entry."
            )

        # 4. Every runs-on line, byte for byte (whitespace-normalised), against
        #    the allowlist. Read from the RAW SOURCE rather than the parsed
        #    document so no YAML feature — anchors, quoting, tags — can present
        #    one value to this script and another to GitHub.
        allowed = (HOSTED,) if policy.hosted_only else LEGAL_RUNS_ON
        seen = 0
        for lineno, line in enumerate(lines, start=1):
            m = RUNS_ON_RE.match(line)
            if not m:
                continue
            seen += 1
            value = " ".join(m.group("value").split())
            if not value or value[0] in "|>&*":
                r.add(
                    f"{name}:{lineno}: runs-on must be a single plain scalar on "
                    f"one line (no block scalars, anchors, or mapping form). "
                    f"Got: {line.strip()!r}"
                )
                continue
            normalised = f"runs-on: {value}"
            if normalised not in allowed:
                hint = f" ({policy.why})" if policy.hosted_only and policy.why else ""
                r.add(
                    f"{name}:{lineno}: illegal runs-on{hint}.\n"
                    f"    got:      {normalised}\n"
                    + "".join(f"    expected: {a}\n" for a in allowed)
                    + "    Copy one of the expected lines exactly. Do not write an "
                    "equivalent expression — only these strings are legal, so that "
                    "reviewing a routing change never requires evaluating one."
                )

        # 5. Parity: one runs-on line per job. Catches a runs-on in flow style,
        #    nested somewhere the line scanner cannot see, or a job with none.
        jobs = doc.get("jobs") or {}
        if seen != len(jobs):
            r.add(
                f"{name}: found {seen} `runs-on:` line(s) but {len(jobs)} job(s). "
                f"Every job needs exactly one runs-on on its own line, and nothing "
                f"else in the file may contain the text 'runs-on:'."
            )

        # 6. No reusable workflows. A job-level `uses:` moves runs-on into
        #    another file, out from under check 4.
        for job_id, job in jobs.items():
            if isinstance(job, dict) and "uses" in job:
                r.add(
                    f"{name}: job '{job_id}' calls a reusable workflow. Its runs-on "
                    f"would escape this audit. Inline it, or extend this script to "
                    f"follow the reference first."
                )

        # 7. Runner labels must not appear as literals in a value anywhere.
        #    Comments may discuss them — the routing needs explaining.
        for lineno, line in enumerate(lines, start=1):
            code = COMMENT_RE.sub("", line)
            if RUNS_ON_RE.match(code):
                continue  # check 4 owns this line
            for label in FORBIDDEN_LABELS:
                if label in code:
                    r.add(
                        f"{name}:{lineno}: runner label {label!r} must never be "
                        f"written into a workflow file. Routing goes through "
                        f"vars.CI_RUNNER_LABEL / vars.CI_RUNNER_LABEL_KVM in one of "
                        f"the canonical runs-on lines and nowhere else."
                    )

    # 8. The run-time half must stay documented.
    #
    #    This check used to assert that docker/Dockerfile.ci-runner and
    #    docker/ci-runner/job-started-guard.sh existed here and still contained
    #    their fork checks. The image now lives in Scripts-and-Notebooks
    #    (devops/github-actions-runner/) — deliberately, because the guard is the
    #    one control a pull request cannot edit, and that is only true while it
    #    is outside every repository it protects. A file check is therefore no
    #    longer possible from here.
    #
    #    So this is weaker than what it replaced, and worth being honest about:
    #    it verifies the CONTRACT IS STILL WRITTEN DOWN, not that it is still
    #    enforced. The enforcement proof moved with the image and is stronger
    #    there — the runner-image workflow runs 13 accept/refuse cases against
    #    the built image and gates `docker push` on them, so a regressed guard
    #    cannot reach the registry at all.
    #
    #    What this still buys: the pointer cannot silently vanish. Someone
    #    removing the self-hosted runner story from this repo has to delete the
    #    documented contract too, and CI notices.
    if not RUNNER_DOC.is_file():
        r.add(
            "policy/ci_self_hosted_runners.md is missing — it records the runner "
            "image contract (the fork guard, its allowlist variable, and where "
            "the image is built). It is the only trace of that contract in this "
            "repository."
        )
    else:
        doc = RUNNER_DOC.read_text(encoding="utf-8")
        for needle in RUNNER_CONTRACT:
            if needle not in doc:
                r.add(
                    f"policy/ci_self_hosted_runners.md no longer mentions {needle!r} "
                    f"— the runner-image contract has drifted. If the image moved "
                    f"or the guard changed, update the doc and RUNNER_CONTRACT in "
                    f"this script together."
                )

    return r.errors


def main() -> int:
    errors = check()
    if errors:
        plural = "s" if len(errors) != 1 else ""
        print(
            f"CI runner-routing policy violated ({len(errors)} problem{plural}):\n",
            file=sys.stderr,
        )
        for err in errors:
            print(f"  - {err}", file=sys.stderr)
        print(
            "\nBackground: policy/ci_self_hosted_runners.md. This repository is "
            "public and its CI runs on self-hosted runners; a pull_request routed "
            "to one executes a stranger's code inside the cluster.",
            file=sys.stderr,
        )
        return 1
    print(f"CI runner-routing policy holds across {len(WORKFLOWS)} workflow file(s).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
