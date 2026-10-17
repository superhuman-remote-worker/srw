#!/usr/bin/env python3
"""Render the GitHub Release notes for one SRW release.

main.yml's ``resolve-version`` job renders them before anything is built, so a
broken highlights file fails the run before an image is pushed; the
``github-release`` job publishes the result after the tag exists. The same
conventional-commit parser decides the version (next_version.py), so the
"Operator action required" list is exactly the set of commits that drove a
MINOR bump.

Layout, top to bottom:

1. The hand-written highlights in ``docs/release-notes/<tag>.md``, if that file
   exists (its YAML frontmatter and ``#`` title are dropped; the rest is used
   verbatim).
2. Operator action required: every commit marked ``!`` or ``BREAKING CHANGE:``,
   with its footer text.
3. Features (``feat``), then Fixes (``fix``, ``perf``, ``revert``).
   ``test``, ``style``, ``chore``, ``ci``, ``docs``, ``refactor``, ``build``,
   unprefixed commits and merges are counted, not listed.
4. The chart reference, the upgrade command and the full-changelog link.

Usage (from the repository root)::

    python3 scripts/release/release_notes.py --tag v0.1.0 --previous-tag v0.0.30 \\
        [--rev HEAD] [--highlights PATH] [--output FILE]
"""

from __future__ import annotations

import argparse
import importlib.util
from pathlib import Path
import subprocess
import sys
from typing import Sequence

# PYTHONSAFEPATH (set in CI) keeps this directory off sys.path, so load the
# sibling by path instead of importing it by name.
_SPEC = importlib.util.spec_from_file_location(
    "srw_release_next_version", Path(__file__).with_name("next_version.py")
)
assert _SPEC and _SPEC.loader
nv = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = nv  # dataclasses resolve annotations through sys.modules
_SPEC.loader.exec_module(nv)

CHART = "oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker"
REPO_URL = "https://github.com/superhuman-remote-worker/srw"
HIGHLIGHTS_DIR = Path("docs/release-notes")
FEATURE_TYPES = frozenset({"feat"})
FIX_TYPES = frozenset({"fix", "perf", "revert"})
# GitHub rejects a release body above 125,000 characters. Failing here, before
# any image is pushed, beats failing after the tag exists.
MAX_BODY = 125_000


def load_highlights(path: Path) -> str | None:
    """The body of a highlights page: frontmatter and the ``#`` title dropped."""
    if not path.is_file():
        return None
    lines = path.read_text(encoding="utf-8").replace("\r\n", "\n").split("\n")
    if lines and lines[0].strip() == "---":
        closing = [i for i in range(1, len(lines)) if lines[i].strip() == "---"]
        if not closing:
            raise nv.ReleaseError(f"{path}: frontmatter is opened but never closed")
        lines = lines[closing[0] + 1 :]
    while lines and not lines[0].strip():
        lines.pop(0)
    if lines and lines[0].startswith("# "):
        lines.pop(0)
    body = "\n".join(lines).strip()
    return body or None


def _entry(commit: nv.Commit) -> str:
    scope = f"**{commit.scope}:** " if commit.scope else ""
    lines = [f"- {scope}{commit.description} ({commit.sha[:7]})"]
    lines += [f"  - {note}" for note in commit.breaking_notes if note]
    return "\n".join(lines)


def _unique(commits: Sequence[nv.Commit]) -> list[nv.Commit]:
    """Drop repeats of one change (a cherry-pick, a re-landed squash)."""
    seen: set[tuple[str | None, str | None, str]] = set()
    kept = []
    for commit in commits:
        key = (commit.type, commit.scope, commit.description)
        if key not in seen:
            seen.add(key)
            kept.append(commit)
    return kept


def render_notes(
    *,
    tag: str,
    previous_tag: str,
    commits: Sequence[nv.Commit],
    highlights: str | None = None,
    repo_url: str = REPO_URL,
    chart: str = CHART,
) -> str:
    """Release notes for ``tag``. ``commits`` are newest first, as git logs them."""
    version = tag.removeprefix("v")
    ordered = list(reversed(commits))  # read in the order things landed
    action = _unique([c for c in ordered if c.breaking])
    features = _unique(
        [c for c in ordered if not c.breaking and c.type in FEATURE_TYPES]
    )
    fixes = _unique([c for c in ordered if not c.breaking and c.type in FIX_TYPES])
    listed = {c.sha for c in action + features + fixes}
    unlisted = sum(1 for c in ordered if c.sha not in listed and not c.is_merge)

    if previous_tag:
        changelog = f"{repo_url}/compare/{previous_tag}...{tag}"
    else:
        changelog = f"{repo_url}/commits/{tag}"
    out: list[str] = []
    if highlights:
        out += [highlights, ""]

    out += ["## Operator action required", ""]
    if action:
        out += [
            "Each change below needs something from you when you upgrade: a values "
            "change, a new Secret key, a non-rolling migration, a removed API or a "
            "changed default. Read them all before running `helm upgrade`.",
            "",
            *(_entry(c) for c in action),
            "",
        ]
    else:
        out += ["No commit in this release is marked as needing operator action.", ""]

    for heading, group in (("Features", features), ("Fixes", fixes)):
        if group:
            out += [f"## {heading}", "", *(_entry(c) for c in group), ""]

    if unlisted:
        noun = "commit" if unlisted == 1 else "commits"
        out += [
            f"{unlisted} more {noun} (tests, docs, CI, refactoring and chores) "
            f"are in the [full changelog]({changelog}).",
            "",
        ]

    out += [
        "## Install or upgrade",
        "",
        f"Helm chart `{chart}`, version `{version}`. It pins every component "
        "image to this release's commit.",
        "",
        "```bash",
        f"helm upgrade srw {chart} \\",
        f"  --version {version} -n srw -f my-values.yaml",
        "```",
        "",
        f"New install: [chart README]({repo_url}/blob/{tag}/helm/README.md). "
        f"Before upgrading, read [Versioning and upgrades]"
        f"({repo_url}/blob/{tag}/docs/versioning.md) and the operator actions "
        "above.",
        "",
        f"**Full changelog:** {changelog}",
    ]
    body = "\n".join(out).rstrip() + "\n"
    if len(body) > MAX_BODY:
        raise nv.ReleaseError(
            f"release notes are {len(body)} characters; GitHub accepts at most "
            f"{MAX_BODY}. Shorten docs/release-notes/{tag}.md."
        )
    return body


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--tag", required=True, help="the release tag, vX.Y.Z")
    parser.add_argument(
        "--previous-tag", default="", help="the last release tag; empty for none"
    )
    parser.add_argument("--rev", default="HEAD", help="the commit being released")
    parser.add_argument(
        "--highlights",
        type=Path,
        help="hand-written highlights (default: docs/release-notes/<tag>.md)",
    )
    parser.add_argument("--repo-url", default=REPO_URL)
    parser.add_argument("--chart", default=CHART)
    parser.add_argument("--output", type=Path, help="write here instead of stdout")
    args = parser.parse_args(argv)

    try:
        version = nv.Version.parse(args.tag.removeprefix("v"))
        if version is None or not args.tag.startswith("v"):
            raise nv.ReleaseError(f"--tag {args.tag!r} is not vX.Y.Z")
        previous = None
        if args.previous_tag:
            previous = nv.Version.parse(args.previous_tag.removeprefix("v"))
            if previous is None:
                raise nv.ReleaseError(
                    f"--previous-tag {args.previous_tag!r} is not vX.Y.Z"
                )
        highlights_path = args.highlights or HIGHLIGHTS_DIR / f"{args.tag}.md"
        notes = render_notes(
            tag=args.tag,
            previous_tag=args.previous_tag,
            commits=nv.read_commits(args.rev, previous),
            highlights=load_highlights(highlights_path),
            repo_url=args.repo_url.rstrip("/"),
            chart=args.chart,
        )
    except nv.ReleaseError as exc:
        print(f"::error title=Release notes::{exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(f"::error title=Release notes::git failed: {exc.stderr}", file=sys.stderr)
        return 1

    if args.output:
        args.output.write_text(notes, encoding="utf-8")
        found = "with" if highlights_path.is_file() else "without"
        print(
            f"Wrote {args.output} ({len(notes)} characters, {found} {highlights_path})",
            file=sys.stderr,
        )
    else:
        sys.stdout.write(notes)
    return 0


if __name__ == "__main__":
    sys.exit(main())
