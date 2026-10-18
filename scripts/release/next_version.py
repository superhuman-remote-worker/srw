#!/usr/bin/env python3
"""Decide the next SRW release version from the commits since the last release.

main.yml's ``resolve-version`` job runs this on every release run (a push to
``main`` or a dispatch on ``main``). The policy is docs/versioning.md:

* A release is a ``vX.Y.Z`` tag written by main.yml: annotated, with the
  message ``Release vX.Y.Z``. Pre-release tags (``v0.1.0-rc.1``) and other
  shapes are not releases and are ignored. If the highest ``vX.Y.Z`` tag was
  not written by the workflow, someone pushed it by hand: it has no chart and
  no GitHub Release, and counting from it would skip a version, so this
  refuses until it is deleted.
* Before 1.0, a release that contains an operator-action commit bumps MINOR;
  every other release bumps PATCH, features included. An operator-action
  commit has ``!`` after its type/scope (``feat(helm)!: ...``) or a
  ``BREAKING CHANGE:`` footer.
* From 1.0 on, ordinary SemVer: operator action bumps MAJOR, a ``feat``
  bumps MINOR, anything else bumps PATCH.
* An explicit version wins: ``--release-as`` (the workflow_dispatch input
  ``release_as``), else the newest ``Release-As: X.Y.Z`` footer among the
  commits since the last release. It must be plain ``X.Y.Z`` and strictly
  greater than the last release.

Usage (from the repository root)::

    python3 scripts/release/next_version.py [--release-as X.Y.Z] [--rev HEAD]

Prints ``key=value`` lines for ``$GITHUB_OUTPUT``: ``version``, ``tag``,
``previous_tag`` (empty for the first release), ``bump`` and ``title``. The
reasoning goes to stderr. Standard library only; release_notes.py reuses the
parsing below.
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import re
import subprocess
import sys
from typing import NamedTuple, Sequence

_NUMBER = r"(0|[1-9][0-9]*)"
_VERSION_RE = re.compile(rf"{_NUMBER}\.{_NUMBER}\.{_NUMBER}")
# Conventional Commits header: type, optional (scope), optional !, ": ".
_HEADER_RE = re.compile(
    r"(?P<type>[A-Za-z]+)(?:\((?P<scope>[^()\r\n]*)\))?(?P<bang>!)?:[ \t]+"
    r"(?P<description>\S.*)"
)
_BREAKING_RE = re.compile(r"BREAKING[ -]CHANGE:[ \t]*(?P<text>.*)")
_RELEASE_AS_RE = re.compile(r"(?i)Release-As:[ \t]*(?P<version>\S*)[ \t]*")
# Any git trailer line ("Token: value" or "Token #value"); it ends a footer.
_TRAILER_RE = re.compile(r"[A-Za-z][A-Za-z-]*(?::[ \t]| #)")

PUBLIC_BETA_SUFFIX = " — public beta"


class ReleaseError(Exception):
    """The release cannot be versioned as asked. The message says why and how."""


class Version(NamedTuple):
    major: int
    minor: int
    patch: int

    @classmethod
    def parse(cls, text: str) -> Version | None:
        match = _VERSION_RE.fullmatch(text)
        return cls(*(int(part) for part in match.groups())) if match else None

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"

    @property
    def tag(self) -> str:
        return f"v{self}"


@dataclass(frozen=True)
class Tag:
    """One ``refs/tags`` entry as ``git for-each-ref`` reports it."""

    name: str
    object_type: str  # "tag" for an annotated tag, "commit" for a lightweight one
    subject: str  # the annotation's first line; the commit's subject if lightweight

    @property
    def version(self) -> Version | None:
        return Version.parse(self.name[1:]) if self.name.startswith("v") else None

    @property
    def written_by_release_workflow(self) -> bool:
        return self.object_type == "tag" and self.subject == f"Release {self.name}"


@dataclass(frozen=True)
class Commit:
    sha: str
    message: str
    parents: tuple[str, ...] = ()

    @property
    def subject(self) -> str:
        return self.message.split("\n", 1)[0].strip()

    @property
    def _header(self) -> re.Match[str] | None:
        return _HEADER_RE.fullmatch(self.subject)

    @property
    def conventional(self) -> bool:
        return self._header is not None

    @property
    def type(self) -> str | None:
        header = self._header
        return header.group("type").lower() if header else None

    @property
    def scope(self) -> str | None:
        header = self._header
        if header is None:
            return None
        return (header.group("scope") or "").strip() or None

    @property
    def description(self) -> str:
        header = self._header
        return header.group("description").strip() if header else self.subject

    @property
    def is_merge(self) -> bool:
        return len(self.parents) > 1

    @property
    def body_lines(self) -> list[str]:
        return self.message.split("\n")[1:]

    @property
    def breaking_notes(self) -> list[str]:
        """The text of each ``BREAKING CHANGE:`` footer, continuation lines joined."""
        notes: list[list[str]] = []
        current: list[str] | None = None
        for line in self.body_lines:
            match = _BREAKING_RE.fullmatch(line.rstrip())
            if match:
                current = [match.group("text").strip()]
                notes.append(current)
            elif current is not None:
                if not line.strip() or _TRAILER_RE.match(line):
                    current = None
                else:
                    current.append(line.strip())
        return [" ".join(part for part in note if part) for note in notes]

    @property
    def breaking(self) -> bool:
        header = self._header
        bang = bool(header and header.group("bang"))
        return bang or any(
            _BREAKING_RE.fullmatch(line.rstrip()) for line in self.body_lines
        )

    @property
    def release_as(self) -> str | None:
        """The value of the last ``Release-As:`` footer, if the commit has one."""
        values = [
            match.group("version")
            for line in self.body_lines
            if (match := _RELEASE_AS_RE.fullmatch(line.rstrip()))
        ]
        return values[-1] if values else None


@dataclass(frozen=True)
class Decision:
    version: Version
    previous: Version | None
    bump: str  # "patch" | "minor" | "major" | "override"
    reasons: tuple[str, ...]

    @property
    def outputs(self) -> dict[str, str]:
        return {
            "version": str(self.version),
            "tag": self.version.tag,
            "previous_tag": self.previous.tag if self.previous else "",
            "bump": self.bump,
            "title": release_title(self.version),
        }


def release_title(version: Version) -> str:
    """``vX.Y.Z``; every 0.y release from 0.1 on is the public beta.

    v0.0.x is the alpha history. The beta label lives in the title (and the
    README badge), never in the version string: helm, Fleet and Renovate hide
    pre-release versions by default.
    """
    beta = version.major == 0 and version.minor >= 1
    return version.tag + (PUBLIC_BETA_SUFFIX if beta else "")


def last_release(tags: Sequence[Tag]) -> Version | None:
    """The highest ``vX.Y.Z`` release tag; pre-release and other tags never count."""
    versioned = [(tag.version, tag) for tag in tags if tag.version is not None]
    if not versioned:
        return None
    version, tag = max(versioned, key=lambda item: item[0])
    if not tag.written_by_release_workflow:
        found = "an annotated" if tag.object_type == "tag" else "a lightweight"
        raise ReleaseError(
            f"the highest release tag {tag.name} was not written by the release "
            f"workflow (expected an annotated tag with the message 'Release "
            f"{tag.name}', found {found} tag). A hand-pushed tag has no chart and "
            f"no GitHub Release, and counting from it would skip a version. "
            f"Delete it (git push --delete origin {tag.name}) and re-run."
        )
    return version


def parse_release_as(text: str, previous: Version | None, source: str) -> Version:
    version = Version.parse(text)
    if version is None:
        raise ReleaseError(
            f"{source} {text!r} is not a release version: give exactly X.Y.Z "
            f"(digits only, no leading 'v', no leading zeros, no pre-release suffix)"
        )
    floor = previous or Version(0, 0, 0)
    if version <= floor:
        raise ReleaseError(
            f"{source} {version} is not greater than the last release "
            f"{previous.tag if previous else '(none, so 0.0.0)'}"
        )
    return version


def bump(previous: Version | None, commits: Sequence[Commit]) -> tuple[Version, str]:
    base = previous or Version(0, 0, 0)
    operator_action = any(commit.breaking for commit in commits)
    if base.major == 0:
        if operator_action:
            return Version(0, base.minor + 1, 0), "minor"
        return Version(0, base.minor, base.patch + 1), "patch"
    if operator_action:
        return Version(base.major + 1, 0, 0), "major"
    if any(commit.type == "feat" for commit in commits):
        return Version(base.major, base.minor + 1, 0), "minor"
    return Version(base.major, base.minor, base.patch + 1), "patch"


def next_version(
    previous: Version | None,
    commits: Sequence[Commit],
    release_as: str | None = None,
) -> Decision:
    """Decide the next release.

    ``commits`` are the commits since ``previous``, newest first (git log order).
    """
    if release_as:
        version = parse_release_as(release_as, previous, "release_as")
        return Decision(version, previous, "override", ("release_as input",))

    for commit in commits:  # newest first: the newest footer wins
        footer = commit.release_as
        if footer is not None:
            version = parse_release_as(
                footer, previous, f"Release-As footer of {commit.sha[:7]}"
            )
            return Decision(
                version,
                previous,
                "override",
                (f"Release-As: {version} in {commit.sha[:7]} {commit.subject}",),
            )

    if not commits:
        raise ReleaseError(
            f"no commits since {previous.tag if previous else 'the beginning'}: "
            f"nothing to release. Dispatch with release_as to re-release this "
            f"commit under a new version on purpose."
        )

    version, kind = bump(previous, commits)
    reasons = tuple(
        f"operator action: {commit.sha[:7]} {commit.subject}"
        for commit in commits
        if commit.breaking
    )
    return Decision(version, previous, kind, reasons)


# --- git ------------------------------------------------------------------


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args], check=True, capture_output=True, text=True, encoding="utf-8"
    )
    return result.stdout


def read_tags() -> list[Tag]:
    out = _git(
        "for-each-ref",
        "refs/tags/v*",
        "--format=%(refname:strip=2)%1f%(objecttype)%1f%(contents:subject)",
    )
    tags = []
    for line in out.splitlines():
        if line:
            name, object_type, subject = (line.split("\x1f", 2) + ["", ""])[:3]
            tags.append(Tag(name, object_type, subject))
    return tags


def parse_log(text: str) -> list[Commit]:
    """Parse ``git log -z --format=%H%x1f%P%x1f%B`` output."""
    commits = []
    for record in text.split("\0"):
        record = record.lstrip("\n")
        if not record:
            continue
        sha, parents, message = (record.split("\x1f", 2) + ["", ""])[:3]
        commits.append(Commit(sha.strip(), message.strip("\n"), tuple(parents.split())))
    return commits


def read_commits(rev: str, since: Version | None) -> list[Commit]:
    """Commits reachable from ``rev`` and not from the ``since`` tag, newest first."""
    revision_range = f"{since.tag}..{rev}" if since else rev
    return parse_log(_git("log", "-z", "--format=%H%x1f%P%x1f%B", revision_range, "--"))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument(
        "--release-as",
        default="",
        help="exact X.Y.Z to release (the workflow_dispatch input); empty = derive",
    )
    parser.add_argument("--rev", default="HEAD", help="the commit being released")
    args = parser.parse_args(argv)

    try:
        previous = last_release(read_tags())
        commits = read_commits(args.rev, previous)
        decision = next_version(previous, commits, args.release_as.strip() or None)
    except ReleaseError as exc:
        print(f"::error title=Release version::{exc}", file=sys.stderr)
        return 1
    except subprocess.CalledProcessError as exc:
        print(
            f"::error title=Release version::git failed: {exc.stderr}", file=sys.stderr
        )
        return 1

    last = previous.tag if previous else "(no release yet)"
    print(
        f"Last release: {last}; {len(commits)} commit(s) since. "
        f"Bump: {decision.bump} -> {decision.version.tag}",
        file=sys.stderr,
    )
    for reason in decision.reasons:
        print(f"  {reason}", file=sys.stderr)
    for key, value in decision.outputs.items():
        print(f"{key}={value}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
