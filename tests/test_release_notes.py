"""GitHub Release notes: operator actions first, noise dropped, highlights on top."""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "release" / "release_notes.py"
SPEC = importlib.util.spec_from_file_location("release_notes", SCRIPT)
assert SPEC and SPEC.loader
notes = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = notes
SPEC.loader.exec_module(notes)
nv = notes.nv  # the parser the version decision uses


def commit(message: str, n: int, parents: int = 1) -> nv.Commit:
    return nv.Commit(
        f"{n:07d}" + "0" * 33, message, tuple("p" * 40 for _ in range(parents))
    )


# git log order: newest first.
HISTORY = [
    commit("Merge pull request #150 from org/develop", 9, parents=2),
    commit("test(cockpit): cover the button", 8),
    commit(
        "feat(helm)!: rename vm.mode to vm.tier\n\nBREAKING CHANGE: set vm.tier; vm.mode is gone",
        7,
    ),
    commit("ci: cache harder", 6),
    commit("fix(db): close a leaked connection", 5),
    commit("perf(search): one query instead of three", 4),
    commit("feat(sessions): resume from any device", 3),
    commit("chore: bump a pin", 2),
    commit("feat(sessions): resume from any device", 1),  # cherry-picked twice
    commit("docs: explain upgrades", 0),
]


def render(**kwargs) -> str:
    defaults = dict(tag="v0.1.0", previous_tag="v0.0.30", commits=HISTORY)
    return notes.render_notes(**(defaults | kwargs))


def section(body: str, heading: str) -> str:
    start = body.index(f"## {heading}\n")
    end = body.find("\n## ", start + 1)
    return body[start : end if end != -1 else None]


def test_sections_come_in_order_with_operator_action_first():
    body = render()
    order = [
        body.index(f"## {h}\n")
        for h in ("Operator action required", "Features", "Fixes", "Install or upgrade")
    ]
    assert order == sorted(order)


def test_operator_action_lists_the_marked_commit_with_its_footer():
    action = section(render(), "Operator action required")
    assert "- **helm:** rename vm.mode to vm.tier (0000007)" in action
    assert "  - set vm.tier; vm.mode is gone" in action
    # Listed once, under operator action, never again as a feature.
    assert render().count("rename vm.mode") == 1


def test_features_and_fixes_drop_the_noise():
    body = render()
    assert section(body, "Features").count("resume from any device") == 1
    fixes = section(body, "Fixes")
    assert "**db:** close a leaked connection (0000005)" in fixes
    assert "**search:** one query instead of three" in fixes
    for noise in (
        "cover the button",
        "cache harder",
        "bump a pin",
        "explain upgrades",
        "Merge pull request",
    ):
        assert noise not in body
    # chore, ci, docs, test and the duplicate feature count; the merge does not.
    assert "5 more commits (tests, docs, CI, refactoring and chores)" in body


def test_entries_read_in_the_order_they_landed():
    history = [commit("fix: second", 2), commit("fix: first", 1)]
    fixes = section(render(commits=history), "Fixes")
    assert fixes.index("first") < fixes.index("second")


def test_no_operator_action_says_so():
    body = render(commits=[commit("fix: a", 1)])
    assert "No commit in this release is marked as needing operator action." in body
    assert "## Features" not in body


def test_install_pointer_and_changelog():
    body = render()
    chart = "oci://ghcr.io/superhuman-remote-worker/charts/superhuman-remote-worker"
    assert (
        f"helm upgrade srw {chart} \\\n  --version 0.1.0 -n srw -f my-values.yaml"
        in body
    )
    assert "/blob/v0.1.0/docs/versioning.md" in body
    assert "/blob/v0.1.0/helm/README.md" in body
    assert body.rstrip().endswith(
        "**Full changelog:** https://github.com/superhuman-remote-worker/srw/compare/v0.0.30...v0.1.0"
    )


def test_first_release_links_the_history_instead_of_a_compare():
    body = render(previous_tag="")
    assert "srw/commits/v0.1.0" in body and "/compare/" not in body


def test_highlights_page_goes_first_without_frontmatter_or_title(tmp_path):
    page = tmp_path / "v0.1.0.md"
    page.write_text(
        "---\ntitle: v0.1.0 release notes\ndescription: The first public release.\n---\n\n"
        "# v0.1.0 release notes\n\n## Highlights\n\nSRW is in public beta.\n"
    )
    highlights = notes.load_highlights(page)
    assert highlights == "## Highlights\n\nSRW is in public beta."
    body = render(highlights=highlights)
    assert body.startswith(
        "## Highlights\n\nSRW is in public beta.\n\n## Operator action required"
    )


def test_missing_or_empty_highlights_are_none(tmp_path):
    assert notes.load_highlights(tmp_path / "absent.md") is None
    empty = tmp_path / "empty.md"
    empty.write_text("---\ntitle: x\ndescription: y\n---\n# x\n\n")
    assert notes.load_highlights(empty) is None


def test_unclosed_frontmatter_fails_before_anything_is_published(tmp_path):
    page = tmp_path / "v0.1.0.md"
    page.write_text("---\ntitle: oops\n\n# v0.1.0\n")
    with pytest.raises(nv.ReleaseError, match="never closed"):
        notes.load_highlights(page)


def test_oversized_notes_fail_early():
    with pytest.raises(nv.ReleaseError, match="GitHub accepts at most"):
        render(highlights="x" * notes.MAX_BODY)


def test_cli_reads_the_default_highlights_path_from_the_repository(tmp_path):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env |= {
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@example.com",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@example.com",
    }

    def git(*args):
        subprocess.run(
            ["git", *args], cwd=tmp_path, env=env, check=True, capture_output=True
        )

    git("init", "-q", "-b", "main")
    git("commit", "-q", "--allow-empty", "-m", "fix: before")
    git("tag", "-a", "v0.0.30", "-m", "Release v0.0.30")
    git("commit", "-q", "--allow-empty", "-m", "feat(chart)!: a values key moved")
    pages = tmp_path / "docs" / "release-notes"
    pages.mkdir(parents=True)
    (pages / "v0.1.0.md").write_text(
        "---\ntitle: t\ndescription: d\n---\n\n# t\n\nHello operators.\n"
    )
    out = tmp_path / "notes.md"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--tag",
            "v0.1.0",
            "--previous-tag",
            "v0.0.30",
            "--output",
            str(out),
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    body = out.read_text()
    assert body.startswith("Hello operators.\n")
    assert "- **chart:** a values key moved" in section(
        body, "Operator action required"
    )
    # The fix released in v0.0.30 is not in this range.
    assert "## Fixes" not in body


@pytest.mark.parametrize(
    "args", [["--tag", "0.1.0"], ["--tag", "v0.1.0", "--previous-tag", "latest"]]
)
def test_cli_refuses_malformed_tags(args, tmp_path):
    result = subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1 and "::error title=Release notes::" in result.stderr
