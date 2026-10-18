"""The release version is derived from conventional commits, and fails loudly.

Policy: docs/versioning.md. The script is scripts/release/next_version.py; the
end-to-end tests run the exact resolve-version shell from main.yml against a
scratch repository with an ``origin`` to fetch tags from.
"""

from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "release" / "next_version.py"
SPEC = importlib.util.spec_from_file_location("release_next_version", SCRIPT)
assert SPEC and SPEC.loader
nv = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = nv  # dataclasses resolve annotations through sys.modules
SPEC.loader.exec_module(nv)

V = nv.Version


def commit(message: str, sha: str = "a" * 40, parents: int = 1) -> nv.Commit:
    return nv.Commit(sha, message, tuple("p" * 40 for _ in range(parents)))


def release_tag(name: str) -> nv.Tag:
    return nv.Tag(name, "tag", f"Release {name}")


FIX = commit("fix(cockpit): a button works again")
FEAT = commit("feat(sessions): resume from any device")
CHORE = commit("chore: bump a pin")


# --- finding the last release ----------------------------------------------


def test_no_tags_means_no_previous_release():
    assert nv.last_release([]) is None
    assert nv.last_release([nv.Tag("latest", "commit", "x")]) is None


def test_highest_release_is_numeric_not_lexical():
    tags = [release_tag(f"v0.0.{n}") for n in (9, 10, 29, 30, 3)]
    assert nv.last_release(tags) == V(0, 0, 30)


@pytest.mark.parametrize("name", ["v0.1.0-rc.1", "v0.1.0-beta", "v0.1", "v1", "0.2.0"])
def test_pre_release_and_odd_tags_are_never_the_last_release(name):
    # git's default version sort puts v0.1.0-rc.1 above v0.1.0; this must not.
    tags = [release_tag("v0.0.30"), nv.Tag(name, "commit", "hand-pushed")]
    assert nv.last_release(tags) == V(0, 0, 30)


@pytest.mark.parametrize(
    "tag",
    [
        nv.Tag("v0.1.0", "commit", "feat: something"),  # lightweight
        nv.Tag("v0.1.0", "tag", "public beta!"),  # annotated by hand
    ],
)
def test_a_hand_pushed_highest_tag_is_refused(tag):
    with pytest.raises(nv.ReleaseError, match="git push --delete origin v0.1.0"):
        nv.last_release([release_tag("v0.0.30"), tag])


def test_a_hand_pushed_older_tag_does_not_matter():
    tags = [nv.Tag("v0.0.5", "commit", "x"), release_tag("v0.0.30")]
    assert nv.last_release(tags) == V(0, 0, 30)


# --- commit classification --------------------------------------------------


@pytest.mark.parametrize(
    ("message", "breaking"),
    [
        ("feat!: drop the v1 API", True),
        ("feat(helm)!: rename vm.mode to vm.tier", True),
        ("fix(db)!: migration 0400 needs a maintenance window", True),
        ("feat(helm): add vm.tier\n\nBREAKING CHANGE: vm.mode is removed", True),
        ("feat: x\n\nBREAKING-CHANGE: the hyphenated token counts too", True),
        ("feat: mention BREAKING CHANGE: in prose only", False),
        ("feat: x\n\n  BREAKING CHANGE: indented is prose", False),
        ("feat: x\n\nbreaking change: lower case is not the token", False),
        ("feat(helm): add vm.tier", False),
        ("Merge pull request #144 from org/develop", False),
    ],
)
def test_operator_action_markers(message, breaking):
    assert commit(message).breaking is breaking


def test_conventional_header_parts():
    c = commit("feat(helm)!: rename vm.mode\n\nbody")
    assert (c.type, c.scope, c.description) == ("feat", "helm", "rename vm.mode")
    plain = commit("Require proven checkpoint cleanup")
    assert (plain.conventional, plain.type, plain.scope) == (False, None, None)
    assert plain.description == "Require proven checkpoint cleanup"


def test_breaking_footer_text_stops_at_the_next_trailer():
    c = commit(
        "feat(helm): add vm.tier\n\n"
        "Body text.\n\n"
        "BREAKING CHANGE: vm.mode is removed;\n"
        "set vm.tier instead.\n"
        "Co-Authored-By: Someone <s@example.com>"
    )
    assert c.breaking_notes == ["vm.mode is removed; set vm.tier instead."]


# --- the bump ---------------------------------------------------------------


def test_no_tags_first_release_is_a_patch():
    decision = nv.next_version(None, [FIX])
    assert (str(decision.version), decision.bump) == ("0.0.1", "patch")
    assert decision.outputs["previous_tag"] == ""


def test_no_tags_with_operator_action_is_0_1_0():
    decision = nv.next_version(None, [commit("feat!: x")])
    assert str(decision.version) == "0.1.0"


@pytest.mark.parametrize("commits", [[FIX], [FEAT], [CHORE], [FEAT, FIX, CHORE]])
def test_before_1_0_anything_without_operator_action_is_a_patch(commits):
    decision = nv.next_version(V(0, 0, 30), commits)
    assert (str(decision.version), decision.bump) == ("0.0.31", "patch")


@pytest.mark.parametrize(
    "marker",
    [
        "feat!: drop the legacy session path",
        "feat(helm)!: rename vm.mode to vm.tier",
        "fix: x\n\nBREAKING CHANGE: a new required Secret key",
    ],
)
def test_before_1_0_operator_action_is_a_minor(marker):
    decision = nv.next_version(V(0, 1, 4), [FIX, commit(marker), FEAT])
    assert (str(decision.version), decision.bump) == ("0.2.0", "minor")
    assert len(decision.reasons) == 1 and "operator action" in decision.reasons[0]


@pytest.mark.parametrize(
    ("commits", "expected"),
    [([FIX], "1.2.4"), ([FIX, FEAT], "1.3.0"), ([commit("fix!: x"), FEAT], "2.0.0")],
)
def test_from_1_0_on_ordinary_semver(commits, expected):
    assert str(nv.next_version(V(1, 2, 3), commits).version) == expected


def test_nothing_to_release_fails_loudly():
    with pytest.raises(nv.ReleaseError, match="nothing to release"):
        nv.next_version(V(0, 0, 30), [])


# --- explicit versions -------------------------------------------------------


def test_override_wins_over_the_commits():
    decision = nv.next_version(V(0, 0, 30), [commit("feat!: x")], release_as="0.1.0")
    assert (str(decision.version), decision.bump) == ("0.1.0", "override")


def test_override_may_re_release_a_commit_on_purpose():
    assert str(nv.next_version(V(0, 0, 31), [], release_as="0.1.0").version) == "0.1.0"


@pytest.mark.parametrize("value", ["0.0.30", "0.0.29", "0.0.0"])
def test_override_not_greater_than_the_last_release_fails(value):
    with pytest.raises(nv.ReleaseError, match="not greater than the last release"):
        nv.next_version(V(0, 0, 30), [FIX], release_as=value)


@pytest.mark.parametrize(
    "value", ["v0.1.0", "0.1", "0.1.0-rc.1", "0.1.0+build", "01.0.0", "0.1.0 ", "x"]
)
def test_override_must_be_plain_x_y_z(value):
    with pytest.raises(nv.ReleaseError, match="exactly X.Y.Z"):
        nv.next_version(V(0, 0, 30), [FIX], release_as=value)


def test_release_as_footer_sets_the_version_newest_first():
    commits = [  # git log order: newest first
        commit("chore(release): cut v0.1.0\n\nRelease-As: 0.1.0", sha="b" * 40),
        commit("chore: older\n\nRelease-As: 0.0.99", sha="c" * 40),
        FIX,
    ]
    decision = nv.next_version(V(0, 0, 30), commits)
    assert (str(decision.version), decision.bump) == ("0.1.0", "override")
    assert "bbbbbbb" in decision.reasons[0]


def test_release_as_input_beats_the_footer():
    commits = [commit("chore: x\n\nRelease-As: 0.2.0")]
    assert str(nv.next_version(V(0, 0, 30), commits, "0.1.0").version) == "0.1.0"


def test_an_invalid_release_as_footer_fails_loudly():
    with pytest.raises(nv.ReleaseError, match="Release-As footer of"):
        nv.next_version(V(0, 1, 0), [commit("chore: x\n\nRelease-As: 0.1.0")])


# --- titles -----------------------------------------------------------------


@pytest.mark.parametrize(
    ("version", "title"),
    [
        (V(0, 0, 31), "v0.0.31"),
        (V(0, 1, 0), "v0.1.0 — public beta"),
        (V(0, 1, 7), "v0.1.7 — public beta"),
        (V(1, 0, 0), "v1.0.0"),
    ],
)
def test_release_title(version, title):
    assert nv.release_title(version) == title


# --- against a real repository ------------------------------------------------


def git_env() -> dict[str, str]:
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("GIT_") and k != "GITHUB_OUTPUT"
    }
    env.update(
        GIT_CONFIG_GLOBAL=os.devnull,
        GIT_CONFIG_NOSYSTEM="1",
        GIT_AUTHOR_NAME="Release Test",
        GIT_AUTHOR_EMAIL="release-test@example.com",
        GIT_COMMITTER_NAME="github-actions[bot]",
        GIT_COMMITTER_EMAIL="41898282+github-actions[bot]@users.noreply.github.com",
        GIT_TERMINAL_PROMPT="0",
    )
    return env


def git(cwd: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        env=git_env(),
        check=True,
        capture_output=True,
        text=True,
    ).stdout


@pytest.fixture
def released(tmp_path):
    """A clone of an origin with v0.0.29 and v0.0.30 as workflow releases."""
    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "main")
    for n, message in enumerate(("feat: first", "fix: second")):
        (work / "f.txt").write_text(str(n))
        git(work, "add", "f.txt")
        git(work, "commit", "-q", "-m", message)
        tag = f"v0.0.{29 + n}"
        git(work, "tag", "-a", tag, "-m", f"Release {tag}")
    origin = tmp_path / "origin.git"
    git(tmp_path, "clone", "-q", "--bare", str(work), str(origin))
    clone = tmp_path / "clone"
    git(tmp_path, "clone", "-q", str(origin), str(clone))
    return clone


def add_commits(repo: Path, *messages: str) -> None:
    for message in messages:
        git(repo, "commit", "-q", "--allow-empty", "-m", message)


def run_cli(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=repo,
        env=git_env(),
        capture_output=True,
        text=True,
    )


def outputs(text: str) -> dict[str, str]:
    return dict(line.split("=", 1) for line in text.splitlines() if "=" in line)


def test_cli_ignores_a_later_rc_tag(released):
    add_commits(released, "fix: third")
    git(released, "tag", "v0.0.31-rc.1")
    result = run_cli(released)
    assert result.returncode == 0, result.stderr
    assert outputs(result.stdout) == {
        "version": "0.0.31",
        "tag": "v0.0.31",
        "previous_tag": "v0.0.30",
        "bump": "patch",
        "title": "v0.0.31",
    }


def test_cli_refuses_a_hand_pushed_tag(released):
    add_commits(released, "fix: third")
    git(released, "tag", "v0.0.31")
    result = run_cli(released)
    assert result.returncode == 1
    assert "::error title=Release version::" in result.stderr
    assert result.stdout == ""


def resolve_version_script() -> str:
    workflow = yaml.safe_load((ROOT / ".github/workflows/main.yml").read_text())
    steps = workflow["jobs"]["resolve-version"]["steps"]
    step = next(s for s in steps if s.get("id") == "bump")
    assert step["env"] == {"RELEASE_AS": "${{ inputs.release_as }}"}
    return step["run"]


def run_resolve_version(repo: Path, tmp_path: Path, release_as: str = ""):
    """Run the workflow's own shell, as the runner does (bash -eo pipefail)."""
    scripts = repo / "scripts" / "release"
    scripts.mkdir(parents=True, exist_ok=True)
    shutil.copy(SCRIPT, scripts / SCRIPT.name)  # untracked: history is unchanged
    output = tmp_path / "github_output"
    output.write_text("")
    env = git_env() | {"GITHUB_OUTPUT": str(output), "RELEASE_AS": release_as}
    result = subprocess.run(
        [
            "bash",
            "--noprofile",
            "--norc",
            "-eo",
            "pipefail",
            "-c",
            resolve_version_script(),
        ],
        cwd=repo,
        env=env,
        capture_output=True,
        text=True,
    )
    return result, outputs(output.read_text())


@pytest.mark.parametrize(
    ("messages", "release_as", "version", "bump"),
    [
        (["fix: a", "feat(cockpit): b"], "", "0.0.31", "patch"),
        (["fix: a", "feat(helm)!: rename vm.mode"], "", "0.1.0", "minor"),
        (["fix: a\n\nBREAKING CHANGE: new Secret key"], "", "0.1.0", "minor"),
        (["fix: a", "feat: b"], "0.1.0", "0.1.0", "override"),
        (
            ["fix: a", "chore(release): cut v0.1.0\n\nRelease-As: 0.1.0"],
            "",
            "0.1.0",
            "override",
        ),
    ],
)
def test_the_workflow_step_end_to_end(
    released, tmp_path, messages, release_as, version, bump
):
    add_commits(released, *messages)
    result, out = run_resolve_version(released, tmp_path, release_as)
    assert result.returncode == 0, result.stderr
    assert out["version"] == version and out["tag"] == f"v{version}"
    assert out["previous_tag"] == "v0.0.30" and out["bump"] == bump
    assert "Last release: v0.0.30" in result.stderr


@pytest.mark.parametrize("release_as", ["0.0.30", "v0.1.0"])
def test_the_workflow_step_fails_on_a_bad_override(released, tmp_path, release_as):
    add_commits(released, "fix: a")
    result, out = run_resolve_version(released, tmp_path, release_as)
    assert result.returncode != 0
    assert out == {}  # nothing half-written into $GITHUB_OUTPUT


def test_the_workflow_step_sees_tags_only_origin_has(released, tmp_path):
    """`git fetch --tags` runs first: a release pushed since the checkout counts."""
    origin = Path(git(released, "remote", "get-url", "origin").strip())
    seed = tmp_path / "seed"
    git(tmp_path, "clone", "-q", str(origin), str(seed))
    add_commits(seed, "fix: shipped elsewhere")
    git(seed, "tag", "-a", "v0.0.31", "-m", "Release v0.0.31")
    git(seed, "push", "-q", "origin", "main", "v0.0.31")
    git(released, "pull", "-q", "--no-tags", "origin", "main")
    add_commits(released, "fix: next")
    result, out = run_resolve_version(released, tmp_path)
    assert result.returncode == 0, result.stderr
    assert out["version"] == "0.0.32"
