"""main.yml publishes only on a release run: a push to main or a dispatch on main.

resolve-version is the single gate. It runs only on a release run, and every
registry login, image push, :latest/:vX.Y.Z tag, chart push, git tag and
GitHub Release keys off its success. A PR to main, or a dispatch of this
workflow on any other ref, is a validation run that publishes nothing.
"""

from __future__ import annotations

from pathlib import Path
import re

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
RAW = (ROOT / ".github/workflows/main.yml").read_text()
WORKFLOW = yaml.safe_load(RAW)
JOBS = WORKFLOW["jobs"]
PUBLISH = "needs.resolve-version.result == 'success'"
ON_MAIN = "github.ref == 'refs/heads/main'"


def condition(job: str) -> str:
    return " ".join(JOBS[job]["if"].split())


def steps():
    for name, job in JOBS.items():
        for step in job.get("steps", []):
            yield name, step


def test_dispatch_takes_an_optional_release_as():
    dispatch = WORKFLOW[True]["workflow_dispatch"]  # YAML 1.1 reads `on` as True
    release_as = dispatch["inputs"]["release_as"]
    assert release_as["type"] == "string" and release_as["required"] is False
    assert set(WORKFLOW[True]) == {"push", "pull_request", "workflow_dispatch"}
    assert WORKFLOW[True]["push"] == {"branches": ["main"]}


def test_resolve_version_runs_only_on_a_release_run():
    gate = condition("resolve-version")
    assert ON_MAIN in gate
    assert (
        "(github.event_name == 'push' || github.event_name == 'workflow_dispatch')"
        in gate
    )
    for needed in ("lint", "dependency-audit", "test-python", "test-cockpit"):
        assert f"needs.{needed}.result == 'success'" in gate


def test_inputs_never_reach_a_script_unquoted():
    for name, step in steps():
        assert "inputs." not in step.get("run", ""), name
    bump = next(s for s in JOBS["resolve-version"]["steps"] if s.get("id") == "bump")
    assert bump["env"]["RELEASE_AS"] == "${{ inputs.release_as }}"
    assert '--release-as "$RELEASE_AS"' in bump["run"]


def test_the_old_event_name_push_predicate_is_gone():
    assert "github.event_name != 'pull_request'" not in RAW
    assert "github.event_name == 'push' &&" not in RAW.replace(
        "(github.event_name == 'push' || github.event_name == 'workflow_dispatch') &&",
        "",
    )


def test_every_image_push_login_and_release_tag_keys_off_the_gate():
    pushes = logins = tags = 0
    for name, step in steps():
        uses = step.get("uses", "")
        with_ = step.get("with", {})
        if uses.startswith("docker/build-push-action@"):
            pushes += 1
            assert with_["push"] == "${{ " + PUBLISH + " }}", name
        if uses.startswith("docker/metadata-action@"):
            for line in with_["tags"].splitlines():
                if "type=raw" in line:
                    tags += 1
                    assert line.strip().endswith("enable=${{ " + PUBLISH + " }}"), name
        if uses.startswith("docker/login-action@") and name.startswith("build-"):
            if name.startswith("build-agent-vm-base"):
                continue  # pulls stage1 on every run; its pushes are gated below
            logins += 1
            assert step["if"] == PUBLISH, name
    assert pushes >= 14 and logins >= 12 and tags >= 28


def test_the_vm_base_jobs_publish_only_on_a_release():
    stage1 = condition("build-agent-vm-base-stage1")
    assert ON_MAIN in stage1 and PUBLISH in stage1
    stage2 = {s.get("name", ""): s for s in JOBS["build-agent-vm-base"]["steps"]}
    pull = stage2["Pull stage1 containerDisk and extract qcow2"]
    assert pull["env"]["PUBLISH"] == "${{ " + PUBLISH + " }}"
    build = next(s for n, s in stage2.items() if "containerDisk image" in n)
    assert build["env"]["PUBLISH"] == "${{ " + PUBLISH + " }}"
    assert 'if [ "$PUBLISH" = true ]; then' in build["run"]
    assert "docker push" in build["run"].split('if [ "$PUBLISH" = true ]; then')[1]


@pytest.mark.parametrize("job", ["release-chart", "github-release"])
def test_publication_jobs_are_guarded_by_ref_and_gate(job):
    gate = condition(job)
    assert ON_MAIN in gate and PUBLISH in gate
    assert "always() && !cancelled()" in gate


def test_builds_still_run_as_validation_when_the_gate_is_skipped():
    for name, job in JOBS.items():
        if name.startswith("build-") and name != "build-agent-vm-base-stage1":
            assert "needs.resolve-version.result == 'skipped'" in condition(name), name


def test_the_tag_message_is_what_next_version_counts():
    script = "\n".join(s.get("run", "") for s in JOBS["release-chart"]["steps"])
    assert '-m "Release ${{ needs.resolve-version.outputs.tag }}"' in script
    tag_step = script.index("git push origin")
    assert tag_step > script.index("helm push"), "the tag goes after the chart"


def test_notes_are_rendered_before_any_build_and_published_after_the_tag():
    resolve = JOBS["resolve-version"]
    rendered = "\n".join(s.get("run", "") for s in resolve["steps"])
    assert "scripts/release/release_notes.py" in rendered
    upload = next(
        s
        for s in resolve["steps"]
        if s.get("uses", "").startswith("actions/upload-artifact@")
    )
    assert upload["with"]["name"] == "release-notes"
    assert upload["with"]["if-no-files-found"] == "error"
    for output in ("version", "tag", "previous_tag", "title"):
        assert output in resolve["outputs"]

    release = JOBS["github-release"]
    assert set(release["needs"]) == {"resolve-version", "release-chart"}
    assert release["permissions"] == {"contents": "write"}
    download = next(
        s
        for s in release["steps"]
        if s.get("uses", "").startswith("actions/download-artifact@")
    )
    assert download["with"]["name"] == "release-notes"
    create = release["steps"][-1]
    assert create["env"]["TITLE"] == "${{ needs.resolve-version.outputs.title }}"
    run = " ".join(create["run"].split())
    assert re.search(r'gh release create "\$TAG" --verify-tag --latest', run)
    assert "--prerelease" not in run and "--draft" not in run


def test_the_preview_is_read_only_and_pr_only():
    preview = JOBS["release-preview"]
    assert preview["if"] == "github.event_name == 'pull_request'"
    assert preview["permissions"] == {"contents": "read"}
    assert not preview.get("needs")
    assert all(
        not s.get("uses", "").startswith(("docker/", "actions/upload-artifact@"))
        for s in preview["steps"]
    )
