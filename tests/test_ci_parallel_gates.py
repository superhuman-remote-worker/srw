"""Parallel verification remains a prerequisite for every existing publisher."""

from pathlib import Path
import subprocess
import os

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("branch", ["main", "develop"])
def test_parallel_jobs_preserve_publication_gates(branch):
    jobs = yaml.safe_load((ROOT / f".github/workflows/{branch}.yml").read_text())[
        "jobs"
    ]
    python = jobs["test-python-shards"]
    assert python["strategy"] == {"fail-fast": False, "matrix": {"shard": [0, 1]}}
    run = next(s["run"] for s in python["steps"] if s.get("name") == "Run tests")
    assert "scripts/pytest_shard.py" in run
    assert "--index ${{ matrix.shard }} --count 2" in run
    assert "test-python-shards" in jobs["test-python"]["needs"]
    assert "always()" in jobs["test-python"]["if"]
    gate = jobs["test-cockpit"]
    assert {"test-cockpit-build", "test-cockpit-unit", "test-cockpit-browser"} <= set(
        gate["needs"]
    )
    assert "always()" in gate["if"]
    browser = jobs["test-cockpit-browser"]
    assert browser["strategy"]["fail-fast"] is False
    assert browser["strategy"]["matrix"]["gate"] == ["canvas", "cloud-review"]
    assert browser["needs"] == ["test-cockpit-build"]
    upload = next(
        s["with"]
        for s in jobs["test-cockpit-build"]["steps"]
        if s.get("uses", "").startswith("actions/upload-artifact@")
    )
    download = next(
        s["with"]
        for s in browser["steps"]
        if s.get("uses", "").startswith("actions/download-artifact@")
    )
    # Rerun failed browser jobs must find the earlier successful build; rerun
    # all jobs must replace it without an immutable-artifact name collision.
    assert upload["name"] == download["name"] == "cockpit-production"
    assert upload["overwrite"] is True
    for name, job in jobs.items():
        if name.startswith("build-"):
            assert {"test-python", "test-cockpit"} <= set(job["needs"]), name


@pytest.mark.parametrize(
    "failed", ["UNIT_RESULT", "BUILD_RESULT", "BROWSER_RESULT", None]
)
@pytest.mark.parametrize("outcome", ["failure", "cancelled", "skipped"])
def test_aggregate_gate_shell_rejects_every_non_success(failed, outcome):
    jobs = yaml.safe_load((ROOT / ".github/workflows/main.yml").read_text())["jobs"]
    script = jobs["test-cockpit"]["steps"][0]["run"]
    env = dict(
        os.environ,
        UNIT_RESULT="success",
        BUILD_RESULT="success",
        BROWSER_RESULT="success",
    )
    if failed:
        env[failed] = outcome
    result = subprocess.run(["bash", "-e", "-c", script], env=env)
    assert (result.returncode == 0) is (failed is None)


def test_stage1_reuse_skips_heavy_work_and_passes_verified_digest_to_stage2():
    jobs = yaml.safe_load((ROOT / ".github/workflows/main.yml").read_text())["jobs"]
    base = jobs["build-agent-vm-base-stage1"]
    assert base["outputs"]["image"] == "${{ steps.resolved.outputs.image }}"
    for step in base["steps"]:
        if step.get("name", "").startswith(
            ("Packer", "Free disk", "Install QEMU", "Build and push", "Download Ubuntu")
        ):
            assert "steps.base.outputs.reuse != 'true'" in step["if"]
    pull = next(
        s
        for s in jobs["build-agent-vm-base"]["steps"]
        if s.get("name") == "Pull stage1 containerDisk and extract qcow2"
    )
    assert (
        pull["env"]["VERIFIED_STAGE1_IMAGE"]
        == "${{ needs.build-agent-vm-base-stage1.outputs.image }}"
    )
    assert 'test "${{ github.event_name }}" = pull_request' in pull["run"]


def test_all_stage1_writers_record_the_same_input_identity():
    for workflow in ("main.yml", "develop.yml", "stage1-rebuild.yml"):
        text = (ROOT / ".github/workflows" / workflow).read_text()
        assert "python3 ../../scripts/vm_stage1_image.py" in text
        assert '--build-arg "SRW_STAGE1_INPUTS=${STAGE1_INPUTS}"' in text
