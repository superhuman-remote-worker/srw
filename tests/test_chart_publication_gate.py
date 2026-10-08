"""Build failures and missing reused images must prevent chart publication."""

import importlib.util
import json
from pathlib import Path
import subprocess

import pytest
import yaml


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "verify_chart_images.py"
SPEC = importlib.util.spec_from_file_location("verify_chart_images", SCRIPT)
assert SPEC and SPEC.loader
gate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(gate)


def needs(*, changed="true", result="success"):
    return {
        "architecture": {"result": "success"},
        "changes": {
            "result": "success",
            "outputs": {
                key: value
                for component in gate.COMPONENTS
                for key, value in ((component, changed), (component + "-sha", "a" * 40))
            },
        },
        **{"build-" + component: {"result": result} for component in gate.COMPONENTS},
    }


@pytest.mark.parametrize("result", ["failure", "cancelled", "skipped", None])
def test_changed_image_requires_a_successful_build(result):
    state = needs()
    state["build-agent"]["result"] = result
    with pytest.raises(ValueError, match="agent: rebuild=true"):
        gate.expected_images(state, "ghcr.io/example/srw")


def test_verified_unchanged_images_may_be_reused():
    refs = gate.expected_images(
        needs(changed="false", result="skipped"), "ghcr.io/example/srw"
    )
    observed = []

    def inspect(ref):
        observed.append(ref)
        return "sha256:" + "b" * 64

    verified = gate.verify_images(refs, inspect)
    assert set(verified) == set(gate.COMPONENTS) | set(gate.COBUILT)
    assert observed == list(refs.values())
    assert all(ref.endswith(":sha-aaaaaaa") for ref in observed)


@pytest.mark.parametrize("digest", ["", "sha256:bad", "sha256:" + "b" * 64 + "\nBAD=1"])
def test_registry_digest_must_be_valid(digest):
    refs = gate.expected_images(needs(), "ghcr.io/example/srw")
    with pytest.raises(ValueError, match="valid digest"):
        gate.verify_images(refs, lambda ref: digest)


def test_missing_registry_image_is_a_failure_even_when_build_was_skipped():
    refs = gate.expected_images(
        needs(changed="false", result="skipped"), "ghcr.io/example/srw"
    )

    def missing(ref):
        raise subprocess.CalledProcessError(1, ["inspect", ref])

    with pytest.raises(subprocess.CalledProcessError):
        gate.verify_images(refs, missing)


@pytest.mark.parametrize("sha", ["", "latest", "a" * 7, None])
def test_component_revision_has_no_run_sha_fallback(sha):
    state = needs()
    state["changes"]["outputs"]["mcp-sha"] = sha
    with pytest.raises(ValueError, match="mcp: missing or invalid"):
        gate.expected_images(state, "ghcr.io/example/srw")


@pytest.mark.parametrize("job", ["architecture", "changes"])
def test_missing_or_failed_prerequisite_refuses_publication(job):
    state = needs()
    state[job]["result"] = "failure"
    with pytest.raises(ValueError, match="did not succeed"):
        gate.expected_images(state, "ghcr.io/example/srw")


def test_release_requires_all_builds_at_the_release_revision():
    state = needs()
    del state["changes"]
    refs = gate.expected_images(state, "ghcr.io/example/srw", "c" * 40)
    assert all(ref.endswith(":sha-ccccccc") for ref in refs.values())
    state["build-vm-controller"]["result"] = "skipped"
    with pytest.raises(ValueError, match="vm-controller: rebuild=true"):
        gate.expected_images(state, "ghcr.io/example/srw", "c" * 40)


def test_cli_writes_no_partial_outputs_when_the_last_registry_image_is_missing(
    tmp_path, monkeypatch
):
    output = tmp_path / "github.env"
    output.write_text("EXISTING=value\n")
    inventory = tmp_path / "images.json"
    monkeypatch.setenv("NEEDS_JSON", json.dumps(needs()))
    monkeypatch.setenv("GITHUB_ENV", str(output))
    observed = []

    def inspect(args, **kwargs):
        observed.append(args[4])
        if len(observed) == len(gate.COMPONENTS) + len(gate.COBUILT):
            raise subprocess.CalledProcessError(1, args)
        return subprocess.CompletedProcess(args, 0, "sha256:" + "b" * 64 + "\n")

    monkeypatch.setattr(gate.subprocess, "run", inspect)
    result = gate.main(
        ["--repository", "ghcr.io/example/srw", "--inventory", str(inventory)]
    )
    assert result == 1
    assert len(observed) == len(gate.COMPONENTS) + len(gate.COBUILT)
    assert output.read_text() == "EXISTING=value\n"
    assert not inventory.exists()


@pytest.mark.parametrize(
    ("workflow", "publication"),
    [("develop", "deploy-experimental"), ("main", "release-chart")],
)
def test_architecture_and_verified_images_gate_publication_on_the_tested_revision(
    workflow, publication
):
    path = SCRIPT.parents[1] / ".github" / "workflows" / f"{workflow}.yml"
    jobs = yaml.safe_load(path.read_text())["jobs"]
    architecture = jobs["architecture"]
    assert not architecture.get("continue-on-error", False)
    checks = [
        step for step in architecture["steps"] if "lint-imports" in step.get("run", "")
    ]
    assert checks and all(not step.get("continue-on-error", False) for step in checks)

    for name, job in jobs.items():
        for step in job.get("steps", []):
            if step.get("uses", "").startswith("actions/checkout@"):
                assert step.get("with", {}).get("ref") == "${{ github.sha }}", name
        if name.startswith("build-") or name == publication:
            assert "architecture" in job["needs"], name
            assert "needs.architecture.result == 'success'" in job["if"], name

    publish = jobs[publication]
    assert all(
        "build-" + component in publish["needs"] for component in gate.COMPONENTS
    )
    steps = publish["steps"]
    verification_index = next(
        i
        for i, step in enumerate(steps)
        if "scripts/verify_chart_images.py" in step.get("run", "")
    )
    package_index = next(
        i for i, step in enumerate(steps) if "helm package" in step.get("run", "")
    )
    verification = steps[verification_index]
    assert verification_index < package_index
    assert verification["env"]["NEEDS_JSON"] == "${{ toJSON(needs) }}"
    assert not verification.get("continue-on-error", False)
    assert all(
        step.get("if", "") != "always()" for step in steps[verification_index + 1 :]
    )


def test_the_minimal_workspace_image_shares_the_workspace_identity():
    state = needs(changed="false", result="skipped")
    state["changes"]["outputs"]["workspace-sha"] = "d" * 40
    refs = gate.expected_images(state, "ghcr.io/example/srw")
    assert refs["workspace"] == "ghcr.io/example/srw-workspace:sha-ddddddd"
    assert refs["workspace-minimal"] == (
        "ghcr.io/example/srw-workspace-minimal:sha-ddddddd"
    )


def test_a_missing_minimal_image_refuses_publication():
    refs = gate.expected_images(needs(), "ghcr.io/example/srw")

    def inspect(ref):
        if "workspace-minimal" in ref:
            raise subprocess.CalledProcessError(1, ["inspect", ref])
        return "sha256:" + "b" * 64

    with pytest.raises(subprocess.CalledProcessError):
        gate.verify_images(refs, inspect)


def test_a_cobuilt_image_never_needs_its_own_job():
    assert set(gate.COBUILT) == {"workspace-minimal"}
    assert set(gate.COBUILT.values()) <= set(gate.COMPONENTS)
    assert not set(gate.COBUILT) & set(gate.COMPONENTS)


def workflow(name):
    path = SCRIPT.parents[1] / ".github" / "workflows" / f"{name}.yml"
    return path.read_text(), yaml.safe_load(path.read_text())["jobs"]


@pytest.mark.parametrize("name", ["develop", "main"])
def test_one_job_builds_both_workspace_targets_with_the_same_tags(name):
    _, jobs = workflow(name)
    steps = jobs["build-workspace"]["steps"]
    metadata = {
        step["id"]: step["with"]
        for step in steps
        if step.get("uses", "").startswith("docker/metadata-action@")
    }
    assert metadata["meta-minimal"]["images"].endswith("-workspace-minimal")
    assert metadata["meta"]["images"].endswith("-workspace")
    assert metadata["meta-minimal"]["tags"] == metadata["meta"]["tags"]

    builds = [
        step["with"]
        for step in steps
        if step.get("uses", "").startswith("docker/build-push-action@")
    ]
    assert [build["target"] for build in builds] == ["minimal", "full"]
    minimal, full = builds
    assert minimal["tags"] == "${{ steps.meta-minimal.outputs.tags }}"
    assert full["tags"] == "${{ steps.meta.outputs.tags }}"
    for key in ("context", "file", "build-args", "push", "provenance", "cache-from"):
        assert minimal[key] == full[key], key
    # Only the last build exports the cache; it covers both stages.
    assert "cache-to" not in minimal and "mode=max" in full["cache-to"]


@pytest.mark.parametrize(
    ("name", "publication"),
    [("develop", "deploy-experimental"), ("main", "release-chart")],
)
def test_the_chart_is_stamped_with_the_minimal_image(name, publication):
    _, jobs = workflow(name)
    scripts = "\n".join(step.get("run", "") for step in jobs[publication]["steps"])
    assert ".image.workspaceMinimal.tag" in scripts


def test_develop_rebuilds_when_either_workspace_image_is_missing():
    text, _ = workflow("develop")
    assert "docker/assert-workspace-contract.sh" in text
    assert 'image_missing workspace-minimal "$WORKSPACE_SHA"' in text


@pytest.mark.parametrize(
    ("name", "publication"),
    [("develop", "deploy-experimental"), ("main", "release-chart")],
)
def test_the_driver_shim_is_vetted_tested_built_and_pinned_by_digest(name, publication):
    """Every service driver pod runs the shim: CI vets and tests it with the
    toolchain its pinned base carries, and the chart pins its digest."""
    import re

    _, jobs = workflow(name)
    steps = jobs["build-driver-shim"]["steps"]
    runs = "\n".join(step.get("run", "") for step in steps)
    assert "go vet ./..." in runs and "go test" in runs
    go = next(s for s in steps if s.get("uses", "").startswith("actions/setup-go@"))
    dockerfile = (SCRIPT.parents[1] / "docker/Dockerfile.driver-shim").read_text()
    base = re.search(
        r"FROM --platform=\$BUILDPLATFORM golang:([0-9.]+)-alpine[0-9.]*"
        r"@sha256:[0-9a-f]{64} AS build",
        dockerfile,
    )
    assert base is not None and base.group(1) == go["with"]["go-version"]
    assert 'GOARCH="$goarch"' in dockerfile and "TARGETARCH" in dockerfile
    build = next(
        s["with"] for s in steps if s.get("uses", "").startswith("docker/build-push")
    )
    assert build["file"] == "./docker/Dockerfile.driver-shim"
    scripts = "\n".join(step.get("run", "") for step in jobs[publication]["steps"])
    assert ".connectors.drivers.shim.image.digest = strenv(DRIVER_SHIM_DIGEST)" in (
        scripts
    )
    assert "driver-shim" in gate.COMPONENTS


def test_develop_rebuilds_the_shim_when_its_inputs_change():
    text, _ = workflow("develop")
    assert "DRIVER_SHIM_PATHS=(drivers/shim/" in text
    assert 'image_missing driver-shim "$DRIVER_SHIM_SHA"' in text


@pytest.mark.parametrize(
    ("name", "publication"),
    [("develop", "deploy-experimental"), ("main", "release-chart")],
)
def test_the_mcp_front_is_vetted_tested_built_and_pinned_by_digest(name, publication):
    """Every managed MCP pod runs the front (D5a): CI vets and tests it with
    the toolchain its pinned base carries, publishes it, and the chart pins
    its digest; publication waits for it."""
    import re

    _, jobs = workflow(name)
    steps = jobs["build-driver-mcp-front"]["steps"]
    test = next(s for s in steps if s.get("name") == "Vet and test the front")
    assert test["working-directory"] == "drivers/mcp-front"
    assert "go vet ./..." in test["run"] and "go test" in test["run"]
    assert "-race" in test["run"]
    # The development MCP server it is tested against: vetted and tested.
    server = next(
        s for s in steps if s.get("name") == "Vet and test the MCP test server"
    )
    assert server["working-directory"] == "drivers/mcp-test"
    assert "go vet ./..." in server["run"] and "go test" in server["run"]
    go = next(s for s in steps if s.get("uses", "").startswith("actions/setup-go@"))
    dockerfile = (SCRIPT.parents[1] / "docker/Dockerfile.driver-mcp-front").read_text()
    base = re.search(
        r"FROM --platform=\$BUILDPLATFORM golang:([0-9.]+)-alpine[0-9.]*"
        r"@sha256:[0-9a-f]{64} AS build",
        dockerfile,
    )
    assert base is not None and base.group(1) == go["with"]["go-version"]
    build = next(
        s["with"] for s in steps if s.get("uses", "").startswith("docker/build-push")
    )
    assert build["file"] == "./docker/Dockerfile.driver-mcp-front"
    assert "driver-mcp-front" in build["cache-to"]
    scripts = "\n".join(step.get("run", "") for step in jobs[publication]["steps"])
    assert (
        ".connectors.drivers.mcpFront.image.digest = strenv(DRIVER_MCP_FRONT_DIGEST)"
        in scripts
    )
    assert "build-driver-mcp-front" in jobs[publication]["needs"]
    assert "driver-mcp-front" in gate.COMPONENTS


def test_develop_rebuilds_the_mcp_front_when_its_inputs_change():
    text, jobs = workflow("develop")
    assert "DRIVER_MCP_FRONT_PATHS=(drivers/mcp-front/ drivers/mcp-test/" in text
    assert 'image_missing driver-mcp-front "$DRIVER_MCP_FRONT_SHA"' in text
    outputs = jobs["changes"]["outputs"]
    assert "driver-mcp-front" in outputs and "driver-mcp-front-sha" in outputs
    # The development MCP test server is tested, never built or published.
    for name in ("develop", "main"):
        text = workflow(name)[0]
        assert "Dockerfile.driver-mcp-test" not in text
        assert "driver-mcp-test" not in text
