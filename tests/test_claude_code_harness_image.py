"""Static checks for the Claude Code harness image, its CI jobs and example Job."""

from pathlib import Path
import tomllib

import yaml
from jsonschema import Draft202012Validator

from shared.manifests import load_schema, parse_documents

ROOT = Path(__file__).resolve().parents[1]
BASE = (
    "python:3.12-slim@sha256:"
    "57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
)


def test_dockerfile_is_pinned_non_root_isolated_and_has_no_cmd():
    text = (ROOT / "docker/Dockerfile.harness-claude-code").read_text()
    assert BASE in text
    assert "USER 999:999" in text
    # Isolated mode: a Job's env (PYTHONPATH, PYTHONSTARTUP, PYTHONHOME) can
    # never make the interpreter load code before the harness does (C1), and
    # the package is installed where the interpreter looks on its own.
    assert 'ENTRYPOINT ["python3", "-I", "-u", "-m", "srw_claude_code_harness"]' in text
    assert "PYTHONPATH=" not in text
    assert "harnesses/claude-code/pyproject.toml" in text
    assert "pip install --no-cache-dir --no-deps" in text
    assert "\nCMD" not in text
    assert "harnesses/claude-code/requirements.txt" in text
    # uid 999 (the pod's runAsUser) is a real account and the environment names it: asyncssh asks
    # getpass.getuser() for the local user before every connection, and Node
    # tools ask os.userInfo(); an unknown uid with no USER fails both.
    assert "useradd --uid 999" in text
    assert "USER=harness" in text and "LOGNAME=harness" in text
    # The CLI's local cwd mirrors the workspace root the model sees (R16).
    assert "mkdir -p /home/agent-host/workspace" in text
    assert text.index("chown -R 999:999 /home/agent-host") < text.index(
        "\nUSER 999:999"
    )


def test_harness_package_declares_itself_without_dependencies():
    """The image installs this package; its dependencies are pinned by
    requirements.txt and installed first."""
    pyproject = tomllib.loads(
        (ROOT / "harnesses/claude-code/pyproject.toml").read_text()
    )
    assert pyproject["project"]["name"] == "srw-claude-code-harness"
    assert pyproject["project"].get("dependencies", []) == []
    assert pyproject["tool"]["setuptools"]["packages"]["find"]["include"] == [
        "srw_claude_code_harness",
        "srw_claude_code_harness.*",
    ]


def test_requirements_pin_the_sdk_and_asyncssh():
    lines = (ROOT / "harnesses/claude-code/requirements.txt").read_text().split()
    assert "claude-agent-sdk==0.2.174" in lines
    assert "asyncssh==2.24.0" in lines
    assert all("==" in line for line in lines if line and not line.startswith("#"))


def test_ci_builds_the_harness_like_a_driver():
    main = (ROOT / ".github/workflows/main.yml").read_text()
    develop = (ROOT / ".github/workflows/develop.yml").read_text()
    assert "build-harness-claude-code:" in main
    assert "io.srw.component=harness-claude-code" in main
    assert (
        "HARNESS_CLAUDE_CODE_PATHS=(harnesses/claude-code/ .dockerignore "
        "docker/Dockerfile.harness-claude-code)"
    ) in develop
    assert "build-harness-claude-code:" in develop
    assert "io.srw.component=harness-claude-code" in develop


def test_develop_runs_the_python_tests_for_harness_only_changes():
    develop = (ROOT / ".github/workflows/develop.yml").read_text()
    start = develop.index(
        "git diff --name-only \"$BASE\" HEAD -- \\\n            'src/'"
    )
    python_filter = develop[start : develop.index('echo "python=true"', start)]
    for path in (
        "'harnesses/'",
        "'docker/Dockerfile.harness-claude-code'",
        "'examples/manifests/'",
    ):
        assert path in python_filter


def test_example_manifests_validate_against_the_manifest_schema():
    from orchestrator.services.manifest_approved_images import IMAGE_REFERENCE

    validator = Draft202012Validator(load_schema())
    docs = {}
    for name in ("claude-code-harness-expert.yaml", "claude-code-harness-job.yaml"):
        text = (ROOT / "examples/manifests" / name).read_text()
        documents = parse_documents(text)
        for document in documents:
            assert list(validator.iter_errors(document)) == [], name
        (docs[name],) = documents
    expert = docs["claude-code-harness-expert.yaml"]
    assert expert["kind"] == "Expert"
    runtime = expert["spec"]["runtime"]
    assert runtime["adapter"] == "srw/v1"
    assert IMAGE_REFERENCE.fullmatch(runtime["image"])
    assert runtime["image"].startswith(
        "ghcr.io/superhuman-remote-worker/srw-harness-claude-code@sha256:"
    )
    assert runtime["image"].endswith("0" * 64)
    assert runtime["config"]["config_name"] == "worker_base"
    for launch_field in ("command", "args", "env", "probes"):
        assert launch_field not in runtime
    job = docs["claude-code-harness-job.yaml"]
    assert job["kind"] == "Job"
    assert (
        job["spec"]["execution"]["expert"]["ref"]["name"]
        == (expert["metadata"]["name"])
    )


def test_ci_smoke_tests_the_image_before_it_is_published():
    """M5: tests run against the venv's SDK; the image pins its own. Both CI
    jobs run the built image (no network) from a start request through
    job_options before the step that pushes."""
    for workflow in ("develop.yml", "main.yml"):
        jobs = yaml.safe_load((ROOT / ".github/workflows" / workflow).read_text())[
            "jobs"
        ]
        steps = jobs["build-harness-claude-code"]["steps"]
        names = [step.get("name", "") for step in steps]
        build = names.index("Build the image for the smoke test")
        smoke = names.index("Smoke-test the image")
        push = next(
            i for i, step in enumerate(steps) if step.get("with", {}).get("push")
        )
        assert build < smoke < push, workflow
        built = steps[build]["run"]
        assert built.startswith("docker buildx build --load ")
        assert "--file ./docker/Dockerfile.harness-claude-code" in built
        assert "--tag srw-harness-claude-code:smoke" in built and "--push" not in built
        command = steps[smoke]["run"]
        # The imports run under -I, so they prove the installed package is
        # found without PYTHONPATH.
        assert (
            "--network none --entrypoint python3 srw-harness-claude-code:smoke -I -c"
            in command
        )
        for needle in ("parse_job_start(", "job_options(", "make_tools("):
            assert needle in command, (workflow, needle)
        # The real start command runs too: without ORCHESTRATOR_URL it exits 2.
        assert "docker run --rm --network none srw-harness-claude-code:smoke" in command
        assert 'test "$rc" -eq 2' in command
        # The local user resolves with the USER variables cleared (passwd entry)
        # and under the image's own environment.
        assert "-e USER= -e LOGNAME=" in command and "getpass.getuser()" in command
        # The pod runs every agent image as 999:999 on a read-only root: the
        # smoke test runs under that identity too.
        assert "--user 999:999" in command and "--read-only" in command
