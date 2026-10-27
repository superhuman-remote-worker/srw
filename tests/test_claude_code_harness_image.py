"""Static checks for the Claude Code harness image, its CI jobs and example Job."""

from pathlib import Path

from jsonschema import Draft202012Validator

from shared.manifests import load_schema, parse_documents

ROOT = Path(__file__).resolve().parents[1]
BASE = (
    "python:3.12-slim@sha256:"
    "57cd7c3a7a273101a6485ba99423ee568157882804b1124b4dd04266317710de"
)


def test_dockerfile_is_pinned_non_root_and_has_no_cmd():
    text = (ROOT / "docker/Dockerfile.harness-claude-code").read_text()
    assert BASE in text
    assert "USER 10001:10001" in text
    assert 'ENTRYPOINT ["python3", "-m", "srw_claude_code_harness"]' in text
    assert "\nCMD" not in text
    assert "harnesses/claude-code/requirements.txt" in text
    # The CLI's local cwd mirrors the workspace root the model sees (R16).
    assert "mkdir -p /home/agent-host/workspace" in text
    assert text.index("chown -R 10001:10001 /home/agent-host") < text.index("USER ")


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


def test_example_job_validates_against_the_manifest_schema():
    text = (ROOT / "examples/manifests/claude-code-harness-job.yaml").read_text()
    documents = parse_documents(text)
    validator = Draft202012Validator(load_schema())
    for document in documents:
        assert list(validator.iter_errors(document)) == []
    (job,) = documents
    assert job["kind"] == "Job"
    assert job["spec"]["execution"]["expert"]["inline"]["runtime"]["image"].endswith(
        "@sha256:" + "0" * 64
    )
