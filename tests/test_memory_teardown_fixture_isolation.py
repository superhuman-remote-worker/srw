"""Run the actual legacy memory tests after another file advertised an exact life."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "name",
    ["test_archive_captures_session_end", "test_terminate_captures_session_end_once"],
)
def test_legacy_memory_teardown_isolates_inherited_runtime_identity(name):
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "KUBECONFIG": "/dev/null"}
    for key in (
        "PYTHONPATH",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_XDIST_WORKER",
    ):
        env.pop(key, None)
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-p",
                "tests._memory_lifecycle_inherited_state_plugin",
                "tests/test_memory_cutover.py::TestTeardownWiring::" + name,
                "-q",
                "--tb=short",
                "-p",
                "no:cacheprovider",
                "--maxfail=0",
            ],
            cwd=root,
            env=env,
            capture_output=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("legacy memory fixture waited for an inherited exact retirement")
    assert result.returncode == 0, (
        "legacy memory fixture borrowed another file's identity"
    )
