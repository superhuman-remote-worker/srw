"""The actual Officer queue test must not borrow another file's shutdown owner."""

import os
from pathlib import Path
import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "fence,prior_task", [(False, False), (True, False), (True, True)]
)
def test_officer_queue_fixture_isolates_and_restores_inherited_owners(
    fence, prior_task
):
    root = Path(__file__).resolve().parents[1]
    env = {**os.environ, "KUBECONFIG": "/dev/null"}
    # This child loads a repository-only pytest plugin before collection adds
    # the test root. Keep that test import available under CI's safe-path env.
    for key in (
        "PYTHONPATH",
        "PYTHONSAFEPATH",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_XDIST_WORKER",
    ):
        env.pop(key, None)
    env["OFFICER_INHERITED_FENCE"] = "1" if fence else "0"
    env["OFFICER_INHERITED_TASK"] = "1" if prior_task else "0"
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-p",
                "tests._officer_lifecycle_inherited_state_plugin",
                "tests/test_officer_substrate.py::TestOfficerInputWait::"
                "test_sleep_request_files_clamped_wake_and_queue_wins",
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
        pytest.fail("Officer fixture waited behind an inherited termination fence")
    assert result.returncode == 0, (result.stdout + result.stderr).decode(
        errors="replace"
    )
