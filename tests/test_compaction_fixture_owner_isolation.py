"""Actual compaction tests must arrange and restore their own lifecycle owners."""

import os
import subprocess
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize(
    "name",
    [
        "test_compact_refuses_while_rewind_lock_held",
        "test_compact_boundary_maps_to_keep_recent",
        "test_compact_boundary_excludes_injections_from_keep_count",
    ],
)
@pytest.mark.parametrize(
    "state", ["open", "termination", "retirement", "pending-task", "sentinel"]
)
def test_legacy_compaction_fixture_isolates_and_restores_prior_life(name, state):
    env = {**os.environ, "KUBECONFIG": "/dev/null"}
    for key in (
        "PYTHONPATH",
        "PYTEST_ADDOPTS",
        "PYTEST_PLUGINS",
        "PYTEST_XDIST_WORKER",
    ):
        env.pop(key, None)
    env["COMPACTION_INHERITED_STATE"] = state
    try:
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "pytest",
                "-p",
                "tests._compaction_lifecycle_inherited_state_plugin",
                "tests/test_rewind_handler.py::" + name,
                "-q",
                "--tb=short",
                "--maxfail=0",
                "-p",
                "no:cacheprovider",
            ],
            cwd=Path(__file__).resolve().parents[1],
            env=env,
            capture_output=True,
            timeout=15,
        )
    except subprocess.TimeoutExpired:
        pytest.fail("compaction fixture waited behind another file's life")
    assert result.returncode == 0, result.stdout.decode(errors="replace")
