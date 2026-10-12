"""The controller's production logging setup must protect aiohttp access URLs."""

import os
import subprocess
import sys

import pytest


@pytest.mark.parametrize("log_level", ["INFO", "DEBUG"])
def test_aiohttp_access_log_masks_lifecycle_signature(log_level):
    # Import in a fresh interpreter: pytest's existing root handlers make
    # logging.basicConfig a no-op in this process.
    script = """
import logging
from types import SimpleNamespace

from aiohttp.web_log import AccessLogger
import vm_controller.controller

request = SimpleNamespace(
    remote="127.0.0.1",
    method="GET",
    path_qs=(
        "/vms/job-1?provision_generation=generation-2"
        "&lifecycle_auth=synthetic-signature-123"
        "&lifecycle_auth_issued_at=123"
        "&lifecycle_auth_request_id=request-1"
    ),
    version=SimpleNamespace(major=1, minor=1),
    headers={},
)
response = SimpleNamespace(status=200, body_length=0, headers={})
AccessLogger(logging.getLogger("aiohttp.access")).log(request, response, 0.01)
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        timeout=20,
        env={**os.environ, "LOG_LEVEL": log_level},
    )
    assert result.returncode == 0, result.stderr
    line = result.stderr
    assert "synthetic-signature-123" not in line
    assert "GET /vms/job-1?provision_generation=generation-2" in line
    assert "lifecycle_auth=***REDACTED***" in line
    assert "lifecycle_auth_issued_at=123" in line
    assert "lifecycle_auth_request_id=request-1" in line
    assert 'HTTP/1.1" 200' in line
