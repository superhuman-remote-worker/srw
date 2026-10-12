"""A session's browser may open the workspace's own loopback.

The browser runs inside the session's workspace container (browser-exec over
SSH), so ``localhost`` is that container, as for workers. Session 2c6d3e06
(2026-10-11) could not preview a page it served on 127.0.0.1 because the
session role kept expert_base's conservative blocklist.
"""

import pytest

from agent.tools.research.browser_security import validate_url_with_config
from shared.runtime.core.loader import load_agent_config, resolve_config_path


def _browser_config(role: str) -> dict:
    path, deployment_dir = resolve_config_path(role)
    return load_agent_config(path, deployment_dir).extra["browser"]


@pytest.mark.parametrize("role", ["session_base", "worker_base"])
def test_role_browser_opens_the_workspace_loopback(role):
    browser = _browser_config(role)

    assert validate_url_with_config("http://127.0.0.1:8765/", browser)
    assert validate_url_with_config("http://localhost:3000/", browser)


def test_session_browser_still_blocks_cloud_metadata():
    browser = _browser_config("session_base")

    with pytest.raises(ValueError, match="Blocked domain"):
        validate_url_with_config("http://169.254.169.254/latest/", browser)
