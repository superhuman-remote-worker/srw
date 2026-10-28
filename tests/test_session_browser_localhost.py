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


@pytest.mark.parametrize("role", ["session_base", "worker_base"])
@pytest.mark.parametrize(
    "url",
    [
        "http://169.254.169.254/latest/",
        "http://169.254.170.2/v2/credentials",  # ECS task metadata
        "http://2852039166/",  # 169.254.169.254, decimal
        "http://0xa9fea9fe/",
        "http://0251.0376.0251.0376/",
        "http://[::ffff:169.254.169.254]/",
        "http://[fd00:ec2::254]/",
        "http://[fe80::1]/",
        "http://168.63.129.16/",  # Azure wireserver
        "http://224.0.0.1/",
    ],
)
def test_role_browser_blocks_metadata_in_any_spelling(role, url):
    # The worker overlay clears blocked_domains, so before the shared
    # address rule a worker's browser could open cloud metadata.
    browser = _browser_config(role)

    with pytest.raises(ValueError, match="Blocked"):
        validate_url_with_config(url, browser)


@pytest.mark.parametrize(
    "url",
    [
        "http://10.0.0.5:8080/",
        "http://192.168.1.10/",
        "http://[::1]:3000/",
        "http://0.0.0.0:3000/",  # a dev server's announced address
    ],
)
def test_worker_browser_still_opens_workspace_private_addresses(url):
    browser = _browser_config("worker_base")

    assert validate_url_with_config(url, browser) == url


@pytest.mark.parametrize(
    "url",
    [
        # Chromium ends the authority at the backslash: this is metadata.
        "http://169.254.169.254\\@example.com/latest/",
        "http://%31%36%39.254.169.254/",
    ],
)
def test_worker_browser_reads_the_host_chromium_dials(url):
    browser = _browser_config("worker_base")

    with pytest.raises(ValueError, match="Blocked address"):
        validate_url_with_config(url, browser)
