"""Where a credential file may land (``shared.connectors.file_targets``).

One allowlist decides for the orchestrator (a 400 when a connector is
saved), the connector's Test (rows saved before the rule) and the agent
(which skips such a row at delivery and says so in the README).
"""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from orchestrator.services.connector_drivers.credential_files import (
    CredentialFileDriver,
    undeliverable_files,
)
from shared.connectors.builtin import GENERIC_FILE_SPEC
from shared.connectors.file_targets import (
    NOT_ALLOWED,
    OUTSIDE_HOME,
    REFUSED_CONFIG_APPS,
    allowed_targets_text,
    home_relative,
    mode_problem,
    safe_mode,
    target_problem,
)


@pytest.mark.parametrize(
    ("path", "relative"),
    [
        ("/home/srw/.kube/config", ".kube/config"),
        ("~/.kube/configs/a.yaml", ".kube/configs/a.yaml"),
        ("/home/srw/.aws/credentials", ".aws/credentials"),
        ("/home/srw/.azure/azureProfile.json", ".azure/azureProfile.json"),
        ("/home/srw/.docker/config.json", ".docker/config.json"),
        ("/home/srw/.config/gcloud/application_default_credentials.json", None),
        ("/home/srw/.config/gh/hosts.yml", ".config/gh/hosts.yml"),
        ("/home/srw/.config/rclone/rclone.conf", ".config/rclone/rclone.conf"),
        ("/home/srw/.netrc", ".netrc"),
        ("/home/srw/.pgpass", ".pgpass"),
        ("/home/srw/.srw-files/vendor/key.pem", ".srw-files/vendor/key.pem"),
        ("/home/srw/./.kube/x/../config", ".kube/config"),
    ],
)
def test_data_locations_are_allowed(path, relative):
    expected = relative or home_relative(path)
    assert target_problem(path) == (expected, None)


@pytest.mark.parametrize(
    "path",
    [
        # The blocker: ~/.ssh is C1's, and an ssh config runs ProxyCommand.
        "/home/srw/.ssh/config",
        "/home/srw/.ssh/id_ed25519",
        "/home/srw/.ssh/srw-managed/config.d/x.conf",
        # Executed or sourced by the shell, git, Python, the session.
        "/home/srw/.local/bin/git",
        "/home/srw/bin/git",
        "/home/srw/.bash_aliases",
        "/home/srw/.bashrc",
        "/home/srw/.profile",
        "/home/srw/.tmux.conf",
        "/home/srw/.gitconfig",
        "/home/srw/.local/lib/python3.12/site-packages/x.pth",
        "/home/srw/.config/git/config",
        "/home/srw/.config/fish/config.fish",
        "/home/srw/.config/systemd/user/x.service",
        "/home/srw/.config/autostart/x.desktop",
        "/home/srw/.config/environment.d/x.conf",
        "/home/srw/.config/nvim/init.lua",
        "/home/srw/.config/code-server/config.yaml",
        "/home/srw/.config/mimeapps.list",
        "/home/srw/.config",
        # Cloud sync uploads it.
        "/home/srw/workspace/creds.txt",
        # Code-loading subtrees of an allowed directory.
        "/home/srw/.docker/cli-plugins/docker-x",
        "/home/srw/.azure/cliextensions/x/__init__.py",
        # The directories themselves are no file target.
        "/home/srw/.kube",
        "/home/srw/.srw-files",
        # SRW's store, and anything aliasing back into the home.
        "/home/srw/.srw-credentials/x.sh",
        "/home/srw/me/.ssh/rc",
    ],
)
def test_everything_else_in_the_home_is_refused(path):
    assert target_problem(path) == (None, NOT_ALLOWED)


@pytest.mark.parametrize(
    "path",
    ["/tmp/x", "/run/x", "/workspace/x", "/home/srw", "/home/srw/../x", "x", ""],
)
def test_outside_the_home_is_refused(path):
    assert target_problem(path) == (None, OUTSIDE_HOME)


def test_every_refused_config_app_is_refused():
    for app in REFUSED_CONFIG_APPS:
        assert target_problem(f"/home/srw/.config/{app}/x") == (None, NOT_ALLOWED)


def test_the_refusal_names_the_allowlist():
    text = allowed_targets_text()
    for part in (
        "~/.kube/",
        "~/.aws/",
        "~/.config/<app>/",
        "~/.netrc",
        "~/.srw-files/",
    ):
        assert part in text
    assert ".ssh" not in text


@pytest.mark.parametrize(
    ("mode", "refused"),
    [
        (0o600, False),
        (0o644, False),
        (0o400, False),
        (0o755, True),
        (0o700, True),
        (0o601, True),
        (0o4600, True),
    ],
)
def test_a_credential_file_is_never_executable(mode, refused):
    assert (mode_problem(mode) is not None) is refused
    assert safe_mode(mode) & 0o111 == 0
    assert safe_mode(mode) & ~0o777 == 0


# =============================================================================
# Test connection reports a row saved before the rule
# =============================================================================


def test_undeliverable_files_name_paths_and_modes_only():
    problems = undeliverable_files(
        {
            "files": [
                {"contents": "secret-1", "target_path": "/home/srw/.kube/c.yaml"},
                {"contents": "secret-2", "target_path": "/tmp/ca.pem"},
                {
                    "contents": "secret-3",
                    "target_path": "/home/srw/.local/bin/git",
                    "mode": "0755",
                },
            ]
        }
    )
    assert problems == [
        "/tmp/ca.pem is outside the home",
        "/home/srw/.local/bin/git is not a credential-file location",
        "/home/srw/.local/bin/git has mode 0755: a credential file is never executable",
    ]
    assert not any("secret" in problem for problem in problems)


@pytest.mark.asyncio
async def test_test_connection_says_why_a_saved_file_is_not_delivered():
    driver = CredentialFileDriver(GENERIC_FILE_SPEC)
    ctx = SimpleNamespace(environment=None)
    refused = await driver.check(
        {},
        {"files": [{"contents": "x", "target_path": "/home/srw/.ssh/config"}]},
        ctx=ctx,
    )
    assert refused["status"] == "error"
    assert refused["error_class"] == "config"
    assert (
        "/home/srw/.ssh/config is not a credential-file location" in refused["message"]
    )
    assert "credential files go under" in refused["message"].lower()
    fine = await driver.check(
        {}, {"files": [{"contents": "x", "target_path": "/home/srw/.netrc"}]}, ctx=ctx
    )
    assert fine["status"] == "unsupported"
