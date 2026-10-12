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
from shared.connectors.env_names import connector_env_problem
from shared.connectors.file_targets import (
    ALLOWED_CONFIG_APPS,
    BAD_CHARACTERS,
    NOT_ALLOWED,
    OUTSIDE_HOME,
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
        ("/home/srw/.config/helm/repositories.yaml", None),
        ("/home/srw/.config/doctl/config.yaml", None),
        ("/home/srw/.config/hcloud/cli.toml", None),
        ("/home/srw/.config/sops/age/keys.txt", None),
        ("/home/srw/.srw-files/vendor keys/a b.json", None),
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
        # The re-review's list: a config that is code, or runs a command.
        "/home/srw/.config/gh/hosts.yml",
        "/home/srw/.config/rclone/rclone.conf",
        "/home/srw/.config/pip/pip.conf",
        "/home/srw/.config/uv/uv.toml",
        "/home/srw/.config/pnpm/rc",
        "/home/srw/.config/containers/systemd/x.container",
        "/home/srw/.config/user-tmpfiles.d/x.conf",
        "/home/srw/.config/mypy/config",
        "/home/srw/.config/python_keyring/keyringrc.cfg",
        "/home/srw/.config/anything-new/x",
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


def test_the_config_apps_are_an_explicit_short_list():
    assert ALLOWED_CONFIG_APPS == {"doctl", "gcloud", "hcloud", "helm", "sops"}
    for app in ALLOWED_CONFIG_APPS:
        assert target_problem(f"/home/srw/.config/{app}/x")[1] is None
        # The app's directory itself is no file target.
        assert target_problem(f"/home/srw/.config/{app}") == (None, NOT_ALLOWED)


@pytest.mark.parametrize(
    "path",
    [
        "/home/srw/.srw-files/a\nb",
        "/home/srw/.srw-files/a\x1b[31mred",
        "/home/srw/.srw-files/$(id)",
        "/home/srw/.srw-files/a*b",
        "/home/srw/.srw-files/a;b",
        "/home/srw/.srw-files/ü",
        "~/.srw-files/a~b",
    ],
)
def test_a_target_holds_only_safe_characters(path):
    assert target_problem(path) == (None, BAD_CHARACTERS)


def test_the_refusal_names_the_allowlist():
    text = allowed_targets_text()
    for part in (
        "~/.kube/",
        "~/.aws/",
        "~/.config/gcloud/",
        "~/.config/sops/",
        "~/.netrc",
        "~/.srw-files/",
    ):
        assert part in text
    assert ".ssh" not in text and "<app>" not in text


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
                {
                    "contents": "secret-4",
                    "target_path": "/home/srw/.srw-files/x",
                    "env_var": "NODE_OPTIONS",
                },
            ]
        }
    )
    assert problems == [
        "/tmp/ca.pem is outside the home",
        "/home/srw/.local/bin/git is not a credential-file location",
        "/home/srw/.local/bin/git has mode 0755: a credential file is never executable",
        "/home/srw/.srw-files/x's variable: NODE_OPTIONS is not a variable a "
        "connector may set: tools read it to run code or load their config",
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


# =============================================================================
# A credential file's env_var follows the one rule for every connector
# =============================================================================


@pytest.mark.parametrize(
    "name",
    [
        # The re-review's list.
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "PSQLRC",
        "PIP_CONFIG_FILE",
        "NPM_CONFIG_USERCONFIG",
        "npm_config_userconfig",
        "INPUTRC",
        "BASH_ENV",
        "ENV",
        "ZDOTDIR",
        "PYTHONSTARTUP",
        "PYTHONPATH",
        "PYTHONHOME",
        "NODE_OPTIONS",
        "LD_PRELOAD",
        "LD_LIBRARY_PATH",
        "GIT_SSH_COMMAND",
        "GIT_SSH",
        "SSH_ASKPASS",
        "GIT_ASKPASS",
        "EDITOR",
        "VISUAL",
        "PAGER",
        "GIT_PAGER",
        "LESSOPEN",
        "MANPAGER",
        "BROWSER",
        "PS1",
        "PS0",
        "PS4",
        # Found with the same effect.
        "PIP_INDEX_URL",
        "PIP_FIND_LINKS",
        "UV_CONFIG_FILE",
        "GIT_EXEC_PATH",
        "GIT_EXTERNAL_DIFF",
        "GIT_PROXY_COMMAND",
        "PERL5LIB",
        "RUBYOPT",
        "JAVA_TOOL_OPTIONS",
        "NODE_PATH",
        "XDG_CONFIG_HOME",
        "DOCKER_CONFIG",
        "HELM_PLUGINS",
        "CURL_HOME",
        "LD_AUDIT",
        # The workspace's own and the kubeconfig merge's.
        "PATH",
        "HOME",
        "KUBECONFIG",
        "SRW_TOKEN",
    ],
)
def test_a_credential_files_variable_never_points_a_tool_at_code(name):
    """The one rule every connector follows (``connector_env_problem``)."""
    assert connector_env_problem(name) is not None


@pytest.mark.parametrize(
    "name",
    [
        "GOOGLE_APPLICATION_CREDENTIALS",
        "AWS_SHARED_CREDENTIALS_FILE",
        "AWS_CONFIG_FILE",
        "SOPS_AGE_KEY_FILE",
        "NETRC",
        "PGPASSFILE",
        "VENDOR_TOKEN_FILE",
        # CA bundles: they name trusted certificates, never code.
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "NODE_EXTRA_CA_CERTS",
    ],
)
def test_a_variable_that_names_a_credential_file_is_fine(name):
    assert connector_env_problem(name) is None


# =============================================================================
# The default directory is the connector's own
# =============================================================================


def test_a_new_connectors_default_directory_carries_a_fresh_token(monkeypatch):
    from orchestrator.services.connector_drivers import credential_files as driver

    tokens = iter(["aaaa1111", "bbbb2222"])
    monkeypatch.setattr(driver, "new_directory_token", lambda: next(tokens))
    first = driver.CredentialFileDriver(GENERIC_FILE_SPEC)._normalize_files(
        "Vendor Keys", {"files": [{"contents": "x", "name": "key.pem"}]}, None
    )
    second = driver.CredentialFileDriver(GENERIC_FILE_SPEC)._normalize_files(
        "vendor-keys", {"files": [{"contents": "y", "name": "key.pem"}]}, None
    )
    assert first["files"][0]["target_path"] == (
        "/home/srw/.srw-files/vendor-keys-aaaa1111/key.pem"
    )
    assert second["files"][0]["target_path"] == (
        "/home/srw/.srw-files/vendor-keys-bbbb2222/key.pem"
    )


def test_a_saved_connectors_default_directory_follows_its_id():
    from orchestrator.services.connector_drivers import credential_files as driver

    normalized = driver.CredentialFileDriver(GENERIC_FILE_SPEC)._normalize_files(
        "Vendor Keys",
        {"files": [{"contents": "x"}]},
        {"id": "0d1e0d1e-2222-4333-8444-555566667777"},
    )
    assert normalized["files"][0]["target_path"] == (
        "/home/srw/.srw-files/vendor-keys-0d1e0d1e/file-0"
    )
