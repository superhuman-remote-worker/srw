"""Tests for the credentials.files[] validator.

The validator lives at ``orchestrator/security/credential_files.py`` and is
invoked from ``orchestrator/main.py`` at the create_datasource /
update_datasource endpoints. It enforces:

- per-file size cap (64 KB) and per-datasource count cap (5)
- target_path safety: a kubeconfig's or generic file's target is on the
  credential-file allowlist (``shared.connectors.file_targets``); an ssh_key's
  must resolve under writable mounts, not under system roots
- file ``mode`` well-formedness (4-digit octal), never an execute bit for a
  file the workspace receives
- ``env_var`` well-formedness (POSIX identifier), not reserved, not KUBECONFIG
- type-specific defaults for ``kubeconfig`` and ``ssh_key``

The validator's output is what eventually gets encrypted into the
``datasources.credentials`` JSONB column and shipped to the agent.
"""

from __future__ import annotations

import pytest

from orchestrator.security.credential_files import (
    AGENT_HOME,
    CREDENTIAL_FILE_TYPES,
    CredentialFileValidationError,
    MAX_FILES_PER_DATASOURCE,
    MAX_FILE_BYTES,
    normalize_credential_files,
    slugify_datasource_name,
)


# =============================================================================
# slugify_datasource_name
# =============================================================================


class TestSlugify:
    def test_simple(self):
        assert slugify_datasource_name("Prod EU Cluster") == "prod-eu-cluster"

    def test_collapse_punctuation(self):
        assert slugify_datasource_name("k3d (local dev)") == "k3d-local-dev"

    def test_unicode_stripped(self):
        # Non-[a-z0-9] sequences collapse to one hyphen.
        assert slugify_datasource_name("über/cluster") == "ber-cluster"

    def test_empty_falls_back(self):
        assert slugify_datasource_name("") == "unnamed"
        assert slugify_datasource_name("   ") == "unnamed"
        assert slugify_datasource_name("---") == "unnamed"


# =============================================================================
# Pass-through for non-credential-file types
# =============================================================================


class TestPassThrough:
    def test_generic_type_untouched(self):
        creds = {"env_vars": {"PGHOST": "db.example"}}
        assert normalize_credential_files("generic", "any", creds) is creds

    def test_repository_type_untouched(self):
        creds = {"ssh_key": "----PRIVATE----", "auth_method": "ssh"}
        out = normalize_credential_files("repository", "Github", creds)
        assert out is creds

    def test_none_passes_through_for_non_credential_types(self):
        assert normalize_credential_files("postgresql", "PG", None) is None


# =============================================================================
# kubeconfig defaults
# =============================================================================


class TestKubeconfig:
    def _ok(self):
        return {"files": [{"contents": "apiVersion: v1\nkind: Config\n"}]}

    def test_minimal_fills_defaults(self):
        out = normalize_credential_files("kubeconfig", "Prod EU", self._ok())
        assert out is not None
        files = out["files"]
        assert len(files) == 1
        f = files[0]
        assert f["target_path"] == f"{AGENT_HOME}/.kube/configs/prod-eu.yaml"
        assert f["mode"] == "0600"
        assert "env_var" not in f  # KUBECONFIG is set on the merged file, not per-ds

    def test_must_have_exactly_one_file(self):
        with pytest.raises(CredentialFileValidationError, match="exactly one file"):
            normalize_credential_files(
                "kubeconfig",
                "x",
                {"files": [{"contents": "a"}, {"contents": "b"}]},
            )

    def test_empty_files_rejected(self):
        with pytest.raises(CredentialFileValidationError, match="non-empty list"):
            normalize_credential_files("kubeconfig", "x", {"files": []})

    def test_missing_credentials_rejected(self):
        with pytest.raises(CredentialFileValidationError, match="required"):
            normalize_credential_files("kubeconfig", "x", None)

    def test_user_override_target_path_accepted(self):
        out = normalize_credential_files(
            "kubeconfig",
            "x",
            {
                "files": [
                    {
                        "contents": "a",
                        "target_path": "~/.kube/custom-config.yaml",
                    }
                ]
            },
        )
        assert (
            out["files"][0]["target_path"] == f"{AGENT_HOME}/.kube/custom-config.yaml"
        )


# =============================================================================
# ssh_key defaults
# =============================================================================


class TestSshKey:
    def test_single_file_is_private_key(self):
        out = normalize_credential_files(
            "ssh_key",
            "Github Deploy",
            {"files": [{"contents": "----PRIVATE----"}]},
        )
        f = out["files"][0]
        assert f["target_path"] == f"{AGENT_HOME}/.ssh/github-deploy"
        assert f["mode"] == "0600"

    def test_two_files_private_then_public(self):
        out = normalize_credential_files(
            "ssh_key",
            "Github",
            {
                "files": [
                    {"contents": "----PRIVATE----"},
                    {"contents": "ssh-ed25519 AAA..."},
                ]
            },
        )
        priv, pub = out["files"]
        assert priv["target_path"] == f"{AGENT_HOME}/.ssh/github"
        assert priv["mode"] == "0600"
        assert pub["target_path"] == f"{AGENT_HOME}/.ssh/github.pub"
        assert pub["mode"] == "0644"

    def test_three_files_rejected(self):
        with pytest.raises(CredentialFileValidationError, match="at most two files"):
            normalize_credential_files(
                "ssh_key",
                "x",
                {"files": [{"contents": c} for c in ("a", "b", "c")]},
            )


# =============================================================================
# generic_file
# =============================================================================


class TestGenericFile:
    def test_the_default_directory_carries_the_connectors_token(self):
        out = normalize_credential_files(
            "generic_file",
            "Vendor Keys",
            {"files": [{"contents": "data"}]},
            directory_token="0d1e0d1e",
        )
        assert out["files"][0]["target_path"] == (
            f"{AGENT_HOME}/.srw-files/vendor-keys-0d1e0d1e/file-0"
        )

    @pytest.mark.parametrize(
        "path", ["~/.srw-files/a\nb", "~/.srw-files/$(id)", "~/.srw-files/a\x07"]
    )
    def test_a_target_with_an_unsafe_character_is_refused(self, path):
        with pytest.raises(CredentialFileValidationError, match="character"):
            normalize_credential_files(
                "generic_file", "x", {"files": [{"contents": "x", "target_path": path}]}
            )

    def test_a_file_without_a_target_goes_to_the_neutral_directory(self):
        out = normalize_credential_files(
            "generic_file",
            "Vendor Keys",
            {
                "files": [
                    {"contents": "data"},
                    {"contents": "more", "name": "../api token.json"},
                ]
            },
        )
        assert [f["target_path"] for f in out["files"]] == [
            f"{AGENT_HOME}/.srw-files/vendor-keys/file-0",
            f"{AGENT_HOME}/.srw-files/vendor-keys/api-token.json",
        ]

    def test_minimal_user_payload(self):
        out = normalize_credential_files(
            "generic_file",
            "GCloud Creds",
            {
                "files": [
                    {
                        "contents": '{"type":"service_account"}',
                        "target_path": "~/.config/gcloud/creds.json",
                    }
                ]
            },
        )
        f = out["files"][0]
        # ~ resolved
        assert f["target_path"] == f"{AGENT_HOME}/.config/gcloud/creds.json"
        # default mode applied
        assert f["mode"] == "0600"

    def test_env_var_normalized(self):
        out = normalize_credential_files(
            "generic_file",
            "x",
            {
                "files": [
                    {
                        "contents": "abc",
                        "target_path": "~/.srw-files/foo",
                        "env_var": "MY_TOKEN_FILE",
                    }
                ]
            },
        )
        assert out["files"][0]["env_var"] == "MY_TOKEN_FILE"


# =============================================================================
# Size / count caps
# =============================================================================


class TestCaps:
    def test_too_many_files(self):
        files = [
            {"contents": "x", "target_path": f"~/.srw-files/{i}"}
            for i in range(MAX_FILES_PER_DATASOURCE + 1)
        ]
        with pytest.raises(CredentialFileValidationError, match="At most"):
            normalize_credential_files("generic_file", "x", {"files": files})

    def test_max_files_accepted(self):
        files = [
            {"contents": "x", "target_path": f"~/.srw-files/{i}"}
            for i in range(MAX_FILES_PER_DATASOURCE)
        ]
        out = normalize_credential_files("generic_file", "x", {"files": files})
        assert len(out["files"]) == MAX_FILES_PER_DATASOURCE

    def test_contents_over_size_cap_rejected(self):
        too_big = "x" * (MAX_FILE_BYTES + 1)
        with pytest.raises(CredentialFileValidationError, match="exceed"):
            normalize_credential_files(
                "generic_file",
                "x",
                {"files": [{"contents": too_big, "target_path": "~/.srw-files/big"}]},
            )

    def test_contents_must_be_string(self):
        with pytest.raises(CredentialFileValidationError, match="UTF-8 string"):
            normalize_credential_files(
                "generic_file",
                "x",
                {"files": [{"contents": 123, "target_path": "~/.srw-files/x"}]},
            )


# =============================================================================
# Target path safety
# =============================================================================


@pytest.mark.parametrize(
    "bad_path",
    [
        "/etc/passwd",
        "/etc/shadow",
        "/proc/self/environ",
        "/sys/kernel",
        "/dev/null",
        "/var/log/messages",
        "/usr/bin/anything",
        # Traversal collapses via normpath, then fails the writable-root check.
        "/tmp/../etc/passwd",
        # ~ expands to /home/srw, then the relative .. crosses into /home.
        "~/../etc/passwd",
        # Outside any writable root.
        "/opt/something",
        "/root/anything",
        # Relative path.
        "relative/path",
    ],
)
def test_blocked_paths(bad_path):
    with pytest.raises(CredentialFileValidationError):
        normalize_credential_files(
            "generic_file",
            "x",
            {"files": [{"contents": "x", "target_path": bad_path}]},
        )


@pytest.mark.parametrize(
    "refused_path",
    [
        # Accepted before D1d's review, never delivered: not in the home.
        "/tmp/something",
        "/run/secret.txt",
        "/workspace/.secrets/key",
        # The shell, sshd and SRW's managed repositories run these.
        "~/.ssh/config",
        "~/.ssh/id_ed25519",
        "~/.ssh/rc",
        "~/.bashrc",
        "~/.bash_profile",
        "~/.profile",
        "~/.bash_aliases",
        "~/workspace.md",
        # First on PATH, Python's user site, git's config.
        "~/.local/bin/git",
        "~/bin/git",
        "~/.local/lib/python3.12/site-packages/x.pth",
        "~/.config/git/config",
        "~/.config/fish/config.fish",
        "~/.config/systemd/user/x.service",
        "~/.config/autostart/x.desktop",
        "~/.config/mimeapps.list",
        # An explicit list of .config apps (D1d re-review): these hold code.
        "~/.config/gh/hosts.yml",
        "~/.config/pip/pip.conf",
        "~/.config/containers/systemd/x.container",
        "~/.config/user-tmpfiles.d/x.conf",
        # Cloud sync uploads the workspace.
        "~/workspace/creds.txt",
        # The subtrees CLIs load code from.
        "~/.docker/cli-plugins/docker-x",
        "~/.azure/cliextensions/x/__init__.py",
        # SRW's own store.
        "~/.srw-credentials/x.sh",
    ],
)
def test_targets_off_the_allowlist_are_refused(refused_path):
    for ds_type in ("generic_file", "kubeconfig"):
        with pytest.raises(CredentialFileValidationError) as caught:
            normalize_credential_files(
                ds_type,
                "x",
                {"files": [{"contents": "x", "target_path": refused_path}]},
            )
        assert "credential files go under ~/.kube/" in str(caught.value)


@pytest.mark.parametrize(
    "good_path",
    [
        "~/.kube/configs/foo.yaml",
        "~/.kube/config",
        "~/.aws/credentials",
        "~/.azure/msal_token_cache.json",
        "~/.docker/config.json",
        "~/.config/gcloud/creds.json",
        "~/.config/helm/repositories.yaml",
        "~/.config/sops/age/keys.txt",
        "~/.netrc",
        "~/.pgpass",
        "~/.srw-files/vendor/key.pem",
    ],
)
def test_allowed_paths(good_path):
    out = normalize_credential_files(
        "generic_file",
        "x",
        {"files": [{"contents": "x", "target_path": good_path}]},
    )
    assert out["files"][0]["target_path"] == AGENT_HOME + good_path[1:]


def test_an_ssh_key_keeps_its_ssh_paths():
    """An ssh_key's files are never written: the allowlist is not theirs."""
    out = normalize_credential_files(
        "ssh_key",
        "Deploy",
        {"files": [{"contents": "k", "target_path": "~/.ssh/id_deploy"}]},
    )
    assert out["files"][0]["target_path"] == f"{AGENT_HOME}/.ssh/id_deploy"


def test_etcd_not_treated_as_etc():
    """Regression: ``/etcd/x`` and ``/usr_data`` must not match the /etc/, /usr/ blocklist."""
    # Both are outside writable roots, so they fail — but with a "writable
    # root" error, not a "blocked system root" one. We assert the latter
    # message is NOT raised.
    for path in ("/etcd/x", "/usrlocal/x"):
        with pytest.raises(CredentialFileValidationError) as excinfo:
            normalize_credential_files(
                "generic_file",
                "x",
                {"files": [{"contents": "x", "target_path": path}]},
            )
        assert "blocked system root" not in str(excinfo.value)


# =============================================================================
# mode and env_var formats
# =============================================================================


@pytest.mark.parametrize("good_mode", ["0600", "0644", "0400", "0640"])
def test_mode_accepted(good_mode):
    out = normalize_credential_files(
        "generic_file",
        "x",
        {
            "files": [
                {"contents": "x", "target_path": "~/.srw-files/x", "mode": good_mode}
            ]
        },
    )
    assert out["files"][0]["mode"] == good_mode


@pytest.mark.parametrize(
    "bad_mode",
    ["600", "8888", "rwxrwxrwx", "0999", 0o600, "0755", "0700", "0711", "0601"],
)
def test_mode_rejected(bad_mode):
    with pytest.raises(CredentialFileValidationError, match="mode"):
        normalize_credential_files(
            "generic_file",
            "x",
            {
                "files": [
                    {"contents": "x", "target_path": "~/.srw-files/x", "mode": bad_mode}
                ]
            },
        )


@pytest.mark.parametrize("good_env", ["MY_VAR", "_X", "AWS_PROFILE_2"])
def test_env_var_accepted(good_env):
    out = normalize_credential_files(
        "generic_file",
        "x",
        {
            "files": [
                {
                    "contents": "x",
                    "target_path": "~/.srw-files/x",
                    "env_var": good_env,
                }
            ]
        },
    )
    assert out["files"][0]["env_var"] == good_env


@pytest.mark.parametrize(
    "bad_env",
    [
        "2_LEADING_DIGIT",
        "has space",
        "has-dash",
        "has.dot",
        # The kubeconfig merge owns it; the workspace reserves the rest.
        "KUBECONFIG",
        "PATH",
        "LD_PRELOAD",
        "SRW_TOKEN",
        "PYTHONPATH",
        # A variable that points a tool at the file as a config or code.
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_KEY_0",
        "PIP_CONFIG_FILE",
        "npm_config_userconfig",
        "NODE_OPTIONS",
        "GIT_SSH_COMMAND",
        "EDITOR",
        "PS1",
    ],
)
def test_env_var_rejected(bad_env):
    with pytest.raises(CredentialFileValidationError, match="env_var"):
        normalize_credential_files(
            "generic_file",
            "x",
            {
                "files": [
                    {
                        "contents": "x",
                        "target_path": "~/.srw-files/x",
                        "env_var": bad_env,
                    }
                ]
            },
        )


def test_empty_env_var_dropped():
    """Empty string is treated as "not set" — the env_var key is omitted from output."""
    out = normalize_credential_files(
        "generic_file",
        "x",
        {"files": [{"contents": "x", "target_path": "~/.srw-files/x", "env_var": ""}]},
    )
    assert "env_var" not in out["files"][0]


# =============================================================================
# Type allowlist contract
# =============================================================================


def test_credential_file_types_set():
    """The orchestrator endpoint depends on this exact set."""
    assert CREDENTIAL_FILE_TYPES == frozenset({"kubeconfig", "ssh_key", "generic_file"})
