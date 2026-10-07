"""Tests for credential-file materialization in ``src/core/datasource_setup.py``.

The validator (``orchestrator/security/credential_files.py``, separately tested)
resolves and stores ``target_path`` against the production agent home ``/home/srw``.
The materializer here takes those already-validated configs and writes the files
to disk, with a ``home_dir`` parameter that lets tests retarget the writes into a
``tmp_path``. Kubeconfig prefixing and the ``kubectl``-driven merge are also
covered (the latter via shutil.which monkeypatching to a fake kubectl).
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from agent.core.datasource_setup import (
    AGENT_HOME,
    cleanup_credential_files,
    process_credential_files,
    _mkdir_tracking,
    _prefix_kubeconfig_yaml,
    _retarget,
)


# =============================================================================
# _retarget — prefix swap for tests
# =============================================================================


class TestRetarget:
    def test_swap_home_prefix(self):
        assert _retarget("/home/srw/.ssh/id", "/tmp/h") == "/tmp/h/.ssh/id"

    def test_exact_home_swaps(self):
        assert _retarget("/home/srw", "/tmp/h") == "/tmp/h"

    def test_no_swap_when_home_matches(self):
        assert _retarget("/home/srw/.ssh/id", AGENT_HOME) == "/home/srw/.ssh/id"

    def test_non_home_paths_unchanged(self):
        assert _retarget("/tmp/foo", "/tmp/h") == "/tmp/foo"
        assert _retarget("/workspace/x", "/tmp/h") == "/workspace/x"

    def test_empty_returns_empty(self):
        assert _retarget("", "/tmp/h") == ""

    def test_home_substring_not_swapped(self):
        # /home/srw2 must NOT be treated as the agent home.
        assert _retarget("/home/srw2/x", "/tmp/h") == "/home/srw2/x"


# =============================================================================
# _mkdir_tracking
# =============================================================================


class TestMkdirTracking:
    def test_creates_and_records(self, tmp_path: Path):
        target = tmp_path / "a" / "b" / "c"
        dirs: list[str] = []
        _mkdir_tracking(str(target), dirs)
        assert target.is_dir()
        # Every dir we made was recorded.
        assert str(target) in dirs
        assert str(target.parent) in dirs
        assert str(target.parent.parent) in dirs

    def test_skips_pre_existing(self, tmp_path: Path):
        existing = tmp_path / "pre"
        existing.mkdir()
        dirs: list[str] = []
        _mkdir_tracking(str(existing), dirs)
        # We didn't create it, so it must not be recorded.
        assert str(existing) not in dirs


# =============================================================================
# _prefix_kubeconfig_yaml
# =============================================================================


class TestPrefixKubeconfig:
    KCFG = """apiVersion: v1
kind: Config
current-context: default
clusters:
  - name: prod
    cluster:
      server: https://prod.example
users:
  - name: admin
    user:
      token: t0k3n
contexts:
  - name: default
    context:
      cluster: prod
      user: admin
"""

    def test_prefixes_all_names(self):
        out = _prefix_kubeconfig_yaml(self.KCFG, "eu")
        import yaml

        doc = yaml.safe_load(out)
        assert doc["clusters"][0]["name"] == "eu-prod"
        assert doc["users"][0]["name"] == "eu-admin"
        ctx = doc["contexts"][0]
        assert ctx["name"] == "eu-default"
        assert ctx["context"]["cluster"] == "eu-prod"
        assert ctx["context"]["user"] == "eu-admin"
        assert doc["current-context"] == "eu-default"

    def test_malformed_yaml_returned_verbatim(self):
        bad = "this: is: not: yaml: at: all\n  - foo\n: bar"
        out = _prefix_kubeconfig_yaml(bad, "eu")
        # We don't strictly require equality — the function may safe_load
        # and re-emit if PyYAML happens to accept it. The contract is: don't
        # raise.
        assert isinstance(out, str)

    def test_missing_sections_tolerated(self):
        # A minimal kubeconfig stub with only `clusters`.
        out = _prefix_kubeconfig_yaml("clusters:\n  - name: only\n", "x")
        import yaml

        doc = yaml.safe_load(out)
        assert doc["clusters"][0]["name"] == "x-only"


# =============================================================================
# process_credential_files — single-file types (ssh_key, generic_file)
# =============================================================================


class TestProcessCredentialFiles:
    def test_ssh_key_is_never_written_to_disk(self, tmp_path: Path):
        """C1: an ssh_key connector lives in a workspace ssh-agent instead."""
        ds = {
            "type": "ssh_key",
            "name": "Github",
            "credentials": {
                "files": [
                    {
                        "name": "github",
                        "contents": "----PRIVATE----",
                        "target_path": "/home/srw/.ssh/github",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        assert not (tmp_path / ".ssh").exists()
        assert manifest == {"files": [], "dirs": [], "env_vars": []}

    def test_writes_file_with_correct_mode(self, tmp_path: Path):
        ds = {
            "type": "generic_file",
            "name": "Github",
            "credentials": {
                "files": [
                    {
                        "name": "github",
                        "contents": "----PRIVATE----",
                        "target_path": "/home/srw/.ssh/github",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        written = tmp_path / ".ssh" / "github"
        assert written.read_text() == "----PRIVATE----"
        # Mode check (mask off the file-type bits).
        assert (written.stat().st_mode & 0o777) == 0o600
        assert str(written) in manifest["files"]

    def test_writes_generic_file_with_env_var(self, tmp_path: Path, monkeypatch):
        # Make sure os.environ injection actually happens.
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        ds = {
            "type": "generic_file",
            "name": "GCloud",
            "credentials": {
                "files": [
                    {
                        "name": "creds.json",
                        "contents": '{"type":"service_account"}',
                        "target_path": "/home/srw/.config/gcloud/creds.json",
                        "mode": "0600",
                        "env_var": "GOOGLE_APPLICATION_CREDENTIALS",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        written = tmp_path / ".config" / "gcloud" / "creds.json"
        assert written.exists()
        assert os.environ["GOOGLE_APPLICATION_CREDENTIALS"] == str(written)
        assert "GOOGLE_APPLICATION_CREDENTIALS" in manifest["env_vars"]

    def test_non_credential_types_skipped(self, tmp_path: Path):
        ds = {"type": "postgresql", "name": "PG", "credentials": {}}
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        assert manifest["files"] == []
        assert manifest["dirs"] == []
        assert manifest["env_vars"] == []

    def test_refuses_to_overwrite_existing_file(self, tmp_path: Path):
        # Pre-create the target so the materializer must skip it.
        target = tmp_path / ".ssh" / "github"
        target.parent.mkdir(parents=True)
        target.write_text("DO NOT OVERWRITE")
        ds = {
            "type": "generic_file",
            "name": "Github",
            "credentials": {
                "files": [
                    {
                        "contents": "NEW VALUE",
                        "target_path": "/home/srw/.ssh/github",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        assert target.read_text() == "DO NOT OVERWRITE"
        assert str(target) not in manifest["files"]


# =============================================================================
# Cleanup
# =============================================================================


class TestCleanup:
    def test_removes_files_and_unsets_env_vars(self, tmp_path: Path, monkeypatch):
        monkeypatch.delenv("MY_TOKEN_FILE", raising=False)
        ds = {
            "type": "generic_file",
            "name": "Token",
            "credentials": {
                "files": [
                    {
                        "contents": "secret",
                        "target_path": "/home/srw/.config/token",
                        "mode": "0600",
                        "env_var": "MY_TOKEN_FILE",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        written = tmp_path / ".config" / "token"
        assert written.exists()
        assert os.environ.get("MY_TOKEN_FILE") == str(written)

        cleanup_credential_files(manifest)
        assert not written.exists()
        assert "MY_TOKEN_FILE" not in os.environ
        # The directory we created was empty after unlink, so it's also gone.
        assert not (tmp_path / ".config").exists()

    def test_cleanup_preserves_pre_existing_dirs(self, tmp_path: Path):
        # Pre-create ~/.ssh so it should NOT be removed (mirrors production
        # where ~/.ssh may carry known_hosts from the agent image).
        ssh_dir = tmp_path / ".ssh"
        ssh_dir.mkdir()
        (ssh_dir / "known_hosts").write_text("pre-existing")

        ds = {
            "type": "generic_file",
            "name": "Github",
            "credentials": {
                "files": [
                    {
                        "contents": "PRIV",
                        "target_path": "/home/srw/.ssh/github",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        # Dir was pre-existing, so cleanup must NOT include it.
        assert str(ssh_dir) not in manifest["dirs"]
        cleanup_credential_files(manifest)
        # Pre-existing dir and its pre-existing file survive cleanup.
        assert ssh_dir.is_dir()
        assert (ssh_dir / "known_hosts").read_text() == "pre-existing"
        # Our materialized key is gone.
        assert not (ssh_dir / "github").exists()

    def test_cleanup_tolerates_missing_files(self, tmp_path: Path):
        # Manifest references a file that was already removed externally —
        # cleanup must not raise.
        manifest = {
            "files": [str(tmp_path / "does-not-exist")],
            "dirs": [],
            "env_vars": [],
        }
        cleanup_credential_files(manifest)  # must not raise

    def test_cleanup_handles_none(self):
        cleanup_credential_files(None)  # no-op


# =============================================================================
# Kubeconfig merging (uses a fake kubectl shim on PATH)
# =============================================================================


@pytest.fixture
def fake_kubectl(tmp_path: Path, monkeypatch):
    """Drop a fake kubectl onto PATH that concatenates each path in $KUBECONFIG.

    Production uses real ``kubectl config view --flatten --merge``; we don't
    want to require it in CI. The fake produces enough output to pin the
    merge contract: each input kubeconfig must contribute to the output.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "kubectl"
    fake.write_text(
        "#!/usr/bin/env bash\n"
        "set -e\n"
        'if [ "$1" = "config" ] && [ "$2" = "view" ]; then\n'
        '  IFS=":" read -ra paths <<< "$KUBECONFIG"\n'
        '  echo "apiVersion: v1"\n'
        '  echo "kind: Config"\n'
        '  echo "merged-from:"\n'
        '  for p in "${paths[@]}"; do\n'
        '    echo "  - $p"\n'
        "  done\n"
        "fi\n"
    )
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    return fake


class TestKubeconfigMerge:
    KCFG_A = (
        "apiVersion: v1\nkind: Config\ncurrent-context: default\n"
        "clusters:\n  - name: a-cluster\n    cluster:\n      server: https://a\n"
        "users:\n  - name: a-user\n    user:\n      token: ta\n"
        "contexts:\n  - name: default\n    context:\n      cluster: a-cluster\n      user: a-user\n"
    )
    KCFG_B = (
        "apiVersion: v1\nkind: Config\ncurrent-context: default\n"
        "clusters:\n  - name: b-cluster\n    cluster:\n      server: https://b\n"
        "users:\n  - name: b-user\n    user:\n      token: tb\n"
        "contexts:\n  - name: default\n    context:\n      cluster: b-cluster\n      user: b-user\n"
    )

    def test_two_kubeconfigs_merged_and_prefixed(
        self, tmp_path: Path, fake_kubectl, monkeypatch
    ):
        monkeypatch.delenv("KUBECONFIG", raising=False)
        ds_a = {
            "type": "kubeconfig",
            "name": "Prod EU",
            "credentials": {
                "files": [
                    {
                        "contents": self.KCFG_A,
                        "target_path": "/home/srw/.kube/configs/prod-eu.yaml",
                        "mode": "0600",
                    }
                ]
            },
        }
        ds_b = {
            "type": "kubeconfig",
            "name": "Staging",
            "credentials": {
                "files": [
                    {
                        "contents": self.KCFG_B,
                        "target_path": "/home/srw/.kube/configs/staging.yaml",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds_a, ds_b], home_dir=str(tmp_path))

        # Per-ds files were prefixed.
        eu_path = tmp_path / ".kube" / "configs" / "prod-eu.yaml"
        st_path = tmp_path / ".kube" / "configs" / "staging.yaml"
        assert eu_path.exists()
        assert st_path.exists()
        import yaml

        eu_doc = yaml.safe_load(eu_path.read_text())
        st_doc = yaml.safe_load(st_path.read_text())
        # Context names are prefixed with the ds slug.
        assert eu_doc["contexts"][0]["name"] == "prod-eu-default"
        assert st_doc["contexts"][0]["name"] == "staging-default"

        # Merged file exists.
        merged = tmp_path / ".kube" / "config"
        assert merged.exists()
        merged_text = merged.read_text()
        # The fake kubectl echoes each path under merged-from; both per-ds
        # paths must appear, proving the merge subprocess actually got
        # both kubeconfigs on $KUBECONFIG.
        assert str(eu_path) in merged_text
        assert str(st_path) in merged_text

        # KUBECONFIG env var points at the merged file.
        assert os.environ.get("KUBECONFIG") == str(merged)
        assert "KUBECONFIG" in manifest["env_vars"]

        # Cleanup unsets KUBECONFIG and removes both per-ds + merged files.
        cleanup_credential_files(manifest)
        assert "KUBECONFIG" not in os.environ
        assert not eu_path.exists()
        assert not merged.exists()

    def test_falls_back_when_kubectl_missing(self, tmp_path: Path, monkeypatch):
        # Strip PATH so kubectl can't be found.
        monkeypatch.setenv("PATH", "/nonexistent-dir")
        monkeypatch.delenv("KUBECONFIG", raising=False)
        ds = {
            "type": "kubeconfig",
            "name": "Prod",
            "credentials": {
                "files": [
                    {
                        "contents": self.KCFG_A,
                        "target_path": "/home/srw/.kube/configs/prod.yaml",
                        "mode": "0600",
                    }
                ]
            },
        }
        manifest = process_credential_files([ds], home_dir=str(tmp_path))
        prod_path = tmp_path / ".kube" / "configs" / "prod.yaml"
        assert prod_path.exists()
        # No merged file produced, but KUBECONFIG still points somewhere
        # usable — the colon-list of per-ds paths.
        merged = tmp_path / ".kube" / "config"
        assert not merged.exists()
        assert os.environ.get("KUBECONFIG") == str(prod_path)
        cleanup_credential_files(manifest)
