"""Credential files reach the workspace home (slice D1d).

The workspace program (``INSTALL_CREDENTIAL_FILES``) runs here for real
against a temporary home, with synthetic values only: what it writes, links,
refuses and removes. ``RemoteBackend.install_credential_files`` is driven
through a local shell standing in for the secret stdin channel, and the real
snapshot pipeline proves a captured home keeps the links but never the
bytes.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

import pytest

from orchestrator.services.snapshot_service import _snapshot_tar_pipeline
from shared.runtime.core.backends.remote import RemoteBackend
from shared.runtime.core.credential_env import INSTALL_CREDENTIAL_FILES
from shared.runtime.core.workspace_backend import WorkspaceBackend

STORE = "files-0123abcd"


def _sync(home: Path, files: list[dict], store: str = STORE) -> int:
    return subprocess.run(
        ["python3", "-c", INSTALL_CREDENTIAL_FILES, str(home), store],
        input=json.dumps(files),
        text=True,
        capture_output=True,
    ).returncode


def _file(name: str, link: str | None, content: str = "secret", mode: int = 0o600):
    return {"name": name, "content": content, "mode": mode, "link": link}


@pytest.fixture
def home(tmp_path: Path) -> Path:
    path = tmp_path / "agent-host"
    path.mkdir()
    return path


def _store(home: Path, store: str = STORE) -> Path:
    return home / ".srw-credentials" / store


class TestTheWorkspaceProgram:
    def test_files_go_to_the_private_store_with_their_mode_and_are_linked(self, home):
        assert (
            _sync(
                home,
                [
                    _file("sa.json", ".config/gcloud/sa.json", '{"k": 1}'),
                    _file("ca.pem", ".config/gcloud/ca.pem", "pem", 0o644),
                    _file("unlinked", None, "only in the store"),
                ],
            )
            == 0
        )
        store = _store(home)
        assert store.stat().st_mode & 0o777 == 0o700
        assert (store / "sa.json").stat().st_mode & 0o777 == 0o600
        assert (store / "ca.pem").stat().st_mode & 0o777 == 0o644
        link = home / ".config/gcloud/sa.json"
        assert link.is_symlink()
        assert os.readlink(link) == str(store / "sa.json")
        assert link.read_text() == '{"k": 1}'
        assert (home / ".config/gcloud/ca.pem").read_text() == "pem"
        assert (store / "unlinked").read_text() == "only in the store"
        # Directories the program creates are private.
        assert (home / ".config").stat().st_mode & 0o777 == 0o700

    def test_an_existing_file_or_foreign_link_is_never_replaced(self, home):
        (home / ".kube").mkdir()
        (home / ".kube/config").write_text("the user's own")
        (home / "elsewhere").write_text("x")
        (home / "foreign").symlink_to(home / "elsewhere")
        assert _sync(home, [_file("kube", ".kube/config"), _file("f", "foreign")]) == 0
        assert not (home / ".kube/config").is_symlink()
        assert (home / ".kube/config").read_text() == "the user's own"
        assert os.readlink(home / "foreign") == str(home / "elsewhere")
        # The contents are still delivered, to the store.
        assert (_store(home) / "kube").read_text() == "secret"

    def test_a_live_link_into_another_work_items_store_is_left_alone(self, home):
        assert (
            _sync(home, [_file("a", "shared.txt", "theirs")], store="files-other") == 0
        )
        assert _sync(home, [_file("a", "shared.txt", "mine")]) == 0
        assert (home / "shared.txt").read_text() == "theirs"

    def test_a_stale_link_from_a_restored_snapshot_is_replaced(self, home):
        """A snapshot keeps the link but never the store it points into."""
        (home / ".kube").mkdir()
        (home / ".kube/config").symlink_to(
            home / ".srw-credentials/files-gone/kubeconfig"
        )
        assert _sync(home, [_file("kubeconfig", ".kube/config", "fresh")]) == 0
        assert (home / ".kube/config").read_text() == "fresh"

    def test_a_resync_replaces_its_own_links_and_contents(self, home):
        assert _sync(home, [_file("a", "creds/a", "one")]) == 0
        assert _sync(home, [_file("b", "creds/a", "two")]) == 0
        assert (home / "creds/a").read_text() == "two"
        assert not (_store(home) / "a").exists()

    def test_what_is_no_longer_delivered_is_removed(self, home):
        assert (
            _sync(
                home,
                [_file("a", ".config/tool/a"), _file("b", ".kube/configs/b.yaml")],
            )
            == 0
        )
        (home / ".config/tool/user-file").write_text("mine")
        assert _sync(home, [_file("a", ".config/tool/a")]) == 0
        assert not os.path.lexists(home / ".kube/configs/b.yaml")
        assert not (_store(home) / "b").exists()
        # The directories it made are gone once empty.
        assert not (home / ".kube").exists()
        assert _sync(home, []) == 0
        assert not os.path.lexists(home / ".config/tool/a")
        assert not _store(home).exists()
        # A directory that now holds the user's own file stays.
        assert (home / ".config/tool/user-file").read_text() == "mine"

    @pytest.mark.parametrize(
        "link",
        [
            "../outside",
            "/etc/passwd-like",
            "a/../../outside",
            ".srw-credentials/hijack",
            ".srw-credentials/files-other/x",
        ],
    )
    def test_a_link_never_lands_outside_the_home_or_in_the_store_root(self, home, link):
        assert _sync(home, [_file("x", link)]) == 0
        assert not (home.parent / "outside").exists()
        assert not os.path.lexists(home / ".srw-credentials/hijack")
        assert not os.path.lexists(home / ".srw-credentials/files-other/x")
        assert (_store(home) / "x").read_text() == "secret"

    def test_a_link_never_follows_a_symlinked_directory_out_of_the_home(
        self, home, tmp_path
    ):
        outside = tmp_path / "outside"
        outside.mkdir()
        (home / "escape").symlink_to(outside)
        assert _sync(home, [_file("x", "escape/planted")]) == 0
        assert not os.path.lexists(outside / "planted")

    @pytest.mark.parametrize("name", ["", "../x", "a/b", ".links.json"])
    def test_a_malformed_store_name_fails_closed(self, home, name):
        assert _sync(home, [_file(name, None)]) == 3


class TestRemoteDelivery:
    def _backend(self, monkeypatch, home: Path, job_id: str, sent: list):
        backend = RemoteBackend(host="unused", job_id=job_id)
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))

        def send(command, secret, **kwargs):
            sent.append((command, secret))
            return (
                subprocess.run(
                    ["bash", "-c", command], input=secret, text=True
                ).returncode
                == 0
            )

        monkeypatch.setattr(backend, "execute_claim_resource_with_secret_stdin", send)
        return backend

    def test_contents_travel_on_secret_stdin_into_the_work_items_store(
        self, monkeypatch, home
    ):
        sent: list = []
        stores = []
        for job_id, value in (("job-one", "first-value"), ("job-two", "second")):
            backend = self._backend(monkeypatch, home, job_id, sent)
            store = backend.install_credential_files(
                [{"name": "f", "content": value, "mode": 0o600, "link": None}]
            )
            command, secret = sent[-1]
            assert value not in command
            assert json.loads(secret)[0]["content"] == value
            assert Path(store, "f").read_text() == value
            assert Path(store).parent == home / ".srw-credentials"
            stores.append(store)
        assert stores[0] != stores[1]

    def test_a_failed_sync_raises(self, monkeypatch, home):
        from shared.runtime.core.workspace_backend import WorkspaceUnavailableError

        backend = RemoteBackend(host="unused", job_id="job")
        monkeypatch.setattr(backend, "_init_shell", lambda: None)
        monkeypatch.setattr(backend, "_get_home_dir", lambda: str(home))
        monkeypatch.setattr(
            backend,
            "execute_claim_resource_with_secret_stdin",
            lambda *args, **kwargs: False,
        )
        with pytest.raises(WorkspaceUnavailableError):
            backend.install_credential_files([])

    def test_a_backend_without_a_shell_refuses(self):
        with pytest.raises(ValueError, match="sandbox or VM workspace"):
            WorkspaceBackend.install_credential_files(object(), [])


def test_a_snapshot_keeps_the_links_but_never_the_contents(tmp_path):
    """C0's excludes cover where the files land: the real capture pipeline.

    The capture excludes ``/tmp/*``, so the fixture home lives under the
    user's home directory, like the production root.
    """
    for tool in ("tar", "zstd", "bash"):
        if shutil.which(tool) is None:
            pytest.skip(f"{tool} not available")
    secret = "d1d-snapshot-secret-6f0c2b"
    with tempfile.TemporaryDirectory(
        prefix="snapshot-credential-files-", dir=os.path.expanduser("~")
    ) as root:
        home = Path(root) / "agent-host"
        home.mkdir()
        (home / "notes.md").write_text("kept\n")
        assert (
            _sync(
                home,
                [
                    _file("kubeconfig", ".kube/config", secret),
                    _file("sa.json", ".config/gcloud/sa.json", secret),
                ],
            )
            == 0
        )
        for strict in (False, True):
            archive = tmp_path / f"home-{strict}.tar.zst"
            with archive.open("wb") as output:
                completed = subprocess.run(
                    [
                        "bash",
                        "-c",
                        _snapshot_tar_pipeline([f"{home}/"], strict_terminal=strict),
                    ],
                    stdout=output,
                )
            assert completed.returncode in (0, 1)
            raw = subprocess.run(
                ["zstd", "-dc", "--", str(archive)], capture_output=True, check=True
            ).stdout
            listing = subprocess.run(
                ["tar", "-tvf", "-"], input=raw, capture_output=True, check=True
            ).stdout.decode()
            assert secret.encode() not in raw
            members = [line.split(" -> ")[0] for line in listing.splitlines() if line]
            assert not [m for m in members if "/.srw-credentials" in m], members
            links = [line for line in listing.splitlines() if line.startswith("l")]
            assert any("/.kube/config ->" in line for line in links), listing
            assert any("/.config/gcloud/sa.json ->" in line for line in links)
            assert "agent-host/notes.md" in listing
