"""Safety contract and expectations of the local folder grants gate (never run here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.services.cloud import NextcloudBackend, ProjectFolderHandle
from orchestrator.services.cloud.config import NextcloudSettings
from orchestrator.services.cloud.folder_grants import principal_ids

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "k3d-folder-grants-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_folder_grants_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

BASE = "http://srw-nextcloud"
WRITER, _ = principal_ids("writer", "11111111-1111-4111-8111-111111111111")
READER, _ = principal_ids("reader", "11111111-1111-4111-8111-111111111111")


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "d4-0123456789"],
        ["--gate-id", "fg-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--pod-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_the_dry_run_prints_the_plan_only(no_cluster, capsys):
    assert gate.main([]) == 0
    assert "writer:" in capsys.readouterr().out


def test_the_expected_url_is_the_adapters():
    backend = NextcloudBackend(
        NextcloudSettings(
            base_url=BASE,
            public_url="https://cloud.localhost",
            admin_user="admin",
            admin_password="admin",
            agent_user="agent-service",
            agent_password="pw",
        )
    )
    handle = ProjectFolderHandle(
        backend="nextcloud", native_id="7", vendor_meta={"mountpoint": "fg x home"}
    )
    assert backend.folder_grant_webdav_url(handle, WRITER) == gate.grant_url(
        BASE, WRITER, "fg x home"
    )


def _mount(user: str, *, access="read_write", mountpoint="home", chunk="0"):
    return {
        "index": 1,
        "name": "home",
        "mount_id": "c1",
        "mount_kind": "project",
        "access": access,
        "remote": {
            "type": "webdav",
            "url": gate.grant_url(BASE, user, mountpoint),
            "vendor": "nextcloud",
            "user": user,
            "nextcloud_chunk_size": chunk,
            "root": "",
        },
    }


def test_a_writer_mount_is_checked_against_its_grant():
    def problems(mount, kind="writer", access="read_write"):
        return gate.mount_problems(
            mount, kind=kind, access=access, base_url=BASE, mountpoint="home"
        )

    assert problems(_mount(WRITER)) == []
    assert problems(None) == ["the folder is not mounted"]
    assert problems(_mount("agent-service"))
    assert problems(_mount(READER, access="read_only"))
    assert problems(_mount(READER, access="read_only"), "reader", "read_only") == []
    assert problems(_mount(WRITER, chunk="10M"))
    wrong = _mount(WRITER)
    wrong["remote"]["url"] = f"{BASE}/remote.php/dav/files/{WRITER}/home/"
    assert problems(wrong)


def test_service_account_project_mounts_are_named():
    outbox = {**_mount("agent-service"), "mount_kind": "cloud_outbox", "name": "outbox"}
    plan = {"mounts": [_mount(WRITER), outbox]}
    assert gate.service_account_mounts(plan, "agent-service") == []
    plan["mounts"].append({**_mount("agent-service"), "name": "legacy"})
    assert gate.service_account_mounts(plan, "agent-service") == ["legacy"]


def test_a_writer_lists_its_own_folder_only():
    root = f"/remote.php/dav/groupfolders/{WRITER}/"
    assert gate.listing_problems([root, f"{root}fg%20home/"], WRITER, "fg home") == []
    assert gate.listing_problems(
        [root, f"{root}fg%20home/", f"{root}other/"], WRITER, "fg home"
    )
    assert gate.listing_problems([root], WRITER, "fg home")


def test_plan_lookups_go_by_connector():
    plan = {
        "mounts": [_mount(WRITER)],
        "excluded": [{"source_ref": "c2", "reason": "access_unavailable"}],
    }
    assert gate.plan_mount(plan, "c1")["name"] == "home"
    assert gate.plan_mount(plan, "c2") is None
    assert gate.plan_exclusion(plan, "c2")["reason"] == "access_unavailable"


def test_the_password_round_trips_rclones_obscuring():
    from orchestrator.services.cloud_mount_plan import rclone_obscure

    assert gate.reveal(rclone_obscure("s3cret pass")) == "s3cret pass"
