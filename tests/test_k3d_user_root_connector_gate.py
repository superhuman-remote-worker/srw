"""Safety contract and pure helpers of the local slice 4 user-root gate (never
run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts" / "k3d-user-root-connector-gate.py"
)
_SPEC = importlib.util.spec_from_file_location("k3d_user_root_connector_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

ROOT_ID = "00000000-0000-4000-8000-0000000000f9"
UID = "a" * 64


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
        ["--gate-id", "ur-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--nextcloud-url", "https://cloud.example.com"],
        ["--nextcloud-url", "http://cloud.localhost"],
        ["--pod-timeout", "5"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_the_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    assert "Plan (dry run" in out
    assert len(gate.PLAN) == out.count("\n  - ")


def test_the_served_modules_exist():
    for path in gate.SERVED:
        assert (gate.ROOT / path).is_file(), path


def test_the_nextcloud_uid_has_user_oidcs_shape_and_is_the_gates_own():
    uid = gate.nextcloud_uid("ur-0123456789")
    assert gate._NC_UID_RE.fullmatch(uid) and len(uid) == 64
    assert uid == gate.nextcloud_uid("ur-0123456789")
    assert uid != gate.nextcloud_uid("ur-0123456780")
    assert gate._NC_UID_RE.fullmatch("ur-0123456789-eve")
    for other in ("admin", "agent-service", "srw-writer-f-" + "0" * 32, "-x"):
        assert not gate._NC_UID_RE.fullmatch(other)


def test_device_tokens_are_counted_by_name():
    listing = [
        {"id": 3, "name": "Superhuman Remote Worker"},
        {"id": 4, "name": "Mozilla/5.0 (user root gate)"},
        {"id": 5, "name": "Superhuman Remote Worker"},
    ]
    assert [t["id"] for t in gate.device_tokens(listing)] == [3, 5]
    assert gate.device_tokens({"error": "x"}) == []


def test_the_login_flow_pages_are_read_as_nextcloud_31_renders_them():
    login = '<head data-requesttoken="ab/c&#x3D;:d+e=">'
    assert gate.page_request_token(login) == "ab/c=:d+e="
    picker = (
        '<form id="login-form" action="https://cloud.localhost/login/v2/grant?'
        'user=&amp;direct=0&amp;stateToken=ST" method="get">'
    )
    assert gate.picker_action(picker) == (
        "https://cloud.localhost/login/v2/grant?user=&direct=0&stateToken=ST"
    )
    grant = (
        '<input type="hidden" name="requesttoken" value="r&#x2F;t" />'
        '<input type="hidden" name="stateToken" value="ST" />'
    )
    assert gate.form_value(grant, "requesttoken") == "r/t"
    assert gate.form_value(grant, "stateToken") == "ST"
    assert gate.form_value(grant, "missing") is None


def _plan(mounts=(), excluded=()):
    return {"mounts": list(mounts), "excluded": list(excluded)}


def _root_mount(**over):
    mount = {
        "index": 0,
        "name": "my_cloud",
        "mount_id": ROOT_ID,
        "mount_kind": "user_root",
        "source_ref": ROOT_ID,
        "access": "read_write",
        "remote": {
            "type": "webdav",
            "url": f"http://srw-nextcloud/remote.php/dav/files/{UID}/",
            "vendor": "nextcloud",
            "user": UID,
            "root": "",
        },
    }
    mount.update(over)
    return mount


def test_the_roots_mount_is_checked_field_by_field():
    assert gate.root_mount_problems(_plan([_root_mount()]), ROOT_ID, UID) == []
    chunked_off = _root_mount()
    chunked_off["remote"] = {**chunked_off["remote"], "nextcloud_chunk_size": "0"}
    as_service = _root_mount()
    as_service["remote"] = {**as_service["remote"], "user": "agent-service"}
    for mount, problem in (
        (chunked_off, "chunked uploads are off"),
        (as_service, "user 'agent-service'"),
        (_root_mount(name="home"), "name 'home'"),
        (_root_mount(access="read_only"), "user_root/read_only"),
    ):
        assert problem in gate.root_mount_problems(_plan([mount]), ROOT_ID, UID)
    assert gate.root_mount_problems(_plan(), ROOT_ID, UID) == ["0 mounts of the root"]


def test_an_exclusion_and_a_substitute_are_found():
    plan = _plan(
        mounts=[{"name": "home", "mount_kind": "session_folder", "mount_id": "x"}],
        excluded=[
            {
                "source_ref": ROOT_ID,
                "mount_kind": "user_root",
                "reason": "cloud_not_connected",
            }
        ],
    )
    assert gate.root_exclusion(plan, ROOT_ID) == "cloud_not_connected"
    assert gate.root_exclusion(plan, "other") is None
    assert gate.substitute_problems(plan) == ["home (session_folder)"]
    assert gate.substitute_problems(_plan([_root_mount()])) == []


def test_a_recorded_plan_and_its_secret_are_read_from_the_pod():
    pod = {
        "metadata": {"annotations": {gate.PLAN_ANNOTATION: json.dumps(_plan())}},
        "spec": {
            "volumes": [
                {"name": gate.CREDENTIAL_VOLUME, "secret": {"secretName": "s-1"}}
            ]
        },
    }
    assert gate.recorded_plan(pod) == _plan()
    assert gate.credential_secret_name(pod) == "s-1"
    assert gate.recorded_plan({"metadata": {"annotations": {}}}) is None


def test_secrets_are_scrubbed_from_every_printed_line():
    value = gate.secret("app-password-SECRET-for-scrub")
    assert gate._scrub(f"x {value} y") == "x <redacted> y"


def test_the_hygiene_query_refuses_anything_but_a_plain_needle(no_cluster):
    with pytest.raises(gate.GateError):
        gate.sql_count_containing("threads", "metadata", "a'; DROP TABLE x; --")
    with pytest.raises(gate.GateError):
        gate.sql_count_containing("threads; x", "metadata", "abcdefgh")


def test_the_workspace_mount_table_is_read_top_of_stack():
    mountinfo = (
        "36 25 0:32 / /cloud rw,relatime - tmpfs tmpfs rw\n"
        "41 36 0:40 / /cloud/my_cloud rw,nosuid - fuse.rclone m0: rw\n"
        "42 36 0:41 / /cloud/my\\040cloud rw - fuse.rclone m1: rw\n"
    )
    tops = gate.top_mounts(mountinfo)
    assert tops["/cloud/my_cloud"][0] == "fuse.rclone"
    assert tops["/cloud/my cloud"][0] == "fuse.rclone"
    assert tops["/cloud"][0] == "tmpfs"
