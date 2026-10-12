"""Safety contract, Pod shapes and evaluators of the D7 in-pod plane spike
(scripts/k3d-in-pod-plane-spike.py; never run against a cluster here)."""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-in-pod-plane-spike.py"
_SPEC = importlib.util.spec_from_file_location("k3d_in_pod_plane_spike", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
spike = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = spike
_SPEC.loader.exec_module(spike)

OPENER = "srw-registry:5000/srw-spike-d7-opener:d7-0123456789"
RCLONE = "srw-registry:5000/srw-spike-d7-rclone-shim:d7-0123456789"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "srw"],
        ["--namespace", "srw-connectors"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", spike.LOCAL_CONFIRMATION],
        ["--run-id", "d6-0123456789"],
        ["--run-id", "d7-xyz"],
        ["--only", "b,elsewhere"],
        ["--only", ","],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert spike.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert spike.main([]) == 0
    out = capsys.readouterr().out
    for phase in ("preflight", "a:", "a-hang", "b:", "b-crash", "b1:", "privws"):
        assert phase in out
    assert "cleanup" in out


def test_only_selects_known_phases():
    args = spike.build_parser().parse_args(["--only", "b, b-hang"])
    assert spike.phases_of(args) == {"b", "b-hang"}
    assert spike.phases_of(spike.build_parser().parse_args([])) == spike.PHASES


def _init(pod: dict, name: str) -> dict:
    return next(c for c in pod["spec"]["initContainers"] if c["name"] == name)


def test_approach_a_runs_rclone_privileged_with_bidirectional_propagation():
    pod = spike.pod_a("a-mount", "upstream:1")
    sidecar = _init(pod, "cloud-mount")
    assert sidecar["securityContext"] == {"privileged": True}
    assert sidecar["restartPolicy"] == "Always"
    assert {
        "name": "cloud",
        "mountPath": "/srw/cloud",
        "mountPropagation": "Bidirectional",
    } in sidecar["volumeMounts"]
    workspace = pod["spec"]["containers"][0]
    assert workspace["volumeMounts"] == [
        {
            "name": "cloud",
            "mountPath": "/home/agent/cloud",
            "mountPropagation": "HostToContainer",
        }
    ]
    assert workspace["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    # A restarted sidecar detaches the dead mount before it stats the target.
    script = sidecar["command"][2]
    assert script.index("umount -l") < script.index('mkdir -p "$T"')


def test_approach_b_is_the_prototype_builders_pod_with_debug_logging():
    pod = spike.pod_b("b-mount", OPENER, RCLONE, term_delay=8)
    assert [c["name"] for c in pod["spec"]["initContainers"]] == [
        "srw-fuse-opener",
        "srw-cloud-mount",
    ]
    rclone = _init(pod, "srw-cloud-mount")
    assert rclone["command"] == ["rclone"]
    assert rclone["args"][-3:] == ["-vv", "--log-file", "/tmp/rclone.log"]
    assert "privileged" not in rclone["securityContext"]
    env = {e["name"]: e["value"] for e in pod["spec"]["containers"][0]["env"]}
    assert env == {"CLOUD_ROOT": "/cloud", "CLOUD_NAME": "root", "TERM_DELAY": "8"}


def test_the_devfd_variant_execs_rclone_on_fd_3_through_the_opener_client():
    pod = spike.pod_b("b1", OPENER, RCLONE, devfd=True)
    rclone = _init(pod, "srw-cloud-mount")
    assert rclone["command"][:2] == ["srw-fuse-opener", "exec"]
    assert rclone["command"][-1] == "rclone"
    assert rclone["args"][:3] == ["mount2", "cloud:", "/dev/fd/3"]
    assert "--allow-non-empty" in rclone["args"]
    bare = _init(
        spike.pod_b("b1n", OPENER, RCLONE, devfd=True, allow_non_empty=False),
        "srw-cloud-mount",
    )
    assert "--allow-non-empty" not in bare["args"]


def test_the_privileged_workspace_variant_is_todays_profile():
    pod = spike.pod_b("p", OPENER, RCLONE, privileged_workspace=True)
    assert pod["spec"]["containers"][0]["securityContext"] == {"privileged": True}


def test_pod_mounts_match_the_mountpoint_field_only():
    uid = "11111111-2222-4333-8444-555555555555"
    pod_dir = f"/var/lib/kubelet/pods/{uid}/volumes/kubernetes.io~empty-dir"
    mountinfo = "\n".join(
        [
            f"1 2 0:1 / {pod_dir}/srw-cloud rw - tmpfs tmpfs rw",
            f"3 1 0:2 / {pod_dir}/srw-cloud/root ro - fuse.rclone srw-cloud ro",
            # The uid in another field (a bind's root, an option) is not this Pod's.
            f"4 2 0:3 /pods/{uid} /var/lib/other rw - fuse.rclone x rw,{uid}",
            f"5 2 0:4 / /var/lib/kubelet/pods/{uid}x/volumes/v rw - fuse.x y rw",
        ]
    )
    assert spike.pod_mounts(mountinfo, uid) == [
        (f"{pod_dir}/srw-cloud", "tmpfs"),
        (f"{pod_dir}/srw-cloud/root", "fuse.rclone"),
    ]


def _recording_cluster(monkeypatch, answers):
    """Fake subprocess.run: record every command, answer by its first words."""
    calls: list[list[str]] = []

    def fake_run(argv, **kwargs):
        calls.append(list(argv))
        joined = " ".join(argv)
        for needle, (code, out) in answers.items():
            if needle in joined:
                return subprocess.CompletedProcess(argv, code, out, "")
        return subprocess.CompletedProcess(argv, 0, "", "")

    monkeypatch.setattr(subprocess, "run", fake_run)
    return calls


def test_preflight_aborts_before_any_docker_exec_without_the_node(monkeypatch):
    calls = _recording_cluster(
        monkeypatch,
        {
            "get-contexts": (0, "k3d-srw\n"),
            "get node k3d-srw-server-0": (1, ""),
        },
    )
    run = spike.Spike(run_id="d7-0123456789")
    assert run.run() == 1
    assert not any(c[:2] == ["docker", "exec"] for c in calls)
    assert not any("create" in c or "delete" in c or "rm" in c for c in calls)
    assert [name for name, ok, _ in run.results if not ok] == ["spike"]


def test_cleanup_deletes_only_what_the_run_created(monkeypatch):
    calls = _recording_cluster(monkeypatch, {"get namespace": (1, "")})
    run = spike.Spike(run_id="d7-0123456789")
    run.cleanup()
    assert not any("delete" in c for c in calls)
    assert not any("rm" in c or "rmi" in c for c in calls)


def test_cleanup_is_best_effort(monkeypatch):
    def failing_run(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, 1)

    monkeypatch.setattr(subprocess, "run", failing_run)
    run = spike.Spike(run_id="d7-0123456789", created_namespace=True)
    run.pod_uids = {"11111111-2222-4333-8444-555555555555"}
    run.owned_repositories = {"srw-spike-d7-opener"}
    run.images = {"opener": "localhost:5005/srw-spike-d7-opener:d7-0123456789"}
    run.pushed_keys = ["opener"]
    run.built_refs = ["localhost:5005/srw-spike-d7-opener:d7-0123456789"]
    run.cleanup()  # every step fails, none raises
    [(name, ok, detail)] = run.results
    assert name == "cleanup" and not ok
    for step in (
        "detach",
        "delete the namespace",
        "registry",
        "node image",
        "local tag",
    ):
        assert step in detail


def test_fixture_secret_carries_the_password_and_an_rclone_config():
    secret, pod, service = spike.fixture_manifests("pw", spike.rclone_config("OBSC"))
    assert set(secret["data"]) == {"password", "rclone.conf"}
    assert pod["spec"]["securityContext"]["runAsUser"] == 65534
    assert service["spec"]["selector"] == {"app": "webdav"}
    assert "pass = OBSC" in spike.rclone_config("OBSC")


# Outputs recorded on k3d-srw (2026-10-08) by the spike's probes.
PROBE_OK = """== read
hello from the fixture
== create
touch: /cloud/root/new.txt: Read-only file system
== overwrite
sh: can't create /cloud/root/notes.txt: Read-only file system
== mkdir
mkdir: can't create directory '/cloud/root/d': Read-only file system
== rm
rm: can't remove '/cloud/root/notes.txt': Read-only file system
== remount
mount: permission denied (are you root?)
rc=1
== umount
umount: can't unmount /cloud/root: Operation not permitted
rc=1
== emptydir
root
== procs
/bin/sh -c
/bin/sh -s -- /cloud/root
sleep 1
== env
none
== credfile
ls: /etc/srw-cloud: No such file or directory
== listen
== caps
CapEff:\t0000000000000000
== end
"""


def test_evaluators_accept_the_recorded_good_probe():
    assert spike.evaluate_readonly(PROBE_OK) == []
    assert spike.evaluate_isolation(PROBE_OK, "root") == []


def test_evaluators_name_each_broken_property():
    broken = (
        PROBE_OK.replace(
            "touch: /cloud/root/new.txt: Read-only file system", "touch: ok"
        )
        .replace("mount: permission denied (are you root?)\nrc=1", "rc=0")
        .replace("sleep 1", "rclone mount2 cloud: /srw/cloud/root")
        .replace("== env\nnone", "== env\nRCLONE_WEBDAV_PASS=x")
        .replace("== listen\n", "== listen\ntcp 0 0 127.0.0.1:5572 0.0.0.0:* LISTEN\n")
    )
    readonly = spike.evaluate_readonly(broken)
    assert any("create" in p for p in readonly)
    assert any("remount" in p for p in readonly)
    isolation = spike.evaluate_isolation(broken, "root")
    assert len(isolation) == 3


def test_rclone_log_evaluator_flags_a_write_that_reached_rclone():
    reads = "DEBUG : docs/readme.txt: Open: flags=O_RDONLY|0x8000\n"
    assert spike.evaluate_rclone_log(reads) == []
    write = 'DEBUG : : Create: name="b.txt", flags=0100102, mode=0100644\n'
    assert spike.evaluate_rclone_log(reads + write)


def test_watch_evaluator_needs_enotconn_then_a_new_readable_mount():
    recovered = """[109842.50] mounts=[4489 ] read=hello from the fixture
[109842.77] mounts=[4489 ] read=cat: can't open '/cloud/root/docs/readme.txt': Transport endpoint is not connected
[109843.57] mounts=[3216 ] read=hello from the fixture
"""
    problems, seconds = spike.evaluate_watch(recovered)
    assert problems == [] and seconds == pytest.approx(0.8)
    never_broke = "[1.0] mounts=[1 ] read=hello from the fixture\n"
    assert spike.evaluate_watch(never_broke)[0] == ["the workspace never saw ENOTCONN"]
    stale = recovered.splitlines()[0] + "\n" + recovered.splitlines()[1] + "\n"
    assert spike.evaluate_watch(stale)[0] == ["the workspace never read a new mount"]
