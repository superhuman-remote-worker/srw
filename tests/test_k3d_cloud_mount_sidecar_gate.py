"""Safety contract and evaluators of the local D7 cloud mount sidecar gate
(never run here)."""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

from orchestrator.services import cloud_mount_plan
from tests.test_in_pod_mount import PLAN, build

ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = ROOT / "scripts" / "k3d-cloud-mount-sidecar-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_cloud_mount_sidecar_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

PROGRAMS = {"api": gate._API_PROGRAM, "hash": gate._HASH_PROGRAM}
SCRIPTS = {
    "inspect": gate._INSPECT_SCRIPT,
    "as-agent": gate._AS_AGENT,
    "nextcloud-find": gate._NEXTCLOUD_FIND,
}


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


def _args(*extra: str):
    return gate.build_parser().parse_args(
        ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *extra]
    )


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "c2-0123456789"],
        ["--gate-id", "d7-xyz"],
        ["--model", "bad model; rm -rf /"],
        ["--user", "Robert'); DROP"],
        ["--pod-timeout", "5"],
        ["--turn-timeout", "99999"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "plane",
        "killed",
        "missing",
        "protected",
        "without",
        "teardown",
        "regression",
        "cleanup",
    ):
        assert f"- {phase}:" in out


@pytest.mark.parametrize("name", sorted(PROGRAMS))
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = PROGRAMS[name]
    compile(program, name, "exec")
    assert "\ncap_memory()\n" in program


@pytest.mark.parametrize("name", sorted(SCRIPTS))
def test_embedded_shell_scripts_parse(name):
    subprocess.run(["sh", "-n", "-c", SCRIPTS[name]], check=True)


def test_the_served_files_exist_and_name_the_slice():
    for path in (*gate.SERVED, *gate.AGENT_SERVED):
        assert (ROOT / path).is_file(), path
    assert "src/orchestrator/services/cloud_mount_plan.py" in gate.SERVED
    assert "src/shared/runtime/services/cloud_mount/sidecar.py" in gate.AGENT_SERVED


def test_the_gate_reveals_what_the_planner_obscures():
    assert gate._OBSCURE_KEY == cloud_mount_plan._OBSCURE_KEY
    password = "p@ss w0rd/ä"
    assert gate.reveal(cloud_mount_plan.rclone_obscure(password)) == password


def test_the_password_is_read_back_from_the_credential_file():
    conf = PLAN.rclone_config()
    sections = gate.parse_rclone_conf(conf)
    assert set(sections) == {"m0", "m1"}
    assert {gate.reveal(s["pass"]) for s in sections.values()} == set(
        PLAN.passwords.values()
    )


def test_the_workspace_inspection_is_parsed():
    status = json.dumps({"state": "unavailable", "reason": "not_found"})
    out = "\n".join(
        [
            "==fuse",
            "absent",
            "==capeff",
            "CapEff:\t00000000a80425fb",
            "==mountinfo",
            "30 25 0:40 / /cloud ro,relatime master:9 - tmpfs tmpfs ro",
            "41 30 0:51 / /cloud/project rw,nosuid,nodev master:11 - fuse.rclone "
            "srw-cloud rw,user_id=65534",
            "42 30 0:52 / /cloud/lower ro,nosuid,nodev master:12 - fuse.rclone "
            "srw-cloud ro",
            "43 30 0:53 / /cloud/my\\040folder rw - fuse.rclone srw-cloud rw",
            "==status",
            "--0",
            json.dumps({"state": "mounted"}),
            "",
            "--1",
            status,
            "",
            "==tcp",
            "  sl  local_address rem_address   st tx_queue",
            "   0: 00000000:0016 00000000:0000 0A 00000000:00000000",
            "   1: 0100007F:15C4 0100007F:9C40 01 00000000:00000000",
            "==rclone-conf",
            "==end",
        ]
    )
    found = gate.sections(out)
    assert found["fuse"] == "absent"
    assert gate.has_cap(found["capeff"], gate.CAP_SYS_ADMIN) is False
    assert gate.has_cap("CapEff:\t000001ffffffffff", gate.CAP_SYS_ADMIN) is True
    tops = gate.top_mounts(found["mountinfo"])
    assert tops["/cloud/project"][0] == "fuse.rclone"
    assert tops["/cloud/lower"] == ("fuse.rclone", "ro,nosuid,nodev")
    assert "/cloud/my folder" in tops
    assert gate.status_files(found["status"]) == {
        0: {"state": "mounted"},
        1: {"state": "unavailable", "reason": "not_found"},
    }
    # 22 listens; 5572 is only an established connection here.
    assert gate.listening_ports(found["tcp"]) == {22}
    assert found["rclone-conf"] == ""


def test_a_sidecar_pod_from_the_real_builder_passes_and_its_faults_do_not(
    monkeypatch,
):
    from tests.test_in_pod_mount import OPENER, RCLONE

    monkeypatch.setenv("CONNECTOR_IN_POD_OPENER_IMAGE", OPENER)
    monkeypatch.setenv("CONNECTOR_IN_POD_RCLONE_IMAGE", RCLONE)
    from orchestrator.services.container_provisioner import ContainerProvisioner

    pod = build(ContainerProvisioner(), PLAN)
    assert gate.sidecar_pod_problems(pod, protected=False) == []
    assert gate.sidecar_pod_problems(pod, protected=True) != []
    assert gate.credential_secret_name(pod)
    assert gate.recorded_plan(pod)["mounts"][0]["name"] == "project"
    faulty = json.loads(json.dumps(pod))
    gate.container(faulty, gate.SUPERVISOR_CONTAINER)["securityContext"][
        "runAsUser"
    ] = 0
    workspace = gate.container(faulty, gate.WORKSPACE_CONTAINER)
    workspace.setdefault("securityContext", {})["privileged"] = True
    problems = gate.sidecar_pod_problems(faulty, protected=False)
    assert "the supervisor is not unprivileged" in problems
    assert "the workspace kept its FUSE profile" in problems


def test_the_thread_state_must_come_from_the_agent():
    status = {
        "mounts": {
            "project": {"state": "mounted", "reason": None, "reported_by": "agent"},
            "other": {
                "state": "unavailable",
                "reason": "not_found",
                "reported_by": "orchestrator",
            },
        }
    }
    assert gate.evaluate_status(status, "project", state="mounted")[0] is True
    assert (
        gate.evaluate_status(status, "other", state="unavailable", reason="not_found")[
            0
        ]
        is False
    )
    assert gate.evaluate_status(None, "project", state="mounted")[0] is False


def test_restarts_read_native_sidecars_and_containers():
    pod = {
        "status": {
            "initContainerStatuses": [{"name": "srw-cloud-mount", "restartCount": 2}],
            "containerStatuses": [{"name": "workspace", "restartCount": 0}],
        }
    }
    assert gate.restarts(pod, "srw-cloud-mount") == 2
    assert gate.restarts(pod, "workspace") == 0
    with pytest.raises(gate.GateError):
        gate.restarts(pod, "missing")


def test_secrets_reach_the_cluster_only_on_stdin(monkeypatch):
    seen: list[tuple[list[str], str | None]] = []

    def fake_run(argv, **kwargs):
        seen.append((argv, kwargs.get("input")))
        body = json.dumps({"status": 200, "body": "{}"})
        return subprocess.CompletedProcess(argv, 0, stdout=body + "\n", stderr="")

    monkeypatch.setattr(subprocess, "run", fake_run)
    run = gate.CloudMountSidecarGate(_args("--password", "s3cret-pw"))
    run.api.call("GET", "/api/health")
    assert seen
    for argv, _stdin in seen:
        assert "s3cret-pw" not in " ".join(argv)
    assert any("s3cret-pw" in (stdin or "") for _argv, stdin in seen)
    assert gate._scrub("x s3cret-pw y") == "x <redacted> y"


def test_the_node_is_reached_only_on_a_k3d_srw_node(monkeypatch, no_cluster):
    run = gate.CloudMountSidecarGate(_args())
    monkeypatch.setattr(gate.shutil, "which", lambda name: "/usr/bin/" + name)
    with pytest.raises(gate.GateError, match="not a k3d-srw node"):
        run.node_of({"spec": {"nodeName": "prod-node-1"}})
    assert run.node_of({"spec": {"nodeName": "k3d-srw-server-0"}}) == (
        "k3d-srw-server-0"
    )
    monkeypatch.setattr(gate.shutil, "which", lambda name: None)
    with pytest.raises(gate.GateError, match="docker"):
        run.node_of({"spec": {"nodeName": "k3d-srw-agent-1"}})


def test_cleanup_reaches_everything_the_run_named(monkeypatch):
    run = gate.CloudMountSidecarGate(_args())
    run.sessions = {
        "rw": gate.Session("rw", "p-rw", "11", thread="t-rw"),
        "missing": gate.Session("missing", "p-missing", "12", thread="t-missing"),
    }
    run.projects = {"rw": "p-rw", "missing": "p-missing"}
    run.jobs = {"job": "j-1"}
    run.moved_folders = {"12": "Gate Project"}
    run.disabled_reader = "srw-reader-u"
    monkeypatch.setattr(run, "titled_threads", lambda: ["t-rw", "t-leftover"])
    monkeypatch.setattr(run, "described_jobs", lambda: [])
    events: list[tuple] = []
    monkeypatch.setattr(
        run.api,
        "call",
        lambda method, path, body=None, **k: events.append(("api", method, path))
        or (404, {}),
    )
    monkeypatch.setattr(
        run, "occ", lambda arguments: events.append(("occ", *arguments)) or 0
    )
    monkeypatch.setattr(gate, "sql", lambda query: "0")

    assert run.cleanup() == []

    assert ("occ", "user:enable", "srw-reader-u") in events
    paths = [event[2] for event in events if event[0] == "api"]
    for thread in ("t-rw", "t-missing", "t-leftover"):
        assert any(thread in path and "permanent=true" in path for path in paths)
    assert ("api", "DELETE", "/api/jobs/j-1") in events
    for project in ("p-rw", "p-missing"):
        assert ("api", "DELETE", f"/api/projects/{project}") in events
    # The folder moves back before its project is deleted.
    assert events.index(("occ", "groupfolders:rename", "12", "Gate Project")) < (
        events.index(("api", "DELETE", "/api/projects/p-missing"))
    )
    assert run.moved_folders == {}


def test_residue_names_objects_folders_and_a_disabled_reader(monkeypatch):
    run = gate.CloudMountSidecarGate(_args())
    run.objects = {"ws-thread-x-cloud-abc"}
    run.moved_folders = {"12": "x"}
    run.disabled_reader = "srw-reader-u"
    monkeypatch.setattr(run, "titled_threads", lambda: [])
    monkeypatch.setattr(run, "described_jobs", lambda: [])
    monkeypatch.setattr(gate, "sql", lambda query: "0")
    monkeypatch.setattr(
        gate,
        "run",
        lambda argv, **k: (0 if argv[-1] == "ws-thread-x-cloud-abc" else 1, "", ""),
    )
    left = run.residue()
    assert "configmap ws-thread-x-cloud-abc" in left
    assert "secret ws-thread-x-cloud-abc" in left
    assert any("not moved back" in item for item in left)
    assert any("srw-reader-u" in item for item in left)


def test_a_registered_password_is_never_printed(capsys):
    """The plane phase reveals the Secret's password to look for copies of
    it; every line the gate prints is scrubbed of it."""
    password = gate.secret("pw-" + "z" * 20)
    gate.Report("d7-0000000000").check("x", True, f"detail {password}")
    gate.Report("d7-0000000000").note(f"note {password}")
    assert password not in capsys.readouterr().out


def test_the_gate_follows_the_lane_thread_admission_picks():
    """Protected sessions always run pinned (thread admission keeps their
    overlay staging off the stateless lane); the gate's sandbox sessions run
    stateless, whose End drains the folders."""
    assert gate.expected_lane(protected=True) == "pinned"
    assert gate.expected_lane(protected=False) == "stateless"
    source = (ROOT / "src/orchestrator/services/thread_admission.py").read_text()
    assert "if request_body.protected_cloud:\n" in source
    assert 'execution_lane = "pinned"' in source


def test_a_pinned_session_gets_the_reader_disabled_before_its_attach(monkeypatch):
    """prot42 disables its reader as soon as the grant is active, and a
    reader that never becomes active is infrastructure trouble."""
    g = gate.CloudMountSidecarGate(_args("--gate-id", "d7-0123456789"))
    session = gate.Session("prot42", "p", "1", thread="t")
    answers = iter(["", "", "srw-reader-a-17aefa77"])
    monkeypatch.setattr(g, "active_reader", lambda _s: next(answers) or None)
    monkeypatch.setattr(gate.time, "sleep", lambda _s: None)
    calls: list[list[str]] = []
    monkeypatch.setattr(g, "occ", lambda argv: calls.append(argv) or 0)
    g.disable_reader_once_granted(session)
    assert calls == [["user:disable", "srw-reader-a-17aefa77"]]
    assert g.disabled_reader == "srw-reader-a-17aefa77"
    g.enable_reader()
    assert calls[-1] == ["user:enable", "srw-reader-a-17aefa77"]
    assert g.disabled_reader is None
