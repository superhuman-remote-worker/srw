"""Offline k3d-only boundary for scripts/tilt-helm-apply.sh.

Every cluster operation must explicitly select k3d-srw; disallowed
environment/argument targets are rejected before any Helm/kubectl call.
All binaries are fakes speaking to dummy files — no real cluster exists
here, and rejection is never tested against the real `main` cluster.
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]

FAKE = (
    f"#!{sys.executable}\n"
    + """
import json, os, sys
from pathlib import Path
CALLS = Path(os.environ['CALLS'])
CTX = 'k3d-srw'
AMBIENT = 'ambient-other'
name = Path(sys.argv[0]).name
args = sys.argv[1:]
CALLS.parent.mkdir(parents=True, exist_ok=True)
with open(CALLS, 'a') as fh:
    fh.write(name + '\\t' + json.dumps(args) + '\\n')
def need_target(args, flag):
    # Explicit targeting is mandatory: assert the expected flag/value and
    # confirm no second selector lurks later in argv.
    assert args[0] == flag and args[1] == CTX, args
    rest = args[2:]
    assert '--kube-context' not in rest and '--context' not in rest, rest
    return rest
if name == 'helm':
    rest = need_target(args, '--kube-context')
    mode = os.environ.get('HELM_MODE', 'deployed')
    if rest[0] == 'status':
        if mode == 'pending':
            print(json.dumps({'info': {
                'status': 'pending-upgrade',
                'last_deployed': os.environ['PENDING_SINCE'],
            }}))
        else:
            print(json.dumps({'info': {'status': 'deployed'}}))
    elif rest[0] == 'history':
        if mode == 'pending':
            print(json.dumps([
                {'revision': 99, 'status': 'pending-upgrade'},
                {'revision': 98, 'status': 'deployed'},
            ]))
        else:
            print(json.dumps([{'revision': 98, 'status': 'deployed'}]))
    elif rest[0] == 'list':
        print(json.dumps([{'revision': 98, 'status': 'deployed'}]))
    elif rest[:2] == ['get', 'manifest']:
        print('{"kind": "List", "items": []}')
    elif rest[:2] == ['upgrade', '--install']:
        Path(os.environ['UPGRADED']).write_text(json.dumps(rest))
    else:
        raise AssertionError(rest)
elif name == 'kubectl':
    rest = need_target(args, '--context')
    if rest[:2] == ['config', 'view']:
        # Emulate real kubectl: `config view --minify` honors --context, so
        # the round-tripped name proves the selected entry resolves. Unless
        # VIEW_NAME overrides (simulating unresolvable selection), in which
        # case ambient leaks through exactly like the old buggy check.
        selected = os.environ.get('VIEW_NAME', CTX)
        print(json.dumps({'current-context': selected, 'contexts': [
            {'name': selected,
             'context': {'cluster': 'c', 'user': 'u', 'namespace': 'old'}}
        ]}))
    elif rest[:2] == ['delete', 'secret']:
        print('secret deleted')
    elif rest == ['get', '-oyaml', '-f', '-']:
        print(sys.stdin.read())
    else:
        raise AssertionError(rest)
else:
    raise AssertionError(name)
"""
)


def run_apply(tmp_path, argv=(), env_extra=None):
    binaries = tmp_path / "bin"
    binaries.mkdir(exist_ok=True)
    for tool in ("helm", "kubectl"):
        executable = binaries / tool
        if not executable.exists():
            executable.write_text(FAKE)
            executable.chmod(0o700)
    kubeconfig = tmp_path / "kubeconfig.json"
    kubeconfig.write_text('{"current-context":"ambient-other"}')
    env = {
        **os.environ,
        "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
        "RELEASE_NAME": "srw",
        "CHART": "./helm",
        "NAMESPACE": "srw",
        "TILT_IMAGE_COUNT": "0",
        "KUBECONFIG": str(kubeconfig),
        "TMPDIR": str(tmp_path),
        "CALLS": str(tmp_path / "calls.log"),
        "UPGRADED": str(tmp_path / "upgraded.json"),
    }
    env.update(env_extra or {})
    return subprocess.run(
        ["bash", str(ROOT / "scripts/tilt-helm-apply.sh"), *argv],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )


def calls(tmp_path):
    path = tmp_path / "calls.log"
    if not path.exists():
        return []
    return [line.split("\t", 1) for line in path.read_text().splitlines()]


def assert_all_calls_targeted(entries):
    assert entries, "expected at least one cluster call"
    for tool, raw in entries:
        args = json.loads(raw)
        if tool == "helm":
            assert args[0] == "--kube-context" and args[1] == "k3d-srw", args
        else:
            assert tool == "kubectl", (tool, args)
            assert args[0] == "--context" and args[1] == "k3d-srw", args


@pytest.mark.parametrize(
    "bad_context", ["main", "other-cluster", "k3d-srw-evil", "K3D-SRW"]
)
def test_disallowed_expect_context_rejected_before_cluster_calls(tmp_path, bad_context):
    result = run_apply(tmp_path, env_extra={"SRW_HELM_EXPECT_CONTEXT": bad_context})
    assert result.returncode != 0
    assert "disallowed" in result.stderr
    assert calls(tmp_path) == [], "no Helm/kubectl call may precede rejection"


@pytest.mark.parametrize(
    "selector",
    [
        ["--kube-context", "main"],
        ["--kube-context=main"],
        ["--context", "main"],
        ["--context=main"],
        ["--cluster", "other"],
        ["--server", "https://other:6443"],
        ["--kubeconfig", "/tmp/evil-config"],
        ["--namespace", "other"],
        ["-n", "other"],
        ["--as", "admin"],
    ],
)
def test_forwarded_cluster_selector_rejected_before_cluster_calls(tmp_path, selector):
    result = run_apply(tmp_path, argv=selector)
    assert result.returncode != 0
    assert "cluster-selecting argument" in result.stderr
    assert calls(tmp_path) == [], "no Helm/kubectl call may precede rejection"


def test_forwarded_set_args_still_reach_apply(tmp_path):
    result = run_apply(tmp_path, argv=["--set", "a=b"])
    assert result.returncode == 0, result.stderr
    upgraded = json.loads((tmp_path / "upgraded.json").read_text())
    assert "--set" in upgraded and "a=b" in upgraded
    assert_all_calls_targeted(calls(tmp_path))


def test_ambient_context_differs_operations_stay_targeted(tmp_path):
    result = run_apply(tmp_path, env_extra={"SRW_HELM_EXPECT_CONTEXT": "k3d-srw"})
    assert result.returncode == 0, result.stderr
    assert_all_calls_targeted(calls(tmp_path))


def test_stale_pending_recovery_uses_intended_target(tmp_path):
    result = run_apply(
        tmp_path,
        argv=["--take-ownership"],
        env_extra={
            "SRW_HELM_EXPECT_CONTEXT": "k3d-srw",
            "HELM_MODE": "pending",
            "PENDING_SINCE": "2020-01-01T00:00:00+00:00",
            "SRW_HELM_STALE_AFTER": "60",
        },
    )
    assert result.returncode == 0, result.stderr
    entries = calls(tmp_path)
    assert_all_calls_targeted(entries)
    deletes = [
        json.loads(raw) for tool, raw in entries if json.loads(raw)[2:3] == ["delete"]
    ]
    assert len(deletes) == 1
    delete = deletes[0]
    assert delete[3:5] == ["secret", "sh.helm.release.v1.srw.v99"], delete
    assert "--namespace" in delete and "srw" in delete
    upgraded = json.loads((tmp_path / "upgraded.json").read_text())
    assert upgraded[:2] == ["upgrade", "--install"]
    assert "--take-ownership" in upgraded


def test_fresh_pending_release_left_alone(tmp_path):
    result = run_apply(
        tmp_path,
        env_extra={
            "SRW_HELM_EXPECT_CONTEXT": "k3d-srw",
            "HELM_MODE": "pending",
            "PENDING_SINCE": datetime.now(timezone.utc).isoformat(),
            "SRW_HELM_STALE_AFTER": "3600",
        },
    )
    assert result.returncode == 0, result.stderr
    entries = calls(tmp_path)
    assert_all_calls_targeted(entries)
    assert [raw for _, raw in entries if '"delete"' in raw] == []
    assert (tmp_path / "upgraded.json").exists()


def test_unresolvable_selection_blocks_destructive_recovery(tmp_path):
    result = run_apply(
        tmp_path,
        env_extra={
            "SRW_HELM_EXPECT_CONTEXT": "k3d-srw",
            "HELM_MODE": "pending",
            "PENDING_SINCE": "2020-01-01T00:00:00+00:00",
            "SRW_HELM_STALE_AFTER": "60",
            "VIEW_NAME": "ambient-other",
        },
    )
    assert result.returncode == 0, result.stderr
    assert "refusing to clear the lock" in result.stderr
    assert [raw for _, raw in calls(tmp_path) if '"delete"' in raw] == []
