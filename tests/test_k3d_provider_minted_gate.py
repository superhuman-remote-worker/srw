"""Safety contract for the local C5 provider-minted gate (never run here)."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
_SCRIPT = _ROOT / "scripts" / "k3d-provider-minted-gate.py"
_SPEC = importlib.util.spec_from_file_location("k3d_provider_minted_gate", _SCRIPT)
assert _SPEC is not None and _SPEC.loader is not None
gate = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = gate
_SPEC.loader.exec_module(gate)

GATE_ID = "c5-0123456789"
IDS = f"srw-gate-{GATE_ID}-ids"
WORK = f"srw-gate-{GATE_ID}-work"


@pytest.fixture
def no_cluster(monkeypatch):
    monkeypatch.setattr(
        subprocess, "run", lambda *a, **k: pytest.fail("must not touch the cluster")
    )


@pytest.fixture
def key_file(tmp_path):
    path = tmp_path / "app.pem"
    path.write_text(
        "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----\n"
    )
    path.chmod(0o600)
    return str(path)


GITHUB = [
    "--github-app-id",
    "12",
    "--github-installation-id",
    "34",
    "--github-repo",
    "https://github.com/acme/scratch.git",
]


@pytest.mark.parametrize(
    "argv",
    [
        ["--context", "k3d-other"],
        ["--namespace", "default"],
        ["--run"],
        ["--run", "--confirm", "yes"],
        ["--confirm", gate.LOCAL_CONFIRMATION],
        ["--gate-id", "c3-0123456789"],
        ["--gate-id", "c5-xyz"],
        ["--model", "bad model"],
        ["--user", "Robert'); DROP"],
        ["--turn-timeout", "5"],
        ["--github-app-id", "12"],
        [*GITHUB[:4], "--github-key-file", "k"],
        [
            *GITHUB[:4],
            "--github-key-file",
            "k",
            "--github-repo",
            "https://gitlab.com/a/b",
        ],
        ["--github-app-id", "x", *GITHUB[2:], "--github-key-file", "k"],
    ],
)
def test_refuses_anything_outside_the_local_disposable_boundary(argv, no_cluster):
    assert gate.main(argv) == 2


def test_a_key_file_others_may_read_is_refused(key_file, no_cluster):
    os.chmod(key_file, 0o644)
    argv = ["--run", "--confirm", gate.LOCAL_CONFIRMATION, *GITHUB]
    assert gate.main([*argv, "--github-key-file", key_file]) == 2


def test_dry_run_prints_the_plan_and_touches_nothing(no_cluster, capsys):
    assert gate.main([]) == 0
    out = capsys.readouterr().out
    for phase in (
        "preflight",
        "serviceaccounts/token",
        "ValidatingAdmissionPolicy",
        "NetworkPolicy",
        "renewal",
        "detach",
        "cancel",
        "pinned",
        "connector edit",
        "connector delete",
        "logs",
        "records",
        "401",
        "github: skipped",
        "cleanup",
        "sweepIntervalSeconds",
        "connectors.providerMinting.privateHosts",
        '["kubernetes.default.svc"]',
    ):
        assert phase in out
    assert gate.main([*GITHUB, "--github-key-file", "k"]) == 0
    assert "github: skipped" not in capsys.readouterr().out


def test_every_kubectl_targets_the_local_context():
    assert gate.KUBE[:2] == ["kubectl", "--context=k3d-srw"]
    assert gate.K[:4] == ["kubectl", "--context=k3d-srw", "-n", "srw"]


@pytest.mark.parametrize(
    "name",
    ["_MINTED_PROGRAM", "_BEARER_PROGRAM", "_WS_PROGRAM", "_DONE_ROWS_PROGRAM"],
)
def test_embedded_programs_compile_and_cap_their_memory(name):
    program = getattr(gate, name).replace("REQUEST", "'{}'")
    compile(program, name, "exec")
    assert "cap_memory()" in program


def test_the_served_sets_name_the_c5_modules_and_exist():
    files = {path for served in gate.SERVED_SETS for path in served.files}
    assert "src/orchestrator/services/connector_minted_credentials.py" in files
    assert (
        "src/orchestrator/database/migrations/app/0430_connector_minted_credentials.sql"
        in files
    )
    for served in gate.SERVED_SETS:
        for path in served.files:
            assert (_ROOT / path).is_file(), path
        for directory in served.dirs:
            assert (_ROOT / directory).is_dir(), directory


def test_the_minting_role_is_the_documented_minimum():
    manifests = gate.identity_manifests(IDS, WORK, GATE_ID, "m")
    [role] = [
        m
        for m in manifests
        if m["kind"] == "Role" and m["metadata"]["name"] == gate.MINTER_SA
    ]
    assert role["metadata"]["namespace"] == IDS
    assert role["rules"] == [
        {
            "apiGroups": [""],
            "resources": ["serviceaccounts/token"],
            "resourceNames": ["agent"],
            "verbs": ["create"],
        },
        {"apiGroups": [""], "resources": ["secrets"], "verbs": ["create", "delete"]},
    ]
    # The module that documents the minimum says the same.
    from shared.connectors import token_request

    for line in ('resources: ["serviceaccounts/token"]', 'verbs: ["create", "delete"]'):
        assert line in token_request.__doc__
    # The target account's permissions live in the work namespace only.
    [binding] = [
        m
        for m in manifests
        if m["kind"] == "RoleBinding" and m["metadata"]["namespace"] == WORK
    ]
    assert binding["subjects"] == [
        {"kind": "ServiceAccount", "name": "agent", "namespace": IDS}
    ]
    assert all(m["metadata"]["labels"] == {gate.GATE_LABEL: GATE_ID} for m in manifests)


def _found(**over):
    found = {
        "kubeconfig_var": True,
        "users": 1,
        "user_keys": ["token"],
        "tokens": 1,
        "digest": "d" * 64,
        "claims": {
            "sub": f"system:serviceaccount:{IDS}:agent",
            "iat": 1000,
            "exp": 1600,
            "secret": "srw-mint-0123",
            "namespace": IDS,
        },
        "marker": "m",
        "can": {
            f"{verb} {resource} -n {namespace}": (
                "yes" if f"{verb} {resource}" in gate.ALLOWED else "no"
            )
            for verb, resource, namespace in gate.can_i_requests(IDS, WORK)
        },
        "secrets_listed": False,
    }
    found.update(over)
    return found


def test_the_workspace_evaluator_accepts_the_minted_kubeconfig():
    assert gate.workspace_problems(_found(), digest="d" * 64, ids=IDS, marker="m") == []


@pytest.mark.parametrize(
    "over",
    [
        {"user_keys": ["exec"]},
        {"user_keys": ["client-certificate-data", "token"]},
        {"digest": "e" * 64},
        {"tokens": 2, "users": 2},
        {"kubeconfig_var": False},
        {"marker": ""},
        {"secrets_listed": True},
        {"claims": {"sub": "system:serviceaccount:x:minter", "iat": 1, "exp": 601}},
        {
            "claims": {
                "sub": f"system:serviceaccount:{IDS}:agent",
                "iat": 0,
                "exp": 3600,
                "secret": "srw-mint-1",
            }
        },
        {
            "claims": {
                "sub": f"system:serviceaccount:{IDS}:agent",
                "iat": 0,
                "exp": 600,
                "secret": "other",
            }
        },
    ],
)
def test_the_workspace_evaluator_refuses_what_must_not_be(over):
    assert gate.workspace_problems(_found(**over), digest="d" * 64, ids=IDS, marker="m")


def test_more_than_the_role_allows_fails():
    found = _found()
    found["can"][f"create configmaps -n {WORK}"] = "yes"
    assert gate.workspace_problems(found, digest="d" * 64, ids=IDS, marker="m")


def _jwt(claims: dict) -> str:
    def part(value) -> str:
        return base64.urlsafe_b64encode(json.dumps(value).encode()).decode().rstrip("=")

    return f"{part({'alg': 'RS256'})}.{part(claims)}.sig"


def test_the_workspace_program_reads_the_kubeconfig_it_was_given(tmp_path):
    """The workspace program against a fake kubectl: it reports the token's
    digest and claims, never the token."""
    token = _jwt(
        {
            "sub": f"system:serviceaccount:{IDS}:agent",
            "iat": 1000,
            "exp": 1600,
            "kubernetes.io": {"namespace": IDS, "secret": {"name": "srw-mint-ab"}},
        }
    )
    config = {
        "users": [{"name": "u", "user": {"token": token}}],
        "contexts": [],
        "clusters": [],
    }
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    kubectl = bin_dir / "kubectl"
    kubectl.write_text(
        "#!/bin/sh\n"
        'case "$1 $2" in\n'
        f"  'config view') cat {tmp_path / 'config.json'} ;;\n"
        "  'auth can-i') case \"$3 $4\" in 'get configmaps'|'list pods') echo yes ;; "
        "*) echo no; exit 1 ;; esac ;;\n"
        "  *) case \"$*\" in *'get configmap'*) printf marker ;; "
        "*'get secrets'*) exit 1 ;; esac ;;\n"
        "esac\n"
    )
    kubectl.chmod(kubectl.stat().st_mode | stat.S_IEXEC)
    (tmp_path / "config.json").write_text(json.dumps(config))
    request = json.dumps(
        {
            "work": WORK,
            "ids": IDS,
            "marker": "srw-gate-marker",
            "can_i": gate.can_i_requests(IDS, WORK),
        }
    )
    program = gate._WS_PROGRAM.replace("REQUEST", repr(request))
    done = subprocess.run(
        [sys.executable, "-I", "-c", program],
        capture_output=True,
        text=True,
        env={"PATH": f"{bin_dir}:/usr/bin:/bin", "KUBECONFIG": "/x"},
        timeout=60,
    )
    assert done.returncode == 0, done.stderr
    found = json.loads(done.stdout.splitlines()[-1])
    assert token not in done.stdout
    assert found["digest"] == hashlib.sha256(token.encode()).hexdigest()
    assert found["claims"]["secret"] == "srw-mint-ab"
    assert found["marker"] == "marker"
    assert not found["secrets_listed"]
    assert (
        gate.workspace_problems(
            found,
            digest=hashlib.sha256(token.encode()).hexdigest(),
            ids=IDS,
            marker="marker",
        )
        == []
    )


def test_the_jwt_claims_helper():
    assert gate.jwt_claims(_jwt({"exp": 5}))["exp"] == 5


def test_without_github_arguments_the_phase_is_skipped_with_a_note(capsys):
    args = gate.build_parser().parse_args(["--gate-id", GATE_ID])
    runner = gate.ProviderMintedGate(args)
    runner.github_checks()
    out = capsys.readouterr().out
    assert "NOTE github: SKIPPED" in out and runner.report.results == []


def test_the_admission_policy_is_the_recommended_one_named_for_the_run():
    from shared.connectors.token_request import admission_policy

    policy, binding = gate.gate_admission_policy(IDS, GATE_ID)
    name = f"srw-gate-{GATE_ID}-minted-secrets"
    assert policy["metadata"] == {"name": name, "labels": {gate.GATE_LABEL: GATE_ID}}
    assert binding["metadata"]["name"] == name
    assert binding["spec"]["policyName"] == name
    recommended, _ = admission_policy(IDS, gate.MINTER_SA)
    assert policy["spec"] == recommended["spec"]
    assert f"system:serviceaccount:{IDS}:minter" in json.dumps(policy["spec"])
    assert binding["spec"]["matchResources"]["namespaceSelector"] == {
        "matchLabels": {"kubernetes.io/metadata.name": IDS}
    }


def test_the_log_scan_counts_without_naming():
    assert gate.log_hits("a token-1 b", ["token-1", "token-2", ""]) == 1
    assert gate.log_hits("", ["token-1"]) == 0


@pytest.mark.parametrize(
    ("enabled", "hosts", "ok"),
    [
        ("true", "kubernetes.default.svc", True),
        ("", "Kubernetes.Default.Svc, ghe.corp", True),
        ("true", "kubernetes.default.svc:443", True),
        ("false", "kubernetes.default.svc", False),
        ("true", "", False),
        ("true", "kubernetes.default", False),
    ],
)
def test_the_preflight_needs_minting_on_and_the_api_server_listed(enabled, hosts, ok):
    assert (gate.private_hosts_problem(enabled, hosts) == "") is ok


def test_the_pinned_served_set_exists():
    for directory in gate.PINNED_SERVED.dirs:
        assert (_ROOT / directory).is_dir(), directory
    for path in gate.PINNED_SERVED.files:
        assert (_ROOT / path).is_file(), path
