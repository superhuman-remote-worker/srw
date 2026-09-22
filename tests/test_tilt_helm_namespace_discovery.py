"""Tilt discovers one Helm release spanning control and native namespaces."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("get_fails", [False, True])
def test_discovery_preserves_explicit_namespaces_and_original_config(
    tmp_path, get_fails
):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    original = tmp_path / "original.json"
    # Ambient current-context is deliberately NOT k3d-srw: the script must
    # operate on the explicitly selected entry regardless of ambient state.
    original.write_text('{"current-context":"ambient-other","private":"unchanged"}')
    before = original.read_bytes()
    observation = tmp_path / "observation.json"
    fake = (
        f"#!{sys.executable}\n"
        + """
import json, os, sys
from pathlib import Path
CTX = os.environ.get('SRW_HELM_EXPECT_CONTEXT', 'k3d-srw')
name = Path(sys.argv[0]).name
args = sys.argv[1:]
def strip_target(args, flag):
    # The apply script must target every cluster operation explicitly: assert
    # the expected flag/value instead of merely discarding unknown prefixes.
    assert args[0] == flag and args[1] == CTX, args
    rest = args[2:]
    assert '--kube-context' not in rest and '--context' not in rest, rest
    return rest
if name == 'helm':
    args = strip_target(args, '--kube-context')
    if args[0] == 'status':
        print(json.dumps({'info': {'status': 'deployed'}}))
    elif args[:2] == ['get', 'manifest']:
        print(json.dumps({'kind': 'List', 'items': [
            {'kind': 'ConfigMap', 'metadata': {'name': 'control'}},
            {'kind': 'Role', 'metadata': {'name': 'native', 'namespace': 'srw-native'}}
        ]}))
    elif args[:2] != ['upgrade', '--install']:
        raise AssertionError(args)
elif name == 'kubectl':
    args = strip_target(args, '--context')
    if args[:2] == ['config', 'view']:
        # Emulate real kubectl: `config view --minify` honors --context, so
        # the round-tripped name proves the explicitly selected entry
        # resolves. Ambient current-context is never consulted here.
        selected = CTX
        print(json.dumps({'current-context': selected, 'contexts': [
            {'name': selected, 'context': {'cluster': 'cluster', 'user': 'user', 'namespace': 'old'}}
        ], 'users': [{'name': 'user', 'user': {'token': 'test-only-no-copy'}}]}))
    else:
        assert args == ['get', '-oyaml', '-f', '-'], args
        paths = os.environ['KUBECONFIG'].split(os.pathsep)
        overlay = json.loads(Path(paths[0]).read_text())
        docs = json.load(sys.stdin)
        json.dump({'overlay': overlay, 'paths': paths, 'docs': docs},
                  open(os.environ['OBSERVATION'], 'w'))
        if os.environ['GET_FAILS'] == 'true':
            sys.exit(9)
        namespace = overlay['contexts'][0]['context']['namespace']
        for item in docs['items']:
            item['metadata'].setdefault('namespace', namespace)
        print(json.dumps(docs))
"""
    )
    for name in ("helm", "kubectl"):
        executable = binaries / name
        executable.write_text(fake)
        executable.chmod(0o700)
    result = subprocess.run(
        ["bash", str(ROOT / "scripts/tilt-helm-apply.sh")],
        env={
            **os.environ,
            "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
            "RELEASE_NAME": "srw",
            "CHART": "./helm",
            "NAMESPACE": "srw",
            "TILT_IMAGE_COUNT": "0",
            "KUBECONFIG": str(original),
            "TMPDIR": str(tmp_path),
            "OBSERVATION": str(observation),
            "GET_FAILS": str(get_fails).lower(),
        },
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == (9 if get_fails else 0), result.stderr
    observed = json.loads(observation.read_text())
    assert observed["paths"][1:] == [str(original)]
    assert observed["overlay"] == {
        "apiVersion": "v1",
        "kind": "Config",
        "current-context": "k3d-srw",
        "contexts": [
            {
                "name": "k3d-srw",
                "context": {"cluster": "cluster", "user": "user", "namespace": "srw"},
            }
        ],
    }
    assert original.read_bytes() == before
    assert not Path(observed["paths"][0]).exists()
    if not get_fails:
        items = json.loads(result.stdout)["items"]
        assert [item["metadata"]["namespace"] for item in items] == [
            "srw",
            "srw-native",
        ]
