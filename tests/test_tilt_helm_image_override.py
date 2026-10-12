"""A saved digest must not override the image Tilt just built."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.parametrize("image_key", ["image.mcp", "vmController.preparation.image"])
@pytest.mark.parametrize("receipt", ["current", "missing", "other-map", "ambiguous"])
def test_tilt_image_replaces_a_saved_digest(tmp_path, image_key, receipt):
    binaries = tmp_path / "bin"
    binaries.mkdir()
    observation = tmp_path / "helm.json"
    fake = (
        f"#!{sys.executable}\n"
        + """
import json, os, sys
from pathlib import Path
CTX = os.environ.get('SRW_HELM_EXPECT_CONTEXT', 'k3d-srw')
args = sys.argv[1:]
name = Path(sys.argv[0]).name
receipt = os.environ['RECEIPT']
if name == 'tilt':
    assert args == ['get', 'imagemap', 'candidate-map', '-o', 'json']
    print(json.dumps({'status': {
        'imageFromLocal': 'localhost:5005/candidate:tilt-fresh',
        'imageFromCluster': 'srw-registry:5000/candidate:' + ('old' if receipt == 'other-map' else 'tilt-fresh')
    }}))
elif name == 'docker':
    assert args == ['image', 'inspect', 'localhost:5005/candidate:tilt-fresh']
    digests = ['localhost:5005/candidate@sha256:' + 'b' * 64]
    if receipt == 'missing': digests = []
    if receipt == 'ambiguous': digests += ['localhost:5005/candidate@sha256:' + 'c' * 64]
    print(json.dumps([{'RepoDigests': digests}]))
elif name == 'kubectl':
    # Read-back runs with an explicit context even when NAMESPACE is empty:
    # assert the targeting instead of merely discarding the flags.
    assert args[0] == '--context' and args[1] == CTX, args
    rest = args[2:]
    assert '--kube-context' not in rest and '--context' not in rest, rest
    print(sys.stdin.read())
elif args[0] == '--kube-context' and args[1] == CTX:
    rest = args[2:]
    assert '--kube-context' not in rest and '--context' not in rest, rest
    if rest[0] == 'status':
        print(json.dumps({'info': {'status': 'deployed'}}))
    elif rest[:2] == ['get', 'manifest']:
        print('{}')
    elif rest[:2] == ['upgrade', '--install']:
        Path(os.environ['OBSERVATION']).write_text(json.dumps(rest))
    else:
        raise AssertionError(rest)
else:
    raise AssertionError(args)
"""
    )
    for name in ("helm", "kubectl", "tilt", "docker"):
        executable = binaries / name
        executable.write_text(fake)
        executable.chmod(0o700)
    result = subprocess.run(
        [
            "bash",
            str(ROOT / "scripts/tilt-helm-apply.sh"),
            "--set-string",
            image_key + ".digest=sha256:" + "a" * 64,
        ],
        env={
            **os.environ,
            "PATH": str(binaries) + os.pathsep + os.environ["PATH"],
            "RELEASE_NAME": "srw",
            "CHART": "./helm",
            "NAMESPACE": "",
            "TILT_IMAGE_COUNT": "1",
            "TILT_IMAGE_0": "srw-registry:5000/candidate:tilt-fresh",
            "TILT_IMAGE_KEY_REPO_0": image_key + ".repository",
            "TILT_IMAGE_KEY_TAG_0": image_key + ".tag",
            "TILT_IMAGE_KEY_DIGEST_0": image_key + ".digest",
            "TILT_IMAGE_MAP_0": "candidate-map",
            "OBSERVATION": str(observation),
            "RECEIPT": receipt,
        },
        capture_output=True,
        text=True,
        timeout=10,
    )
    if receipt != "current":
        assert result.returncode != 0
        assert "Could not verify the pushed Tilt image digest." in result.stderr
        assert not observation.exists(), "Unverified image must not reach Helm apply."
        return
    assert result.returncode == 0, result.stderr
    args = json.loads(observation.read_text())
    settings = {}
    for i, argument in enumerate(args[:-1]):
        if argument in ("--set", "--set-string"):
            key, value = args[i + 1].split("=", 1)
            settings[key] = value
    assert settings == {
        image_key + ".digest": "sha256:" + "b" * 64,
        image_key + ".repository": "srw-registry:5000/candidate",
        image_key + ".tag": "tilt-fresh",
    }
