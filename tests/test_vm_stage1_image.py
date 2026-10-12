"""Reuse is an exact-input, bounded-age decision over one immutable digest."""

from datetime import datetime, timedelta, timezone
from copy import deepcopy
import subprocess

import pytest

from scripts.vm_stage1_image import INPUTS, LABEL, input_digest, reusable_image

NOW = datetime(2026, 9, 30, tzinfo=timezone.utc)
DIGEST = "sha256:" + "a" * 64
IMAGE = "ghcr.io/example/base:latest"
PINNED = "ghcr.io/example/base@" + DIGEST


def config():
    return {
        "created": NOW.isoformat(),
        "architecture": "amd64",
        "os": "linux",
        "config": {"Labels": {LABEL: "inputs", "io.srw.component": "workspace-stage1"}},
    }


def test_matching_base_is_inspected_by_digest_after_resolving_channel():
    calls = []

    def inspect(image, field):
        calls.append((image, field))
        return {"digest": DIGEST} if field == "Manifest" else config()

    assert reusable_image(IMAGE, "inputs", now=NOW, inspect=inspect) == PINNED
    assert calls == [(IMAGE, "Manifest"), (PINNED, "Image")]


@pytest.mark.parametrize(
    "kind", ["changed", "legacy", "old", "future", "architecture", "component"]
)
def test_unqualified_bases_are_rebuilt(kind):
    candidate = deepcopy(config())
    if kind == "changed":
        candidate["config"]["Labels"][LABEL] = "other-inputs"
    if kind == "legacy":
        candidate["config"]["Labels"].pop(LABEL)
    if kind == "old":
        candidate["created"] = (NOW - timedelta(days=9)).isoformat()
    if kind == "future":
        candidate["created"] = (NOW + timedelta(days=1)).isoformat()
    if kind == "architecture":
        candidate["architecture"] = "arm64"
    if kind == "component":
        candidate["config"]["Labels"]["io.srw.component"] = "workspace"

    def inspect(image, field):
        return {"digest": DIGEST} if field == "Manifest" else candidate

    assert reusable_image(IMAGE, "inputs", now=NOW, inspect=inspect) is None


def test_registry_failure_triggers_build():
    def unavailable(*args):
        raise subprocess.CalledProcessError(1, "docker")

    assert reusable_image(IMAGE, "inputs", now=NOW, inspect=unavailable) is None


def test_input_identity_changes_only_with_build_inputs(tmp_path):
    for name in INPUTS:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        if name.endswith("cloud-init"):
            path.mkdir()
            (path / "user-data").write_text("base")
        else:
            path.write_text("base")
    original = input_digest(tmp_path)
    (tmp_path / "README.md").write_text("documentation")
    assert input_digest(tmp_path) == original
    (tmp_path / ".playwright-version").write_text("different")
    assert input_digest(tmp_path) != original
