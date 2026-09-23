"""A1 host factory refuses a shared source before any kubectl invocation."""

from __future__ import annotations

from argparse import Namespace
import importlib.util
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/vm-retained-resume-fixture.py"
SPEC = importlib.util.spec_from_file_location("a1_fixture_host", SCRIPT)
assert SPEC and SPEC.loader
HOST = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HOST)


def _args(tmp_path):
    output_dir = tmp_path / "private"
    output_dir.mkdir(mode=0o700)
    run = "srw-a1-owned-20260923a"
    return Namespace(
        run_id=run, context=run, cluster_uid="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        namespace=run, deployment=f"{run}-orchestrator",
        pod=f"{run}-orchestrator-abc", pod_uid="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        image_id="containerd://sha256:" + "c" * 64,
        vm_image="registry.example/a1@sha256:" + "d" * 64,
        model_id="e2e-vm-a1-20260923a",
        confirm="disposable-vm-retained-resume-fixture-v1",
        output=output_dir / "fixture.json",
        inference_key_file=output_dir / "inference.key",
    )


def test_host_factory_requires_exact_owned_source_and_private_fresh_output(tmp_path):
    args = _args(tmp_path)
    HOST.guard(args)
    args.namespace = "srw"
    with pytest.raises(HOST.FixtureHostRefusal):
        HOST.guard(args)
    args.namespace = args.run_id
    args.output.write_text("old", encoding="utf-8")
    with pytest.raises(HOST.FixtureHostRefusal):
        HOST.guard(args)


def test_host_factory_does_not_call_kubectl_for_shared_namespace(tmp_path, monkeypatch):
    args = _args(tmp_path)
    args.context = "shared"
    monkeypatch.setattr(HOST, "_kubectl", lambda *_a, **_kw: pytest.fail("kubectl called"))
    with pytest.raises(HOST.FixtureHostRefusal):
        HOST.run(args)


def test_host_factory_requires_private_inference_key_before_exec(tmp_path, monkeypatch):
    args = _args(tmp_path)
    monkeypatch.setattr(HOST, "verify_host_source", lambda _args: None)
    monkeypatch.setattr(HOST, "_kubectl", lambda *_a, **_kw: pytest.fail("kubectl called"))
    with pytest.raises(HOST.FixtureHostRefusal, match="key file"):
        HOST.run(args)
