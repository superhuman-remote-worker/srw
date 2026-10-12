"""Policy snapshot for callers that consume runtime network coordinates."""

from __future__ import annotations

import ast
import difflib
import importlib.util
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPT = REPO_ROOT / "scripts" / "check_runtime_coordinate_callers.py"
MANIFEST = REPO_ROOT / "policy" / "runtime_coordinate_callers.txt"


def _load_script():
    spec = importlib.util.spec_from_file_location(
        "check_runtime_coordinate_callers", SCRIPT
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["check_runtime_coordinate_callers"] = module
    spec.loader.exec_module(module)
    return module


def test_runtime_coordinate_inventory_matches_manifest():
    script = _load_script()
    sites = script.collect_sites()
    classifications = script.read_classifications()
    rendered = script.render_manifest(sites, classifications)
    on_disk = MANIFEST.read_text()
    if rendered != on_disk:
        diff = "\n".join(
            difflib.unified_diff(
                on_disk.splitlines(),
                rendered.splitlines(),
                fromfile=str(MANIFEST.relative_to(REPO_ROOT)),
                tofile="<regenerated>",
                lineterm="",
            )
        )
        pytest.fail(
            "Runtime-coordinate inventory is stale. Review new sites and run "
            "the scanner with --write. Diff:\n" + diff
        )


def test_empty_source_discovery_is_an_error(monkeypatch):
    script = _load_script()
    monkeypatch.setattr(script, "_roots", lambda: [])
    with pytest.raises(RuntimeError, match="No Python sources"):
        script.collect_sites()


def test_application_composition_package_is_scanned(tmp_path, monkeypatch):
    """main.py shrinks to an entrypoint; its code moves into application/."""
    script = _load_script()
    (tmp_path / "application" / "nested").mkdir(parents=True)
    for relative in (
        "main.py",
        "application/__init__.py",
        "application/routes.py",
        "application/nested/lifespan.py",
    ):
        (tmp_path / relative).write_text("")
    monkeypatch.setattr(script, "ORCHESTRATOR", tmp_path)
    assert script._roots() == [
        tmp_path / "main.py",
        tmp_path / "application" / "__init__.py",
        tmp_path / "application" / "nested" / "lifespan.py",
        tmp_path / "application" / "routes.py",
    ]


def test_every_runtime_coordinate_site_has_a_reviewed_classification():
    script = _load_script()
    sites = script.collect_sites()
    classifications = script.read_classifications()
    assert set(classifications) == {site.key for site in sites}
    assert all(
        classification in script.ALLOWED_CLASSIFICATIONS
        for classification in classifications.values()
    )


def test_scanner_detects_a_new_coordinate_derived_http_caller():
    script = _load_script()
    source = """
async def mutate_reused_endpoint(client, row):
    pod_ip = row.get("pod_ip")
    endpoint_url = f"http://{pod_ip}:8001/mutate"
    await client.post(endpoint_url, json={"effect": True})
"""
    visitor = script._Visitor("synthetic.py")
    visitor.visit(ast.parse(source))
    assert [(qualname, kind) for qualname, kind, _, _ in visitor.raw_sites] == [
        ("mutate_reused_endpoint", "http")
    ]


def _classifications_of(rendered: str) -> list[str]:
    return [
        line.split()[-1]
        for line in rendered.splitlines()
        if line.strip() and not line.startswith("#")
    ]


_TWO_EFFECTS = """
async def drive_runtime(client, ssh, row):
    pod_ip = row["pod_ip"]
    await client.post(f"http://{pod_ip}:8001/attach", json={"attach": True})
    await client.post(f"http://{pod_ip}:8001/control", json={"control": True})
"""

_TWO_EFFECTS_REORDERED = """
async def drive_runtime(client, ssh, row):
    pod_ip = row["pod_ip"]
    await client.post(f"http://{pod_ip}:8001/control", json={"control": True})
    await client.post(f"http://{pod_ip}:8001/attach", json={"attach": True})
"""


def test_reordering_calls_cannot_transfer_a_classification():
    script = _load_script()
    original = script.sites_for_source("synthetic.py", _TWO_EFFECTS)
    reordered = script.sites_for_source("synthetic.py", _TWO_EFFECTS_REORDERED)
    assert len(original) == 2
    # Distinct calls must not collide on a positional ordinal...
    assert original[0].key != original[1].key
    assert {site.ordinal for site in original} == {1}
    # ...and the identity set must survive a source-order swap unchanged, so a
    # reviewed classification stays attached to the call it was reviewed for.
    assert {site.key for site in original} == {site.key for site in reordered}
    classifications = {
        original[0].key: "exact-runtime-recipient",
        original[1].key: "read-only-probe",
    }
    rendered = script.render_manifest(
        sorted(reordered, key=lambda site: site.key), classifications
    )
    assert set(_classifications_of(rendered)) == {
        "exact-runtime-recipient",
        "read-only-probe",
    }


@pytest.mark.parametrize(
    ("before", "after"),
    (
        ("client.post(url, json=payload)", "client.put(url, json=payload)"),
        ("client.post(url, json=payload)", "client.post(url, json=other)"),
        (
            "await ssh_exec(target_host, 'tar -x')",
            "await ssh_exec(target_host, 'rm -rf /')",
        ),
        (
            "await stream_extract_snapshot(remote_host, token)",
            "await stream_upload_snapshot(remote_host, token)",
        ),
    ),
)
def test_replacing_a_call_target_or_shape_mints_an_unclassified_site(before, after):
    script = _load_script()
    template = (
        "async def drive(client, row, payload, other, token):\n"
        "    target_host = row['ssh_host']\n"
        "    remote_host = row['ssh_host']\n"
        "    url = row['endpoint_url']\n"
        "    {statement}\n"
    )
    original = script.sites_for_source(
        "synthetic.py", template.format(statement=before)
    )
    replaced = script.sites_for_source("synthetic.py", template.format(statement=after))
    assert original and replaced
    reviewed = {site.key: "exact-runtime-recipient" for site in original}
    assert {site.key for site in replaced}.isdisjoint(reviewed)
    rendered = script.render_manifest(replaced, reviewed)
    assert set(_classifications_of(rendered)) == {"unclassified"}


def test_collect_sites_is_deterministic():
    script = _load_script()
    first = script.collect_sites()
    second = script.collect_sites()
    assert [site.key for site in first] == [site.key for site in second]
    assert len({site.key for site in first}) == len(first)
