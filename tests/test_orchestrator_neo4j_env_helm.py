"""Render assertions: the orchestrator gets the Neo4j settings its graph client reads.

``KnowledgeGraphHandle`` turns the orchestrator's graph client on only when
``NEO4J_URL`` is set, and ``KnowledgeGraphDB`` reads ``NEO4J_USERNAME`` and
``NEO4J_PASSWORD``. The chart gave the orchestrator only the ``DEFAULT_DS_*``
copies, which seed the default connector, so on every chart install the client
was off: a note's REST ``relationships`` were always ``[]``, REST deletes and
tag edits never reached the graph, and the KB export answered 503 (dev, 11-06).
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "neo4j-env-test"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(*settings: str) -> list[dict]:
    command = [
        "helm",
        "template",
        RELEASE,
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--show-only",
        "templates/orchestrator/deployment.yaml",
        "--show-only",
        "templates/configmap.yaml",
    ]
    for setting in settings:
        command.extend(["--set", setting])
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout
    return [document for document in yaml.safe_load_all(rendered) if document]


def _orchestrator_env(documents: list[dict]) -> dict[str, dict]:
    (deployment,) = [
        d
        for d in documents
        if d["kind"] == "Deployment" and d["metadata"]["name"].endswith("-orchestrator")
    ]
    (container,) = [
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    ]
    return {entry["name"]: entry for entry in container["env"]}


def _config_maps(documents: list[dict]) -> dict[str, dict]:
    return {
        d["metadata"]["name"]: d.get("data") or {}
        for d in documents
        if d["kind"] == "ConfigMap"
    }


def test_the_orchestrator_reads_the_graph_url_its_agents_use():
    documents = _render()
    env = _orchestrator_env(documents)
    ref = env["NEO4J_URL"]["valueFrom"]["configMapKeyRef"]
    data = _config_maps(documents)[ref["name"]]
    assert ref["key"] == "NEO4J_URL"
    # Agents load this whole ConfigMap; the orchestrator now reads its key.
    assert data["NEO4J_URL"].startswith("bolt://")
    assert data["NEO4J_URL"] == data["DEFAULT_DS_NEO4J_URL"]


def test_its_credentials_come_from_the_secret_the_default_connector_uses():
    env = _orchestrator_env(_render())
    for name in ("NEO4J_USERNAME", "NEO4J_PASSWORD"):
        ref = env[name]["valueFrom"]["secretKeyRef"]
        default_ds = env[f"DEFAULT_DS_{name}"]["valueFrom"]["secretKeyRef"]
        assert (ref["name"], ref["key"]) == (default_ds["name"], name)
        assert ref["optional"] is True


def test_without_neo4j_the_url_is_unset_and_the_client_stays_off():
    documents = _render("databases.neo4j.enabled=false")
    ref = _orchestrator_env(documents)["NEO4J_URL"]["valueFrom"]["configMapKeyRef"]
    assert ref["optional"] is True
    assert "NEO4J_URL" not in _config_maps(documents)[ref["name"]]
