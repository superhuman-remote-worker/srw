"""Render assertions: Neo4j is optional, off by default, and when it is on the
orchestrator gets the same connection the agents do.

The orchestrator container lists its env by hand (no ``envFrom``), so the
ConfigMap's ``NEO4J_URL`` used to reach only the agents; the orchestrator's
knowledge-graph leg (note relationships, update and delete projection, export)
never ran on a chart install. These pin the chart default, the bundled and the
external mode, and that the rendered names are the ones the code reads.
knowledge-base/knowledge/issues/orchestrator_never_receives_neo4j_url.md
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import shutil
import subprocess

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "neo4j-test"
FULLNAME = f"{RELEASE}-superhuman-remote-worker"
EXTERNAL_URL = "bolt://graph.example.com:7687"

ON = ("databases.neo4j.enabled=true",)
EXTERNAL = (
    "databases.neo4j.enabled=true",
    "databases.neo4j.internal=false",
    f"databases.neo4j.externalUrl={EXTERNAL_URL}",
)
# Both egress policies are opt-in; render them so their Neo4j rule is visible.
POLICIES = ("agent.networkPolicy.enabled=true", "workspace.networkPolicy.enabled=true")

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _command(*settings: str) -> list[str]:
    # No ci/*-values.yaml: these assert the chart's own defaults.
    command = [
        "helm",
        "template",
        RELEASE,
        str(CHART),
        "--set",
        "license.acceptTerms=true",
        "--set",
        "global.domain=srw.example.com",
        "--set",
        "secrets.create=true",
    ]
    for setting in settings:
        command.extend(["--set", setting])
    return command


@lru_cache(maxsize=None)
def _render(*settings: str) -> tuple[dict, ...]:
    rendered = subprocess.run(
        _command(*settings), check=True, capture_output=True, text=True
    ).stdout
    return tuple(document for document in yaml.safe_load_all(rendered) if document)


def _named(documents, kind: str, name: str) -> dict | None:
    for document in documents:
        if document["kind"] == kind and document["metadata"]["name"] == name:
            return document
    return None


def _config_data(documents) -> dict:
    return _named(documents, "ConfigMap", f"{FULLNAME}-config")["data"]


def _orchestrator_env(documents) -> dict[str, dict]:
    deployment = _named(documents, "Deployment", f"{FULLNAME}-orchestrator")
    (container,) = [
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    ]
    assert "envFrom" not in container, "the orchestrator lists its env by hand"
    names = [entry["name"] for entry in container["env"]]
    assert len(names) == len(set(names)), "duplicate env names"
    return {entry["name"]: entry for entry in container["env"]}


def _neo4j_env(documents) -> dict[str, dict]:
    return {
        name: entry
        for name, entry in _orchestrator_env(documents).items()
        if "NEO4J" in name
    }


def _bundled_objects(documents) -> set[tuple[str, str]]:
    return {
        (document["kind"], document["metadata"]["name"])
        for document in documents
        if document["metadata"]["name"].startswith(f"{FULLNAME}-neo4j")
    }


def _policies_with_neo4j_egress(documents) -> set[str]:
    names = set()
    for document in documents:
        if document["kind"] != "NetworkPolicy":
            continue
        for rule in document["spec"].get("egress") or []:
            for peer in rule.get("to") or []:
                labels = (peer.get("podSelector") or {}).get("matchLabels") or {}
                if labels.get("app.kubernetes.io/component") == "neo4j":
                    names.add(document["metadata"]["name"])
    return names


# --- off by default ---------------------------------------------------------


def test_neo4j_is_off_by_default():
    documents = _render()
    assert _bundled_objects(documents) == set()
    assert not {key for key in _config_data(documents) if "NEO4J" in key}


def test_the_orchestrator_gets_no_neo4j_env_when_neo4j_is_off():
    """No graph connection, and no seed for a "Default Neo4j" datasource."""
    assert _neo4j_env(_render()) == {}


def test_no_egress_rule_names_neo4j_when_it_is_off():
    assert _policies_with_neo4j_egress(_render(*POLICIES)) == set()


# --- bundled ----------------------------------------------------------------


def test_bundled_neo4j_renders_when_enabled():
    assert _bundled_objects(_render(*ON)) == {
        ("StatefulSet", f"{FULLNAME}-neo4j"),
        ("Service", f"{FULLNAME}-neo4j"),
        ("PersistentVolumeClaim", f"{FULLNAME}-neo4j-data"),
        ("NetworkPolicy", f"{FULLNAME}-neo4j"),
    }


@pytest.mark.parametrize(
    "settings, url",
    [(ON, f"bolt://{FULLNAME}-neo4j:7688"), (EXTERNAL, EXTERNAL_URL)],
    ids=["bundled", "external"],
)
def test_the_orchestrator_reads_the_agents_neo4j_url_from_the_config_map(settings, url):
    documents = _render(*settings)
    ref = _neo4j_env(documents)["NEO4J_URL"]["valueFrom"]["configMapKeyRef"]
    assert ref["name"] == f"{FULLNAME}-config"
    # The very key the agents' envFrom delivers, so both sides dial one URL.
    assert ref["key"] == "NEO4J_URL"
    # Rendered under the same gate, so the ref is not optional: it resolves.
    assert "optional" not in ref
    assert _config_data(documents)["NEO4J_URL"] == url


@pytest.mark.parametrize("settings", [ON, EXTERNAL], ids=["bundled", "external"])
def test_the_orchestrator_reads_neo4j_credentials_from_the_secret(settings):
    env = _neo4j_env(_render(*settings))
    for name in ("NEO4J_USERNAME", "NEO4J_PASSWORD"):
        ref = env[name]["valueFrom"]["secretKeyRef"]
        assert ref["name"] == FULLNAME
        # The keys the bundled server and the agents' envFrom use.
        assert ref["key"] == name
        # Optional like the agents' envFrom: a Secret without them still
        # starts the orchestrator, which then reports the graph unreachable.
        assert ref["optional"] is True


def test_the_default_neo4j_datasource_seed_comes_with_the_graph_env():
    env = _neo4j_env(_render(*ON))
    assert set(env) == {
        "NEO4J_URL",
        "NEO4J_USERNAME",
        "NEO4J_PASSWORD",
        "DEFAULT_DS_NEO4J_URL",
        "DEFAULT_DS_NEO4J_USERNAME",
        "DEFAULT_DS_NEO4J_PASSWORD",
        "DEFAULT_DS_NEO4J_NAME",
        "DEFAULT_DS_NEO4J_READ_ONLY",
    }


def test_agent_and_workspace_egress_reach_the_bundled_neo4j():
    documents = _render(*ON, *POLICIES)
    assert _policies_with_neo4j_egress(documents) == {
        f"{FULLNAME}-agent-egress",
        f"{FULLNAME}-workspace-policy-internet-only",
        f"{FULLNAME}-workspace-policy-home-allowed",
    }


def test_the_browser_ingress_fronts_the_bundled_service():
    documents = _render(*ON, "databases.neo4j.browser.enabled=true")
    ingress = _named(documents, "Ingress", f"{FULLNAME}-neo4j-ingress")
    (path,) = ingress["spec"]["rules"][0]["http"]["paths"]
    assert path["backend"]["service"]["name"] == f"{FULLNAME}-neo4j"


# --- external ---------------------------------------------------------------


def test_external_neo4j_renders_no_bundled_objects_and_no_pod_egress_rule():
    """External Neo4j is no pod here: agents reach it through extraEgress."""
    documents = _render(*EXTERNAL, *POLICIES, "databases.neo4j.browser.enabled=true")
    assert _bundled_objects(documents) == set()
    assert _policies_with_neo4j_egress(documents) == set()


def test_external_neo4j_requires_its_url():
    result = subprocess.run(
        _command("databases.neo4j.enabled=true", "databases.neo4j.internal=false"),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "databases.neo4j.externalUrl is required" in result.stderr


# --- the names the code reads -----------------------------------------------


def test_the_rendered_names_are_the_ones_the_orchestrator_reads(monkeypatch):
    """KnowledgeGraphHandle builds KnowledgeGraphDB() from the environment;
    init.get_neo4j_config reads the same three names."""
    from shared.runtime.services.knowledge_graph import KnowledgeGraphDB

    from orchestrator.init import get_neo4j_config

    names = {
        name for name in _neo4j_env(_render(*ON)) if not name.startswith("DEFAULT_DS_")
    }
    assert names == {"NEO4J_URL", "NEO4J_USERNAME", "NEO4J_PASSWORD"}

    monkeypatch.setenv("NEO4J_URL", "bolt://rendered:7688")
    monkeypatch.setenv("NEO4J_USERNAME", "rendered-user")
    monkeypatch.setenv("NEO4J_PASSWORD", "rendered-password")
    graph = KnowledgeGraphDB()
    assert (graph._uri, graph._username, graph._password) == (
        "bolt://rendered:7688",
        "rendered-user",
        "rendered-password",
    )
    assert get_neo4j_config() == {
        "uri": "bolt://rendered:7688",
        "username": "rendered-user",
        "password": "rendered-password",
    }
