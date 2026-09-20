"""Deployment contracts for the opt-in Collabora Office Canvas surface."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def test_collabora_official_subchart_is_default_off_and_single_replica() -> None:
    chart = yaml.safe_load((ROOT / "helm/Chart.yaml").read_text())
    dependency = next(
        item for item in chart["dependencies"] if item.get("alias") == "collabora"
    )
    assert dependency == {
        "name": "collabora-online",
        "alias": "collabora",
        "version": "1.3.0",
        "repository": "https://collaboraonline.github.io/online",
        "condition": "collabora.enabled",
    }

    values = yaml.safe_load((ROOT / "helm/values.yaml").read_text())
    collabora = values["collabora"]
    assert collabora["enabled"] is False
    assert collabora["replicaCount"] == 1
    assert collabora["autoscaling"]["enabled"] is False
    assert collabora["autoscaling"].get("minReplicas", 2) == 2

    extra_params = collabora["collabora"]["extra_params"]
    for required in (
        "--o:ssl.enable=false",
        "--o:ssl.termination=true",
        "--o:per_document.always_save_on_exit=true",
        "--o:admin_console.enable=false",
        "--o:net.content_security_policy=frame-ancestors",
    ):
        assert required in extra_params
    assert "net.frame_ancestors" not in extra_params


def test_collabora_runtime_config_and_network_policy_fail_closed() -> None:
    configmap = (ROOT / "helm/templates/configmap.yaml").read_text()
    deployment = (ROOT / "helm/templates/orchestrator/deployment.yaml").read_text()
    cockpit = (ROOT / "helm/templates/cockpit/deployment.yaml").read_text()
    cockpit_environment = (ROOT / "helm/templates/_cockpit.tpl").read_text()
    ingress = (ROOT / "helm/templates/ingress.yaml").read_text()
    network = (ROOT / "helm/templates/collabora/network-policy.yaml").read_text()
    environment = (ROOT / "cockpit/src/assets/env.js").read_text()
    docker_env = (ROOT / "docker/cockpit-canvas-env.sh").read_text()

    for name in (
        "COLLABORA_ENABLED",
        "COLLABORA_INTERNAL_URL",
        "COLLABORA_PUBLIC_URL",
        "COLLABORA_WOPI_BASE_URL",
        "COLLABORA_COCKPIT_ORIGIN",
        "COLLABORA_TOKEN_TTL_SECONDS",
        "COLLABORA_DISCOVERY_CACHE_TTL_SECONDS",
    ):
        assert name in configmap
        assert name in deployment

    assert "srw.cockpitEnvironment" in cockpit
    assert "canvasOfficeOrigin" in cockpit_environment
    assert "canvasOfficeOrigin" in environment
    assert "canvasOfficeOrigin" in docker_env
    assert "COLLABORA_PUBLIC_URL" in docker_env

    assert "{{- if .Values.collabora.enabled }}" in network
    assert "kind: NetworkPolicy" in network
    assert "port: 9980" in network
    assert "port: 8085" in network
    assert "port: 53" in network
    assert "non-empty networkPolicy.edgeNamespaceSelector" in network
    assert ".Values.sessionRouter.jwtSecretName" in network
    assert "requires a configured sessionRouter JWT secret" in network
    assert "block-public-wopi" in ingress
    assert "- path: /wopi" in ingress
    assert "{{- if .Values.collabora.enabled }}" in ingress


def test_collabora_public_example_and_ci_wiring_is_complete() -> None:
    public_example = (ROOT / ".env.example").read_text()

    for name in (
        "COLLABORA_ENABLED",
        "COLLABORA_INTERNAL_URL",
        "COLLABORA_PUBLIC_URL",
        "COLLABORA_WOPI_BASE_URL",
        "COLLABORA_COCKPIT_ORIGIN",
        "COLLABORA_TOKEN_TTL_SECONDS",
        "COLLABORA_DISCOVERY_CACHE_TTL_SECONDS",
        "COLLABORA_DISCOVERY_TIMEOUT_SECONDS",
        "CANVAS_MAX_OFFICE_BYTES",
        "SESSION_JWT_SECRET",
    ):
        assert name in public_example

    for workflow in (
        ROOT / ".github/workflows/main.yml",
        ROOT / ".github/workflows/develop.yml",
    ):
        assert "helm dependency build helm/" in workflow.read_text()


def test_collabora_values_schema_pins_boolean_gate() -> None:
    schema = json.loads((ROOT / "helm/values.schema.json").read_text())
    collabora = schema["properties"]["collabora"]
    assert collabora["type"] == "object"
    assert collabora["properties"]["enabled"] == {"type": "boolean"}
