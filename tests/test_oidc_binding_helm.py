"""The Bearer token binding's chart surface (``auth.oidc``).

The ConfigMap names the clients (``azp``) and audiences (``aud``) whose
access tokens the orchestrator accepts, derived from the clients the chart
itself configures; the orchestrator container reads both keys from it.
"""

from __future__ import annotations

import shutil

import pytest

from orchestrator.security.oidc import (
    OIDC_ALLOWED_AUDIENCES_ENV,
    OIDC_ALLOWED_CLIENTS_ENV,
    parse_name_list,
)
from tests.test_connector_lease_helm import _orchestrator, render

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _config(docs: list[dict]) -> dict[str, str]:
    for doc in docs:
        data = doc.get("data") or {}
        if doc["kind"] == "ConfigMap" and "KEYCLOAK_CLIENT_ID" in data:
            return data
    raise AssertionError("no ConfigMap carries KEYCLOAK_CLIENT_ID")


def _binding(docs: list[dict]) -> tuple[frozenset[str], frozenset[str]]:
    data = _config(docs)
    return (
        parse_name_list(data[OIDC_ALLOWED_CLIENTS_ENV], ()),
        parse_name_list(data[OIDC_ALLOWED_AUDIENCES_ENV], ()),
    )


def test_defaults_are_the_bff_and_spa_clients_and_the_api_audience():
    docs = render()
    clients, audiences = _binding(docs)
    assert clients == {"cockpit-bff", "cockpit"}
    assert audiences == {"orchestrator-api"}
    assert _config(docs)["KEYCLOAK_CLIENT_ID"] == "cockpit-bff"


def test_the_orchestrator_reads_both_keys_from_the_configmap():
    docs = render()
    _, container, _ = _orchestrator(docs)
    refs = {
        item["name"]: item["valueFrom"]["configMapKeyRef"]
        for item in container.get("env", [])
        if "configMapKeyRef" in (item.get("valueFrom") or {})
    }
    for key in (OIDC_ALLOWED_CLIENTS_ENV, OIDC_ALLOWED_AUDIENCES_ENV):
        assert refs[key]["key"] == key
        assert not refs[key].get("optional")


def test_renamed_clients_follow_the_values():
    clients, _ = _binding(
        render("auth.bff.clientId=srw-bff", "keycloak.cockpitClientId=srw-spa")
    )
    assert clients == {"srw-bff", "srw-spa"}


def test_extra_clients_and_audiences_are_added():
    clients, audiences = _binding(
        render(
            "auth.oidc.extraAllowedClients={srw-cli,cockpit}",
            "auth.oidc.allowedAudiences={orchestrator-api,api://srw}",
        )
    )
    assert clients == {"cockpit-bff", "cockpit", "srw-cli"}
    assert audiences == {"orchestrator-api", "api://srw"}


def test_cleared_audiences_render_empty_not_the_default():
    _, audiences = _binding(render("auth.oidc.allowedAudiences=null"))
    assert audiences == frozenset()


def test_external_keycloak_uses_the_bootstrapped_spa_client():
    docs = render(
        "keycloak.internal=false",
        "keycloak.externalIssuerUrl=https://kc.example.test",
        "keycloak.bootstrap.cockpitClientId=srw-cockpit",
    )
    clients, _ = _binding(docs)
    assert clients == {"cockpit-bff", "srw-cockpit"}


def test_the_bootstrap_override_does_not_apply_to_the_bundled_realm():
    clients, _ = _binding(render("keycloak.bootstrap.cockpitClientId=srw-cockpit"))
    assert clients == {"cockpit-bff", "cockpit"}
