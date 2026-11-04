"""Render assertions: the bundled Postgres StatefulSets apply ``maxConnections``.

``databases.<key>.maxConnections`` reads as a setting for every engine, but only
the CloudNativePG engine applied it (``srw.dbParameters``). The default
``statefulset`` engine ran at the image default of 100 whatever the values said,
so an install sized for ~30 agents ran out of connections with no warning.

The StatefulSets now pass it as ``-c max_connections=<value>``. The images'
``docker-entrypoint.sh`` prepends ``postgres`` when the first argument is a
flag, so ``args`` alone (no ``command``) still starts the server.

Issue: knowledge-base/knowledge/issues/bundled_postgres_statefulset_ignores_max_connections.md
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

RELEASE = "maxconn-test"
CHART_NAME = yaml.safe_load((CHART / "Chart.yaml").read_text())["name"]
# `srw.fullname` is "<release>-<chart name>" absent an override.
FULLNAME = f"{RELEASE}-{CHART_NAME}"

# values key -> (template, StatefulSet component name)
DATABASES = {
    "postgres": ("postgres.yaml", "postgres"),
    "vector": ("postgres-vector.yaml", "pgvector"),
    "audit": ("postgres-audit.yaml", "auditdb"),
    "gitea": ("postgres-gitea.yaml", "giteadb"),
    "keycloak": ("postgres-keycloak.yaml", "keycloakdb"),
    "nextcloud": ("postgres-nextcloud.yaml", "nextclouddb"),
}

# A distinct value per database, so a template that reads its neighbour's key
# fails instead of passing on a shared default.
CUSTOM = {key: 201 + index for index, key in enumerate(sorted(DATABASES))}

KEEPALIVES = {
    "tcp_keepalives_idle": "10",
    "tcp_keepalives_interval": "10",
    "tcp_keepalives_count": "3",
}

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(*settings: str, show_only: list[str] | None = None) -> list[dict]:
    command = [
        "helm",
        "template",
        RELEASE,
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        # The Nextcloud database renders only beside a bundled Nextcloud.
        "--set",
        "nextcloud.enabled=true",
        "--set",
        "opencloud.enabled=false",
    ]
    # `--show-only` fails on a template that renders nothing, so a render that
    # proves something is ABSENT passes None and reads the whole chart.
    for template in show_only or []:
        command.extend(["--show-only", template])
    for setting in settings:
        command.extend(["--set", setting])
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout
    return [document for document in yaml.safe_load_all(rendered) if document]


def _statefulset_templates() -> list[str]:
    return [f"templates/databases/{template}" for template, _ in DATABASES.values()]


def _containers(*settings: str) -> dict[str, dict]:
    """values key -> the single database container of its StatefulSet."""
    by_name = {
        document["metadata"]["name"]: document
        for document in _render(*settings, show_only=_statefulset_templates())
        if document.get("kind") == "StatefulSet"
    }
    containers = {}
    for key, (_, component) in DATABASES.items():
        pod = by_name[f"{FULLNAME}-{component}"]["spec"]["template"]["spec"]
        assert len(pod["containers"]) == 1, key
        containers[key] = pod["containers"][0]
    return containers


def _server_settings(container: dict) -> dict[str, str]:
    """Parse ``["-c", "name=value", ...]`` and insist it is well formed."""
    args = container.get("args", [])
    assert len(args) % 2 == 0, args
    settings = {}
    for flag, assignment in zip(args[::2], args[1::2]):
        assert flag == "-c", args
        name, separator, value = assignment.partition("=")
        assert separator, args
        settings[name] = value
    return settings


@pytest.fixture(scope="module")
def default_containers() -> dict[str, dict]:
    return _containers()


@pytest.fixture(scope="module")
def custom_containers() -> dict[str, dict]:
    return _containers(
        *(f"databases.{key}.maxConnections={value}" for key, value in CUSTOM.items())
    )


@pytest.fixture(scope="module")
def unset_containers() -> dict[str, dict]:
    return _containers(*(f"databases.{key}.maxConnections=null" for key in DATABASES))


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_default_render_passes_the_values_default(key, default_containers):
    assert _server_settings(default_containers[key])["max_connections"] == "100"


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_custom_value_reaches_its_own_server(key, custom_containers):
    assert _server_settings(custom_containers[key])["max_connections"] == str(
        CUSTOM[key]
    )


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_args_rely_on_the_image_entrypoint(key, default_containers):
    """No ``command``, and a flag first: docker-entrypoint.sh prepends postgres."""
    container = default_containers[key]
    assert "command" not in container
    assert container["args"][0].startswith("-")


def test_app_database_keeps_its_keepalives(default_containers, custom_containers):
    for containers in (default_containers, custom_containers):
        settings = _server_settings(containers["postgres"])
        assert {name: settings[name] for name in KEEPALIVES} == KEEPALIVES


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_unset_value_renders_no_flag(key, unset_containers):
    assert "max_connections" not in _server_settings(unset_containers[key])


def test_unset_value_leaves_only_the_keepalives_or_no_args(unset_containers):
    assert _server_settings(unset_containers["postgres"]) == KEEPALIVES
    for key in set(DATABASES) - {"postgres"}:
        assert "args" not in unset_containers[key], key


# --- CloudNativePG: unchanged, and it means the same thing -------------------


def _cluster_parameters(*settings: str) -> dict[str, dict[str, str]]:
    return {
        document["metadata"]["name"]: document["spec"]["postgresql"]["parameters"]
        for document in _render(
            *settings, show_only=["templates/databases/cnpg-cluster.yaml"]
        )
        if document.get("kind") == "Cluster"
    }


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_cnpg_takes_it_as_a_parameter_and_renders_no_statefulset(key):
    _, component = DATABASES[key]
    settings = (f"databases.{key}.engine=cnpg", f"databases.{key}.maxConnections=250")
    parameters = _cluster_parameters(*settings)[f"{FULLNAME}-{component}"]
    assert parameters["max_connections"] == "250"
    statefulsets = [
        document
        for document in _render(*settings)
        if document.get("kind") == "StatefulSet"
    ]
    assert f"{FULLNAME}-{component}" not in {
        document["metadata"]["name"] for document in statefulsets
    }


def test_cnpg_still_defaults_an_unset_value_to_100():
    settings = []
    for key in DATABASES:
        settings += [
            f"databases.{key}.engine=cnpg",
            f"databases.{key}.maxConnections=null",
        ]
    parameters = _cluster_parameters(*settings)
    for key, (_, component) in DATABASES.items():
        assert parameters[f"{FULLNAME}-{component}"]["max_connections"] == "100", key


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_migrating_applies_one_value_on_both_sides(key):
    _, component = DATABASES[key]
    settings = (
        f"databases.{key}.engine=migrating",
        f"databases.{key}.maxConnections=250",
    )
    parameters = _cluster_parameters(*settings)[f"{FULLNAME}-{component}"]
    container = _containers(*settings)[key]
    assert parameters["max_connections"] == "250"
    assert _server_settings(container)["max_connections"] == "250"
