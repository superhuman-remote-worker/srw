"""Render assertions for the optional pgAdmin (``optional/pgadmin.yaml``).

pgAdmin wrote the StatefulSet Service names ``<fullname>-postgres``,
``-pgvector`` and ``-auditdb`` literally, in ``servers.json``, ``.pgpass`` and
a wait loop. CloudNativePG names its Services ``-rw``, so the wait never
ended, the pod stayed in ``Init`` and ``helm --wait`` failed the release
(prod-private, 2026-11-16: two failed revisions;
pgadmin_waits_forever_for_statefulset_hosts_on_cnpg). The hosts now come from
the chart's host helpers, and there is no wait.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "pga-test"
CHART_NAME = yaml.safe_load((CHART / "Chart.yaml").read_text())["name"]
FULLNAME = f"{RELEASE}-{CHART_NAME}"
TEMPLATE = "templates/optional/pgadmin.yaml"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)

#: A StatefulSet Service name with no engine suffix after it.
UNSUFFIXED = re.compile(rf"{re.escape(FULLNAME)}-(postgres|pgvector|auditdb)(?![-\w])")

ALL_CNPG = [f"databases.{key}.engine=cnpg" for key in ("postgres", "vector", "audit")]
ALL_EXTERNAL = [
    setting
    for key, host, port in (
        ("postgres", "app.db.example.net", 5433),
        ("vector", "vector.db.example.net", 5434),
        ("audit", "audit.db.example.net", 5435),
    )
    for setting in (
        f"databases.{key}.internal=false",
        f"databases.{key}.externalHost={host}",
        f"databases.{key}.externalPort={port}",
    )
]


def _render(*settings: str) -> list[dict]:
    command = [
        "helm",
        "template",
        RELEASE,
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--set",
        "pgadmin.enabled=true",
        "--show-only",
        TEMPLATE,
    ]
    for setting in settings:
        command.extend(["--set", setting])
    rendered = subprocess.run(
        command, check=True, capture_output=True, text=True
    ).stdout
    return [document for document in yaml.safe_load_all(rendered) if document]


def _servers(documents: list[dict]) -> dict[str, dict]:
    (config_map,) = [d for d in documents if d["kind"] == "ConfigMap"]
    servers = json.loads(config_map["data"]["servers.json"])["Servers"]
    return {server["Name"]: server for server in servers.values()}


def _pod(documents: list[dict]) -> dict:
    (deployment,) = [d for d in documents if d["kind"] == "Deployment"]
    return deployment["spec"]["template"]["spec"]


def _pgpass_lines(documents: list[dict]) -> list[str]:
    (init,) = _pod(documents)["initContainers"]
    assert init["name"] == "assemble-pgpass"
    script = init["command"][-1]
    return [line.strip() for line in script.splitlines() if "%s:%s:*" in line]


@pytest.mark.parametrize(
    "settings",
    [pytest.param([], id="statefulset"), pytest.param(ALL_CNPG, id="cnpg")],
)
def test_no_wait_for_the_databases(settings):
    """pgAdmin starts with a server that is down; a wait only ever hung."""
    pod = _pod(_render(*settings))
    names = [c["name"] for c in pod.get("initContainers", [])]
    assert names == ["assemble-pgpass"]
    assert "nc -z" not in json.dumps(pod)


def test_cnpg_hosts_carry_the_rw_suffix():
    documents = _render(*ALL_CNPG)

    hosts = {name: server["Host"] for name, server in _servers(documents).items()}
    assert hosts == {
        "SRW App DB": f"{FULLNAME}-postgres-rw",
        "SRW Vector DB (pgvector)": f"{FULLNAME}-pgvector-rw",
        "SRW Audit DB": f"{FULLNAME}-auditdb-rw",
    }
    lines = _pgpass_lines(documents)
    for component in ("postgres", "pgvector", "auditdb"):
        assert any(f'"{FULLNAME}-{component}-rw" "5432"' in line for line in lines)
    assert not UNSUFFIXED.search(json.dumps(documents))


def test_external_databases_use_their_own_host_and_port():
    documents = _render(*ALL_EXTERNAL)

    servers = _servers(documents)
    assert {(s["Host"], s["Port"]) for s in servers.values()} == {
        ("app.db.example.net", 5433),
        ("vector.db.example.net", 5434),
        ("audit.db.example.net", 5435),
    }
    lines = _pgpass_lines(documents)
    assert any('"vector.db.example.net" "5434"' in line for line in lines)
    assert FULLNAME + "-postgres" not in json.dumps(documents)
    assert not UNSUFFIXED.search(json.dumps(documents))


def test_a_statefulset_release_keeps_its_service_names():
    """Negative control: the default engine's names are the unsuffixed ones."""
    servers = _servers(_render())
    assert servers["SRW App DB"]["Host"] == f"{FULLNAME}-postgres"
    assert servers["SRW App DB"]["Port"] == 5432


def test_without_preregistration_there_is_no_init_container():
    documents = _render("pgadmin.preregisterDatabases=false")
    assert "initContainers" not in _pod(documents)
    assert not [d for d in documents if d["kind"] == "ConfigMap"]
