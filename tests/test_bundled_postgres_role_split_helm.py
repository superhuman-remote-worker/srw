"""Render assertions: the bundled app, pgvector and audit StatefulSets split roles.

The StatefulSets used to hand SRW's own credentials to the image as
POSTGRES_USER/POSTGRES_PASSWORD, which the image makes the bootstrap
SUPERUSER, and every agent pod carries those credentials. Now `postgres` is
the superuser, with a password in a Secret of its own that no application pod
references, and helm/files/postgres-roles.sh makes SRW's role an ordinary owner
(at initdb, and from an init container on every start for existing volumes).

The script's behaviour is tested against real servers in
test_bundled_postgres_role_split_real_postgres.py.

Issue: knowledge-base/knowledge/issues/bundled_postgres_app_role_is_superuser.md
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
SCRIPT = CHART / "files" / "postgres-roles.sh"
MIGRATIONS = ROOT / "src" / "orchestrator" / "database" / "migrations"

RELEASE = "rolesplit-test"
CHART_NAME = yaml.safe_load((CHART / "Chart.yaml").read_text())["name"]
FULLNAME = f"{RELEASE}-{CHART_NAME}"
APP_SECRET = FULLNAME
SUPERUSER_SECRET = f"{FULLNAME}-db-superuser"

# values key -> (StatefulSet component, app Secret key prefix, migrations chain)
SPLIT = {
    "postgres": ("postgres", "POSTGRES", "app"),
    "vector": ("pgvector", "VECTOR_POSTGRES", "vector"),
    "audit": ("auditdb", "AUDIT_POSTGRES", "audit"),
}
# The other bundled databases keep their image-default superuser (out of scope).
UNSPLIT = {"giteadb", "keycloakdb", "nextclouddb"}

CANVAS = [
    "canvas.livePreview.enabled=true",
    "canvas.livePreview.viewer.enabled=true",
    "canvas.livePreview.viewer.database.credentials.create=true",
    "canvas.livePreview.viewer.database.provisionRole=true",
]
CANVAS_STRINGS = [
    "canvas.livePreview.viewer.deploymentProfile=development",
    "canvas.livePreview.viewer.cookieMode=development-cookie-free",
    "canvas.livePreview.viewer.domain=example-userland.test",
    "canvas.livePreview.viewer.hostSuffix=.canvas.example-userland.test",
    "canvas.livePreview.viewer.cockpitOrigins[0]=https://cockpit.example.test",
    "canvas.livePreview.viewer.networkPolicy.edgeNamespaceSelector.matchLabels.edge=trusted",
    "canvas.livePreview.viewer.networkPolicy.edgePodSelector.matchLabels.app=viewer-edge",
]

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(*settings: str, strings: tuple[str, ...] = (), check: bool = True):
    command = [
        "helm",
        "template",
        RELEASE,
        str(CHART),
        "-f",
        str(CHART / "ci/test-values.yaml"),
        "--set",
        "nextcloud.enabled=true",
        "--set",
        "opencloud.enabled=false",
    ]
    for setting in settings:
        command.extend(["--set", setting])
    for setting in strings:
        command.extend(["--set-string", setting])
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if not check:
        return result
    assert result.returncode == 0, result.stderr
    return [document for document in yaml.safe_load_all(result.stdout) if document]


def _statefulset(documents: list[dict], component: str) -> dict:
    [statefulset] = [
        document
        for document in documents
        if document["kind"] == "StatefulSet"
        and document["metadata"]["name"] == f"{FULLNAME}-{component}"
    ]
    return statefulset


def _env(container: dict) -> dict[str, object]:
    resolved = {}
    for entry in container.get("env", []):
        if "value" in entry:
            resolved[entry["name"]] = entry["value"]
        else:
            ref = entry["valueFrom"]
            kind, body = next(iter(ref.items()))
            resolved[entry["name"]] = (kind, body["name"], body["key"])
    return resolved


def _pod_specs(documents: list[dict]):
    """(kind/name, pod spec) for everything that runs a pod."""
    for document in documents:
        spec = document.get("spec") or {}
        if document["kind"] in {"Deployment", "StatefulSet", "DaemonSet", "Job", "ReplicaSet"}:
            yield f"{document['kind']}/{document['metadata']['name']}", spec["template"]["spec"]
        elif document["kind"] == "CronJob":
            yield (
                f"CronJob/{document['metadata']['name']}",
                spec["jobTemplate"]["spec"]["template"]["spec"],
            )
        elif document["kind"] == "Pod":
            yield f"Pod/{document['metadata']['name']}", spec


def _secret_readers(pod_spec: dict, secret: str) -> set[str]:
    """Containers of a pod spec that can read ``secret``, any way at all."""
    readers = set()
    mounted = {
        volume["name"]
        for volume in pod_spec.get("volumes", [])
        if (volume.get("secret") or {}).get("secretName") == secret
        or any(
            (source.get("secret") or {}).get("name") == secret
            for source in (volume.get("projected") or {}).get("sources", [])
        )
    }
    for container in pod_spec.get("initContainers", []) + pod_spec.get("containers", []):
        if any(
            (entry.get("secretRef") or {}).get("name") == secret
            for entry in container.get("envFrom", [])
        ):
            readers.add(container["name"])
        if any(
            (entry.get("valueFrom", {}).get("secretKeyRef") or {}).get("name") == secret
            for entry in container.get("env", [])
        ):
            readers.add(container["name"])
        if any(mount["name"] in mounted for mount in container.get("volumeMounts", [])):
            readers.add(container["name"])
    return readers


@pytest.mark.parametrize("key", sorted(SPLIT))
def test_statefulset_makes_postgres_the_superuser_and_runs_the_split(key: str) -> None:
    component, prefix, _ = SPLIT[key]
    statefulset = _statefulset(_render(), component)
    pod = statefulset["spec"]["template"]
    [database] = pod["spec"]["containers"]
    [init] = pod["spec"]["initContainers"]

    expected = {
        "POSTGRES_USER": "postgres",
        "POSTGRES_PASSWORD": (
            "secretKeyRef",
            SUPERUSER_SECRET,
            f"{prefix}_SUPERUSER_PASSWORD",
        ),
        "SRW_APP_USER": ("secretKeyRef", APP_SECRET, f"{prefix}_USER"),
        "SRW_APP_PASSWORD": ("secretKeyRef", APP_SECRET, f"{prefix}_PASSWORD"),
        "SRW_APP_DB": ("configMapKeyRef", f"{FULLNAME}-config", f"{prefix}_DB"),
    }
    for container in (database, init):
        env = _env(container)
        for name, value in expected.items():
            assert env[name] == value, (container["name"], name)
        # POSTGRES_DB would make initdb create the database owned by the
        # superuser; the script creates it owned by SRW's role instead.
        assert "POSTGRES_DB" not in env

    assert init["name"] == "postgres-roles"
    assert init["image"] == database["image"]
    assert init["command"] == ["bash", "/srw-postgres-roles/postgres-roles.sh", "upgrade"]
    assert {(m["name"], m["mountPath"]) for m in init["volumeMounts"]} == {
        ("data", "/var/lib/postgresql/data"),
        ("postgres-roles", "/srw-postgres-roles"),
    }
    assert {
        "name": "postgres-roles",
        "mountPath": "/docker-entrypoint-initdb.d",
        "readOnly": True,
    } in database["volumeMounts"]

    [volume] = [v for v in pod["spec"]["volumes"] if v["name"] == "postgres-roles"]
    assert volume["configMap"]["name"] == f"{FULLNAME}-postgres-roles"
    # docker-entrypoint.sh executes an executable *.sh; mode 0555.
    assert volume["configMap"]["defaultMode"] == 0o555

    # A change to the script must restart the pod, or it never runs.
    assert pod["metadata"]["annotations"]["checksum/postgres-roles"]


def test_extension_lists_cover_every_migration() -> None:
    """The superuser creates, before migrations, every extension they ask for.

    `vector` is not a trusted extension: if a migration names one the chart's
    list lacks, the non-superuser migration fails on a fresh install.
    """
    documents = _render()
    pattern = re.compile(
        r"CREATE\s+EXTENSION\s+IF\s+NOT\s+EXISTS\s+\"?([a-z0-9_-]+)\"?", re.IGNORECASE
    )
    for key, (component, _, chain) in SPLIT.items():
        wanted = set()
        for migration in (MIGRATIONS / chain).glob("*.sql"):
            for line in migration.read_text().splitlines():
                if line.lstrip().startswith("--"):
                    continue
                wanted.update(name.lower() for name in pattern.findall(line))
        statefulset = _statefulset(documents, component)
        [init] = statefulset["spec"]["template"]["spec"]["initContainers"]
        listed = set(str(_env(init)["SRW_DB_EXTENSIONS"]).split())
        assert wanted <= listed, (key, wanted - listed)


def test_the_scan_finds_the_privileged_extension() -> None:
    """Guards the scan above against matching nothing at all."""
    text = "\n".join(p.read_text() for p in (MIGRATIONS / "vector").glob("*.sql"))
    assert re.search(r"CREATE EXTENSION IF NOT EXISTS vector", text)


def test_superuser_secret_is_separate_generated_and_kept() -> None:
    documents = _render()
    [secret] = [
        document
        for document in documents
        if document["kind"] == "Secret" and document["metadata"]["name"] == SUPERUSER_SECRET
    ]
    assert secret["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    assert set(secret["stringData"]) == {
        "POSTGRES_SUPERUSER_PASSWORD",
        "VECTOR_POSTGRES_SUPERUSER_PASSWORD",
        "AUDIT_POSTGRES_SUPERUSER_PASSWORD",
    }
    passwords = list(secret["stringData"].values())
    assert all(len(password) >= 32 for password in passwords)
    assert len(set(passwords)) == 3

    [configmap] = [
        document
        for document in documents
        if document["kind"] == "ConfigMap"
        and document["metadata"]["name"] == f"{FULLNAME}-postgres-roles"
    ]
    assert configmap["data"]["postgres-roles.sh"] == SCRIPT.read_text()


def test_operator_value_wins_and_never_reaches_the_application_secret() -> None:
    documents = _render(
        "secrets.create=true",
        strings=("secrets.values.POSTGRES_SUPERUSER_PASSWORD=from-the-values-file-0123456789",),
    )
    secrets = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "Secret"
    }
    assert (
        secrets[SUPERUSER_SECRET]["stringData"]["POSTGRES_SUPERUSER_PASSWORD"]
        == "from-the-values-file-0123456789"
    )
    # Agent pods envFrom the application Secret whole.
    for name, secret in secrets.items():
        if name == SUPERUSER_SECRET:
            continue
        assert "POSTGRES_SUPERUSER_PASSWORD" not in (secret.get("stringData") or {}), name
        assert "from-the-values-file-0123456789" not in yaml.safe_dump(secret), name


def test_no_application_pod_can_read_the_superuser_secret() -> None:
    """Only the three database pods and the Canvas role Job's identity step."""
    documents = _render(
        *CANVAS,
        "pgadmin.enabled=true",
        strings=tuple(CANVAS_STRINGS),
    )
    readers = {}
    for name, pod_spec in _pod_specs(documents):
        containers = _secret_readers(pod_spec, SUPERUSER_SECRET)
        if containers:
            readers[name] = containers
    database_pods = {
        f"StatefulSet/{FULLNAME}-{component}": {"postgres-roles", container}
        for component, container in (
            ("postgres", "postgres"),
            ("pgvector", "pgvector"),
            ("auditdb", "auditdb"),
        )
    }
    canvas_job = {
        name: containers for name, containers in readers.items() if "canvas-gateway-role" in name
    }
    assert {name: containers for name, containers in readers.items() if name not in canvas_job} == (
        database_pods
    )
    assert list(canvas_job.values()) == [{"reconcile-legacy-identity"}]


def test_canvas_role_job_creates_its_login_as_the_superuser_only() -> None:
    documents = _render(*CANVAS, strings=tuple(CANVAS_STRINGS))
    [job] = [
        document
        for document in documents
        if document["kind"] == "Job" and "canvas-gateway-role" in document["metadata"]["name"]
    ]
    users = {
        container["name"]: _env(container).get("PGUSER")
        for container in job["spec"]["template"]["spec"]["initContainers"]
    }
    assert users.pop("reconcile-legacy-identity") == "postgres"
    assert users.pop("prove-target-password") == "srw_canvas_gateway"
    # Every other step stays the database owner.
    assert set(users.values()) == {("secretKeyRef", APP_SECRET, "POSTGRES_USER")}


def test_cnpg_and_unsplit_databases_render_no_split() -> None:
    documents = _render(
        "databases.postgres.engine=cnpg",
        "databases.vector.engine=cnpg",
        "databases.audit.engine=cnpg",
    )
    names = {(d["kind"], d["metadata"]["name"]) for d in documents}
    assert ("Secret", SUPERUSER_SECRET) not in names
    assert ("ConfigMap", f"{FULLNAME}-postgres-roles") not in names

    documents = _render()
    for component in UNSPLIT:
        statefulset = _statefulset(documents, component)
        assert "initContainers" not in statefulset["spec"]["template"]["spec"], component


def test_one_cnpg_database_keeps_the_others_split() -> None:
    documents = _render("databases.vector.engine=cnpg")
    names = {d["metadata"]["name"] for d in documents if d["kind"] == "StatefulSet"}
    assert f"{FULLNAME}-pgvector" not in names
    for component in ("postgres", "auditdb"):
        assert _statefulset(documents, component)["spec"]["template"]["spec"]["initContainers"]


def test_app_role_named_like_the_superuser_fails_the_render() -> None:
    result = _render(
        "secrets.create=true",
        strings=("secrets.values.POSTGRES_USER=postgres",),
        check=False,
    )
    assert result.returncode != 0
    assert "bootstrap superuser" in result.stderr
