"""Deployment contract for the bundled Nextcloud's own Postgres: what a new
install connects to, what an existing install keeps, and who may reach it."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "nextcloud-db-proof"
FULLNAME = f"{RELEASE}-{yaml.safe_load((CHART / 'Chart.yaml').read_text())['name']}"
DB = f"{FULLNAME}-nextclouddb"

BUNDLED = (
    "--set",
    "opencloud.enabled=false",
    "--set",
    "nextcloud.enabled=true",
    "--set",
    "nextcloud.replicas=1",
)
PROTECTED = (
    "--set",
    "nextcloud.protectedEffect.enabled=true",
    "--set",
    "nextcloud.protectedEffect.hmacVaultPath=test/protected-effect",
)
SQLITE = ("--set", "nextcloud.database.type=sqlite")
CREATE_MODE = (
    "--set",
    "externalSecrets.enabled=false",
    "--set",
    "secrets.create=true",
)


def _render(*extra: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    return subprocess.run(
        [
            "helm",
            "template",
            RELEASE,
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            *extra,
        ],
        check=check,
        capture_output=True,
        text=True,
    )


def _documents(*extra: str) -> list[dict]:
    return [
        document
        for document in yaml.safe_load_all(_render(*extra).stdout)
        if isinstance(document, dict)
    ]


def _database_objects(documents: list[dict]) -> set[tuple[str, str]]:
    return {
        (document["kind"], document["metadata"]["name"])
        for document in documents
        if document["metadata"]["name"].startswith(DB)
    }


def _pod(documents: list[dict]) -> dict:
    deployment = next(
        document
        for document in documents
        if document["kind"] == "Deployment"
        and document["metadata"]["name"] == f"{FULLNAME}-nextcloud"
    )
    return deployment["spec"]["template"]["spec"]


def _containers(documents: list[dict]) -> dict[str, dict]:
    return {container["name"]: container for container in _pod(documents)["containers"]}


def _init(documents: list[dict]) -> dict[str, dict]:
    return {
        container["name"]: container for container in _pod(documents)["initContainers"]
    }


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env") or []}


def test_a_new_bundled_install_gets_its_own_postgres_by_default() -> None:
    documents = _documents(*BUNDLED)

    assert _database_objects(documents) == {
        ("PersistentVolumeClaim", f"{DB}-data"),
        ("StatefulSet", DB),
        ("Service", DB),
        ("NetworkPolicy", DB),
    }
    env = _env(_containers(documents)["nextcloud"])
    # The entrypoint checks SQLITE_DATABASE before POSTGRES_*: left in place it
    # would install SQLite anyway.
    assert "SQLITE_DATABASE" not in env
    assert env["POSTGRES_HOST"]["value"] == DB
    assert env["POSTGRES_DB"]["value"] == "nextcloud"
    assert env["POSTGRES_USER"]["value"] == "nextcloud"
    assert env["POSTGRES_PASSWORD"]["valueFrom"]["secretKeyRef"]["key"] == (
        "NEXTCLOUD_DB_PASSWORD"
    )
    # An installed Nextcloud keeps its own copy; only a new install needs it.
    assert env["POSTGRES_PASSWORD"]["valueFrom"]["secretKeyRef"]["optional"] is True
    # No installer-made oc_admin role whose password lives only in config.php.
    assert env["NC_setup_create_db_user"]["value"] == "false"

    statefulset = next(
        document
        for document in documents
        if document["kind"] == "StatefulSet" and document["metadata"]["name"] == DB
    )
    postgres = statefulset["spec"]["template"]["spec"]["containers"][0]
    database_env = _env(postgres)
    assert database_env["POSTGRES_USER"]["value"] == "nextcloud"
    assert database_env["POSTGRES_DB"]["value"] == "nextcloud"
    assert database_env["POSTGRES_PASSWORD"]["valueFrom"]["secretKeyRef"] == {
        "name": FULLNAME,
        "key": "NEXTCLOUD_DB_PASSWORD",
    }


def test_the_sqlite_opt_out_renders_no_database() -> None:
    documents = _documents(*BUNDLED, *SQLITE)

    assert _database_objects(documents) == set()
    env = _env(_containers(documents)["nextcloud"])
    assert env["SQLITE_DATABASE"]["value"] == "nextcloud"
    assert not any(name.startswith(("POSTGRES_", "PG")) for name in env)
    assert "NC_setup_create_db_user" not in env
    assert "wait-for-nextclouddb" not in _init(documents)


@pytest.mark.parametrize(
    "extra",
    [
        (
            "--set",
            "cloud.externalBackend=nextcloud",
            "--set",
            "cloud.externalUrl=https://c.example",
        ),
        ("--set", "nextcloud.enabled=true", "--set", "nextcloud.internal=false"),
        (*BUNDLED, "--set", "databases.nextcloud.enabled=false"),
    ],
    ids=["external-nextcloud", "nextcloud-not-internal", "database-disabled"],
)
def test_no_orphan_database_without_a_bundled_postgres_nextcloud(
    extra: tuple[str, ...],
) -> None:
    assert (
        _database_objects(_documents("--set", "opencloud.enabled=false", *extra))
        == set()
    )


def test_wait_for_nextclouddb_gates_only_a_new_install() -> None:
    documents = _documents(*BUNDLED)
    init = _init(documents)

    # Before the version guard and the install/upgrade step that needs it.
    assert list(init) == [
        "wait-for-keycloak",
        "wait-for-nextclouddb",
        "nextcloud-version-guard",
        "nextcloud-upgrade",
    ]
    wait = init["wait-for-nextclouddb"]
    assert wait["image"] == "postgres:16"
    assert wait["volumeMounts"] == [
        {"name": "data", "mountPath": "/var/www/html", "readOnly": True}
    ]
    env = _env(wait)
    assert env["PGHOST"]["value"] == DB
    assert env["PGUSER"]["value"] == "nextcloud"
    assert env["PGPASSWORD"]["valueFrom"]["secretKeyRef"]["optional"] is True
    script = wait["command"][2]
    installed = script.index("'installed' => true")
    sqlite = script.index("'dbtype' => 'sqlite3?'")
    # An installed SQLite Nextcloud never waits for Postgres...
    assert installed < sqlite < script.index("Installed on SQLite: not waiting")
    # ...an installed Postgres one waits only for the server, logging in with
    # config.php's own credentials rather than the Secret's...
    assert "until pg_isready -q" in script[sqlite:]
    # ...and a new install needs a password and a working login first.
    new_install = script.index('if [ -z "$PGPASSWORD" ]')
    assert script.index("psql -tAc 'SELECT 1'") > new_install
    assert "REFUSING TO INSTALL" in script


def test_cnpg_flips_every_connection_to_the_read_write_service() -> None:
    documents = _documents(*BUNDLED, "--set", "databases.nextcloud.engine=cnpg")

    assert ("Cluster", DB) in _database_objects(documents)
    assert ("StatefulSet", DB) not in _database_objects(documents)
    assert _env(_containers(documents)["nextcloud"])["POSTGRES_HOST"]["value"] == (
        f"{DB}-rw"
    )
    assert _env(_init(documents)["wait-for-nextclouddb"])["PGHOST"]["value"] == (
        f"{DB}-rw"
    )

    migrating = _documents(*BUNDLED, "--set", "databases.nextcloud.engine=migrating")
    # The import reads the legacy Service; consumers keep writing to it.
    assert _env(_containers(migrating)["nextcloud"])["POSTGRES_HOST"]["value"] == DB


def test_external_database_takes_its_user_from_the_secret() -> None:
    documents = _documents(
        *BUNDLED,
        "--set",
        "databases.nextcloud.internal=false",
        "--set",
        "databases.nextcloud.externalHost=pg.example.org",
        "--set",
        "databases.nextcloud.externalPort=6432",
    )
    env = _env(_containers(documents)["nextcloud"])
    assert env["POSTGRES_HOST"]["value"] == "pg.example.org"
    assert env["POSTGRES_USER"]["valueFrom"]["secretKeyRef"]["key"] == (
        "NEXTCLOUD_DB_USER"
    )
    assert _env(_init(documents)["wait-for-nextclouddb"])["PGPORT"]["value"] == "6432"

    missing = _render(
        *BUNDLED, "--set", "databases.nextcloud.internal=false", check=False
    )
    assert missing.returncode != 0
    assert "databases.nextcloud.externalHost is required" in missing.stderr


def test_only_the_nextcloud_pod_reaches_the_database() -> None:
    """Agent Pods envFrom the whole application Secret, password included."""
    policy = next(
        document
        for document in _documents(*BUNDLED)
        if document["kind"] == "NetworkPolicy" and document["metadata"]["name"] == DB
    )
    assert (
        policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"]
        == "nextclouddb"
    )
    (rule,) = policy["spec"]["ingress"]
    assert rule["ports"] == [{"port": 5432, "protocol": "TCP"}]
    assert [source["podSelector"]["matchLabels"] for source in rule["from"]] == [
        {
            "app.kubernetes.io/name": "superhuman-remote-worker",
            "app.kubernetes.io/instance": RELEASE,
            # The whole Pod: Apache, cron and the protected-effect pool.
            "app.kubernetes.io/component": "nextcloud",
        }
    ]

    cnpg = next(
        document
        for document in _documents(*BUNDLED, "--set", "databases.nextcloud.engine=cnpg")
        if document["kind"] == "NetworkPolicy" and document["metadata"]["name"] == DB
    )
    sources = [source for rule in cnpg["spec"]["ingress"] for source in rule["from"]]
    assert {"podSelector": {"matchLabels": {"cnpg.io/cluster": DB}}} in sources
    operator = next(
        rule
        for rule in cnpg["spec"]["ingress"]
        if any("namespaceSelector" in source for source in rule["from"])
    )
    assert {port["port"] for port in operator["ports"]} == {5432, 8000}


def test_secrets_create_generates_the_password_once_and_only_when_used() -> None:
    def app_secret(*extra: str) -> tuple[dict, str]:
        result = _render(*CREATE_MODE, *extra)
        documents = [
            d for d in yaml.safe_load_all(result.stdout) if isinstance(d, dict)
        ]
        secret = next(
            d
            for d in documents
            if d["kind"] == "Secret" and "APP_ENCRYPTION_KEY" in d.get("stringData", {})
        )
        return secret, result.stdout

    secret, _ = app_secret(*BUNDLED)
    assert len(secret["stringData"]["NEXTCLOUD_DB_PASSWORD"]) == 32
    assert "NEXTCLOUD_DB_PASSWORD" not in app_secret(*BUNDLED, *SQLITE)[0]["stringData"]
    # The chart default bundles OpenCloud, not Nextcloud.
    assert "NEXTCLOUD_DB_PASSWORD" not in app_secret()[0]["stringData"]

    # An operator value wins, and appears once: a second copy from the
    # pass-through loop would be a duplicate YAML key.
    secret, raw = app_secret(
        *BUNDLED, "--set", "secrets.values.NEXTCLOUD_DB_PASSWORD=op"
    )
    assert secret["stringData"]["NEXTCLOUD_DB_PASSWORD"] == "op"
    assert raw.count("NEXTCLOUD_DB_PASSWORD: ") == 1


def test_every_php_container_shares_the_sslmode() -> None:
    containers = _containers(
        _documents(*BUNDLED, *PROTECTED, "--set", "databases.nextcloud.sslMode=require")
    )
    for name in ("nextcloud", "cron", "nextcloud-protected-effect-fpm"):
        assert _env(containers[name])["PGSSLMODE"]["value"] == "require", name


def test_only_the_protected_pool_bounds_its_statements() -> None:
    """The lane's horizon assumes no write commits after the handler wall plus
    the safety margin; FPM's kill does not stop a statement Postgres runs."""

    def options(*extra: str) -> dict[str, str]:
        containers = _containers(_documents(*BUNDLED, *PROTECTED, *extra))
        assert "PGOPTIONS" not in _env(containers["nextcloud"])
        assert "PGOPTIONS" not in _env(containers["cron"])
        return {
            "PGOPTIONS": _env(containers["nextcloud-protected-effect-fpm"])[
                "PGOPTIONS"
            ]["value"],
            "digest": _env(containers["nextcloud-protected-effect-fpm"])[
                "NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"
            ]["value"],
        }

    default = options()
    # min(handler bound 10 s, safety margin 5 s)
    assert default["PGOPTIONS"] == (
        "-c statement_timeout=5000 -c idle_in_transaction_session_timeout=5000"
    )
    shorter = options("--set", "nextcloud.protectedEffect.handlerBoundSeconds=3")
    assert shorter["PGOPTIONS"] == (
        "-c statement_timeout=3000 -c idle_in_transaction_session_timeout=3000"
    )

    # Derived from lane values the digest covers; the database choice itself
    # leaves the lane digest alone.
    sqlite = _containers(_documents(*BUNDLED, *PROTECTED, *SQLITE))
    fpm = _env(sqlite["nextcloud-protected-effect-fpm"])
    assert "PGOPTIONS" not in fpm
    assert fpm["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]["value"] == default["digest"]


def test_an_unknown_database_type_is_refused() -> None:
    result = _render(*BUNDLED, "--set", "nextcloud.database.type=mysql", check=False)

    assert result.returncode != 0
    assert (
        "nextcloud.database.type" in result.stderr or "database/type" in result.stderr
    )
