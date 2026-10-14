"""Render assertions for the CloudNativePG migration path (Phase 4).

`engine: migrating` renders BOTH workloads and bootstraps the Cluster by logical
import from the legacy Service. `engine: cnpg` drops the StatefulSet and flips
the host helpers to the `-rw` Service. Both still default to `statefulset`, so
this ships inert.

Spec:  knowledge-base/knowledge/superpowers/specs/2026-08-21-cnpg-data-tier-ha-design.md
Plan:  knowledge-base/knowledge/superpowers/plans/2026-08-22-cnpg-migrations.md
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

RELEASE = "cnpg-test"
CHART_NAME = yaml.safe_load((CHART / "Chart.yaml").read_text())["name"]
FULLNAME = f"{RELEASE}-{CHART_NAME}"

# values key -> cluster/component name
DATABASES = {
    "postgres": "postgres",
    "vector": "pgvector",
    "audit": "auditdb",
    "gitea": "giteadb",
    "keycloak": "keycloakdb",
    "nextcloud": "nextclouddb",
}

CLUSTER = "templates/databases/cnpg-cluster.yaml"
OPERAND = "ghcr.io/cloudnative-pg/postgresql:16.15"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _template_command(*settings: str, show_only: str | None = None) -> list[str]:
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
    if show_only:
        command.extend(["--show-only", show_only])
    for setting in settings:
        command.extend(["--set", setting])
    return command


def _render(*settings: str, show_only: str | None = None) -> list[dict]:
    rendered = subprocess.run(
        _template_command(*settings, show_only=show_only),
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    return [document for document in yaml.safe_load_all(rendered) if document]


def _kinds(documents: list[dict], kind: str) -> list[dict]:
    return [d for d in documents if d.get("kind") == kind]


def _all_cnpg() -> list[str]:
    return [f"databases.{key}.engine=cnpg" for key in DATABASES]


def _clusters(*settings: str) -> dict[str, dict]:
    return {
        c["metadata"]["name"]: c
        for c in _kinds(_render(*settings, show_only=CLUSTER), "Cluster")
    }


def _seed_objects(*settings: str) -> dict[str, dict]:
    """kind -> object for the two `-llm-seed` hook resources.

    Rendered from the whole chart rather than with `--show-only`, because helm
    errors when `--show-only` matches nothing and "nothing" is exactly half of
    what these assertions are about.
    """
    return {
        document["kind"]: document
        for document in _render(*settings)
        if str(document.get("metadata", {}).get("name", "")).endswith("-llm-seed")
    }


def _configmap(*settings: str) -> dict:
    documents = _render(*settings, show_only="templates/configmap.yaml")
    return _kinds(documents, "ConfigMap")[0]["data"]


def test_every_cluster_defaults_to_the_operand_image():
    for cluster in _clusters(*_all_cnpg()).values():
        assert cluster["spec"]["imageName"] == OPERAND


def test_operand_tag_is_a_real_version_not_a_commit_sha():
    """imageName is part of the Cluster spec, so changing it rolls every
    Postgres cluster. A sha-* tag would do that on every push."""
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    for key in DATABASES:
        tag = values["databases"][key]["cnpgImage"].rsplit(":", 1)[1]
        assert re.fullmatch(r"\d+\.\d+(-\d+)?", tag), f"{key}: {tag!r}"


def test_operand_image_is_not_wired_into_the_per_commit_tag_job():
    workflow = (ROOT / ".github/workflows/develop.yml").read_text()
    assert "cnpgImage" not in workflow


def test_operand_is_upstream_not_a_custom_build():
    """CloudNativePG's operand already bundles pgvector, uuid-ossp and
    btree_gist. Building our own would mean owning a PostgreSQL image forever
    to gain nothing -- the versions are identical."""
    values = yaml.safe_load((CHART / "values.yaml").read_text())["databases"]
    for key in DATABASES:
        assert values[key]["cnpgImage"].startswith("ghcr.io/cloudnative-pg/postgresql:")


def test_operand_tag_is_pinned_not_floating():
    """A moving tag would roll every Postgres cluster whenever upstream
    publishes, because imageName is part of the Cluster spec."""
    values = yaml.safe_load((CHART / "values.yaml").read_text())["databases"]
    for key in DATABASES:
        tag = values[key]["cnpgImage"].rsplit(":", 1)[1]
        assert re.fullmatch(r"\d+\.\d+", tag), (
            f"{key}: {tag!r} is not a pinned patch version"
        )


# --- postgresql.parameters -------------------------------------------------


def test_parameters_passthrough_merges_with_max_connections():
    # 2GB here would now be refused by the memory-fits guard: against the 1Gi
    # default limit, maintenance_work_mem is also autovacuum_work_mem for each
    # of three workers. That refusal is the point of the guard, so this asserts
    # passthrough with a value that fits.
    cluster = _clusters(
        "databases.postgres.engine=cnpg",
        "databases.postgres.parameters.maintenance_work_mem=64MB",
    )[f"{FULLNAME}-postgres"]
    parameters = cluster["spec"]["postgresql"]["parameters"]
    assert parameters["max_connections"] == "100"
    assert parameters["maintenance_work_mem"] == "64MB"


def test_all_parameters_render_as_strings():
    """CNPG's parameters map is map[string]string. An unquoted integer makes
    the whole Cluster fail admission, which reads as a template bug."""
    cluster = _clusters(
        "databases.vector.engine=cnpg",
        "databases.vector.parameters.max_parallel_maintenance_workers=4",
    )[f"{FULLNAME}-pgvector"]
    for value in cluster["spec"]["postgresql"]["parameters"].values():
        assert isinstance(value, str)


def test_vector_ships_index_build_parameters():
    """A logical restore rebuilds all 3.9 GB of this database's HNSW indexes
    from scratch. The legacy server did that at maintenance_work_mem 64MB."""
    parameters = _clusters("databases.vector.engine=cnpg")[f"{FULLNAME}-pgvector"][
        "spec"
    ]["postgresql"]["parameters"]
    assert parameters["maintenance_work_mem"] == "2GB"
    assert parameters["max_parallel_maintenance_workers"] == "4"


def test_vector_has_headroom_for_the_index_build():
    """maintenance_work_mem above the pod's memory limit is an OOM kill, not a
    slow build. The legacy limit was 1Gi against ~169Mi steady-state."""
    resources = _clusters("databases.vector.engine=cnpg")[f"{FULLNAME}-pgvector"][
        "spec"
    ]["resources"]
    assert resources["limits"]["memory"] == "8Gi"


# Derived from limits.memory for every cnpg cluster; see databases.tuning.
DERIVED_PARAMETERS = {"shared_buffers", "effective_cache_size"}


def test_other_databases_ship_no_hand_written_parameters():
    """Only auditdb and pgvector carry parameters chosen by a human.

    Everything else on those clusters is either the connection budget or
    derived from the memory limit, so a new key appearing here means someone
    tuned a database by hand without recording why.
    """
    for key in ("postgres", "gitea", "keycloak"):
        component = DATABASES[key]
        parameters = _clusters(f"databases.{key}.engine=cnpg")[
            f"{FULLNAME}-{component}"
        ]["spec"]["postgresql"]["parameters"]
        assert set(parameters) == {"max_connections"} | DERIVED_PARAMETERS, key


def test_audit_ships_only_the_measured_parameter():
    """auditdb's work_mem was raised off a measurement (185 MB of sorts spilled
    to disk in 17 hours), not a hunch. Nothing else there is hand-set."""
    parameters = _clusters("databases.audit.engine=cnpg")[f"{FULLNAME}-auditdb"][
        "spec"
    ]["postgresql"]["parameters"]
    assert set(parameters) == {"max_connections", "work_mem"} | DERIVED_PARAMETERS


def test_nextcloud_ships_the_all_in_one_parameters_scaled_to_its_limit():
    """nextclouddb starts from Nextcloud all-in-one's Postgres settings.
    maintenance_work_mem is halved from its 256MB: it is also each autovacuum
    worker's budget, and at 1Gi three of them must fit beside shared_buffers."""
    parameters = _clusters("databases.nextcloud.engine=cnpg")[
        f"{FULLNAME}-nextclouddb"
    ]["spec"]["postgresql"]["parameters"]
    assert (
        set(parameters)
        == {
            "max_connections",
            "work_mem",
            "maintenance_work_mem",
        }
        | DERIVED_PARAMETERS
    )
    assert parameters["work_mem"] == "16MB"
    assert parameters["maintenance_work_mem"] == "128MB"
    assert parameters["shared_buffers"] == "256MB"


def test_legacy_statefulset_resources_are_untouched():
    """`resources` sizes the legacy pod too. Raising it there restarts a live
    database to reserve memory it never uses -- hence the separate key."""
    statefulsets = {d["metadata"]["name"]: d for d in _kinds(_render(), "StatefulSet")}
    container = statefulsets[f"{FULLNAME}-pgvector"]["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert container["resources"]["limits"]["memory"] == "1Gi"


def test_no_imagevolume_extension_against_a_mismatched_operand():
    """The only official pgvector extension images are PostgreSQL 18. The
    operand is 16 and already carries pgvector, so mounting one would be a
    major-version mismatch."""
    for cluster in _clusters(*_all_cnpg()).values():
        assert "extensions" not in cluster["spec"]["postgresql"], cluster["metadata"][
            "name"
        ]


# --- the import ------------------------------------------------------------

IMPORT_SOURCES = {
    "postgres": ("srw", "srw", "POSTGRES_PASSWORD"),
    "vector": ("srw_vector", "srw", "VECTOR_POSTGRES_PASSWORD"),
    "audit": ("srw_audit", "srw", "AUDIT_POSTGRES_PASSWORD"),
    "gitea": ("gitea", "gitea", "GITEA_DB_PASSWORD"),
    "keycloak": ("keycloak", "keycloak", "KC_DB_PASSWORD"),
    "nextcloud": ("nextcloud", "nextcloud", "NEXTCLOUD_DB_PASSWORD"),
}


def test_cnpg_engine_bootstraps_empty():
    """A fresh install has nothing to import from. Going straight to cnpg on
    an EXISTING database is how you get an empty one -- pass through
    migrating."""
    cluster = _clusters("databases.postgres.engine=cnpg")[f"{FULLNAME}-postgres"]
    assert "import" not in cluster["spec"]["bootstrap"]["initdb"]
    assert "externalClusters" not in cluster["spec"]


@pytest.mark.parametrize("key", sorted(IMPORT_SOURCES))
def test_migrating_imports_each_database_from_its_own_legacy_service(key):
    database, owner, password_key = IMPORT_SOURCES[key]
    component = DATABASES[key]
    cluster = _clusters(f"databases.{key}.engine=migrating")[f"{FULLNAME}-{component}"]

    import_spec = cluster["spec"]["bootstrap"]["initdb"]["import"]
    assert import_spec["type"] == "microservice"
    assert import_spec["databases"] == [database]

    source = [
        c
        for c in cluster["spec"]["externalClusters"]
        if c["name"] == import_spec["source"]["externalCluster"]
    ][0]
    assert source["connectionParameters"]["host"] == f"{FULLNAME}-{component}"
    assert source["connectionParameters"]["dbname"] == database
    assert source["connectionParameters"]["user"] == owner
    assert source["password"] == {"name": FULLNAME, "key": password_key}


def test_import_source_is_the_legacy_service_not_the_cluster_itself():
    """The Cluster and the legacy Service share a name. Pointing the import at
    `-rw` would have it bootstrap from itself."""
    cluster = _clusters("databases.postgres.engine=migrating")[f"{FULLNAME}-postgres"]
    host = cluster["spec"]["externalClusters"][0]["connectionParameters"]["host"]
    assert not host.endswith("-rw")


def test_import_parallelises_the_index_rebuild_for_vector():
    """Index creation happens in pg_restore's post-data section."""
    cluster = _clusters("databases.vector.engine=migrating")[f"{FULLNAME}-pgvector"]
    options = cluster["spec"]["bootstrap"]["initdb"]["import"][
        "pgRestorePostdataOptions"
    ]
    assert "--jobs=4" in options


def test_other_databases_do_not_parallelise_by_default():
    cluster = _clusters("databases.gitea.engine=migrating")[f"{FULLNAME}-giteadb"]
    assert (
        "pgRestorePostdataOptions"
        not in cluster["spec"]["bootstrap"]["initdb"]["import"]
    )


def test_migrating_still_renders_the_legacy_statefulset():
    """The import reads from the legacy Service, so it must still exist."""
    documents = _render("databases.postgres.engine=migrating")
    assert f"{FULLNAME}-postgres" in {
        d["metadata"]["name"] for d in _kinds(documents, "StatefulSet")
    }


# --- the -rw flip ----------------------------------------------------------


def test_hosts_are_unchanged_on_the_statefulset_engine():
    assert _configmap()["POSTGRES_HOST"] == f"{FULLNAME}-postgres"


@pytest.mark.parametrize(
    "key,variable,component",
    [
        ("postgres", "POSTGRES_HOST", "postgres"),
        ("vector", "VECTOR_POSTGRES_HOST", "pgvector"),
        ("audit", "AUDIT_POSTGRES_HOST", "auditdb"),
    ],
)
def test_hosts_still_point_at_the_legacy_service_while_migrating(
    key, variable, component
):
    """The import runs against the legacy Service and consumers keep writing
    to it. Repointing here would cut over before the data has arrived."""
    assert (
        _configmap(f"databases.{key}.engine=migrating")[variable]
        == f"{FULLNAME}-{component}"
    )


@pytest.mark.parametrize(
    "key,variable,component",
    [
        ("postgres", "POSTGRES_HOST", "postgres"),
        ("vector", "VECTOR_POSTGRES_HOST", "pgvector"),
        ("audit", "AUDIT_POSTGRES_HOST", "auditdb"),
    ],
)
def test_cnpg_engine_points_at_the_read_write_service(key, variable, component):
    assert (
        _configmap(f"databases.{key}.engine=cnpg")[variable]
        == f"{FULLNAME}-{component}-rw"
    )


def test_no_template_hardcodes_a_database_service_name():
    """The -rw flip only reaches a consumer that goes through the helper. A
    template that spells the Service name itself keeps pointing at the retired
    StatefulSet after cutover, silently -- the canvas gateway and the Gitea
    init container are exactly the kind of consumer that could.

    Checked at the source, because enabling every consumer for a render test
    means satisfying each one's unrelated schema prerequisites."""
    helpers = {
        "postgres": "srw.postgresHost",
        "pgvector": "srw.vectorPostgresHost",
        "auditdb": "srw.auditPostgresHost",
        "giteadb": "srw.giteaDbHost",
        "keycloakdb": "srw.keycloakDbJdbcUrl",
        "nextclouddb": "srw.nextcloudDbHost",
    }
    offenders = []
    for template in (CHART / "templates").rglob("*.yaml"):
        if template.name.startswith("cnpg-") or template.name.startswith("postgres"):
            continue  # the database templates themselves legitimately name it
        body = template.read_text()
        for component in helpers:
            for line in body.splitlines():
                if f'"srw.fullname" . }}}}-{component}' not in line:
                    continue
                # Identity references, not connection targets:
                #   name:            the resource's own name
                #   cnpg.io/cluster: a label selector naming the CNPG cluster,
                #                    which shares the Service's name by design
                # Anything else spelling the Service name is something trying
                # to CONNECT to it, which is what must go through the helper.
                key = line.strip().split(":", 1)[0].lstrip("- ")
                if key in {"name", "cnpg.io/cluster"}:
                    continue
                offenders.append(f"{template.relative_to(CHART)} spells -{component}")
    assert not offenders, "; ".join(offenders)


def test_external_mode_is_untouched_by_the_engine():
    """internal=false means someone else's database. The engine is irrelevant
    and must not append anything to their hostname."""
    config = _configmap(
        "databases.postgres.internal=false",
        "databases.postgres.externalHost=pg.example.com",
        "databases.postgres.engine=cnpg",
    )
    assert config["POSTGRES_HOST"] == "pg.example.com"


def test_gitea_host_flips_with_its_port():
    documents = _render("databases.gitea.engine=cnpg")
    gitea = [
        d
        for d in _kinds(documents, "StatefulSet")
        if d["metadata"]["name"] == f"{FULLNAME}-gitea"
    ][0]
    env = {
        e["name"]: e.get("value")
        for e in gitea["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    # Gitea takes host:port as one string.
    assert env["GITEA__database__HOST"] == f"{FULLNAME}-giteadb-rw:5432"


def test_keycloak_jdbc_url_flips():
    documents = _render("databases.keycloak.engine=cnpg")
    keycloak = [
        d
        for d in _kinds(documents, "Deployment")  # Keycloak is a Deployment
        if d["metadata"]["name"].endswith("-keycloak")
    ][0]
    env = {
        e["name"]: e.get("value")
        for e in keycloak["spec"]["template"]["spec"]["containers"][0]["env"]
    }
    assert (
        env["KC_DB_URL"] == f"jdbc:postgresql://{FULLNAME}-keycloakdb-rw:5432/keycloak"
    )


# --- storage ---------------------------------------------------------------


def test_cluster_storage_defaults_to_the_shared_size():
    cluster = _clusters("databases.postgres.engine=cnpg")[f"{FULLNAME}-postgres"]
    assert cluster["spec"]["storage"]["size"] == "16Gi"


def test_cluster_storage_can_be_sized_independently():
    """A logical restore needs room for the data, the indexes it rebuilds AND
    the WAL it generates, all on one PVC -- so a Cluster wants more than the
    legacy pod ever did."""
    cluster = _clusters(
        "databases.vector.engine=cnpg", "databases.vector.cnpgStorageSize=64Gi"
    )[f"{FULLNAME}-pgvector"]
    assert cluster["spec"]["storage"]["size"] == "64Gi"


def test_legacy_pvc_is_untouched_by_the_cluster_size():
    """storageSize also sizes the legacy PVC, and a PVC cannot shrink. Raising
    it there to give the Cluster headroom would be a one-way change to a live
    database's volume."""
    documents = _render("databases.vector.cnpgStorageSize=64Gi")
    claims = {
        d["metadata"]["name"]: d["spec"]["resources"]["requests"]["storage"]
        for d in _kinds(documents, "PersistentVolumeClaim")
    }
    assert claims[f"{FULLNAME}-pgvector-data"] == "16Gi"


# --- the migrating-mode label collision -------------------------------------


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_migrating_cluster_pods_are_not_selected_by_the_legacy_service(key):
    """`migrating` renders BOTH workloads. Phase 2 gives CNPG pods the chart's
    component labels so NetworkPolicies keep matching -- but the legacy Service
    selects on those same labels, so it would load-balance live traffic across
    the real database and a half-imported copy, and writes landing on the new
    one would be lost when the import is discarded."""
    component = DATABASES[key]
    documents = _render(f"databases.{key}.engine=migrating")

    service = [
        d
        for d in _kinds(documents, "Service")
        if d["metadata"]["name"] == f"{FULLNAME}-{component}"
    ][0]
    selector = service["spec"]["selector"]

    cluster = [
        d
        for d in _kinds(documents, "Cluster")
        if d["metadata"]["name"] == f"{FULLNAME}-{component}"
    ][0]
    inherited = cluster["spec"].get("inheritedMetadata", {}).get("labels", {})

    matched = all(inherited.get(k) == v for k, v in selector.items())
    assert not matched, (
        f"the legacy {component} Service would select the migrating Cluster's pods: "
        f"selector={selector} inherited={inherited}"
    )


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_cnpg_cluster_pods_keep_the_component_labels(key):
    """Once the StatefulSet is gone there is no Service to collide with, and
    the labels are what keep the NetworkPolicies matching."""
    component = DATABASES[key]
    cluster = _clusters(f"databases.{key}.engine=cnpg")[f"{FULLNAME}-{component}"]
    labels = cluster["spec"]["inheritedMetadata"]["labels"]
    assert labels["app.kubernetes.io/component"] == component


class TestWriterQuiesce:
    """Scaling the writers to zero for a cutover, through GitOps.

    A cutover imports from a live source. Anything written between `pg_dump`'s
    snapshot and the traffic flip is silently absent from the target -- the
    pgvector rehearsal measured 40 such rows in 63 minutes. So each database's
    writer has to be stoppable, and stoppable *through Fleet*: a `kubectl scale`
    is reverted by GitOps within ~90s, mid-migration.
    """

    def test_gitea_replicas_defaults_to_one(self):
        sts = _kinds(_render(show_only="templates/services/gitea.yaml"), "StatefulSet")
        assert len(sts) == 1
        assert sts[0]["spec"]["replicas"] == 1

    def test_gitea_can_be_quiesced_to_zero(self):
        """Regression: `default 1` renders 0 as 1, because Helm's `default`
        treats zero as empty. The quiesce would have silently not happened."""
        sts = _kinds(
            _render("gitea.replicas=0", show_only="templates/services/gitea.yaml"),
            "StatefulSet",
        )
        assert sts[0]["spec"]["replicas"] == 0

    def test_keycloak_can_be_quiesced_to_zero(self):
        deployments = _kinds(
            _render(
                "keycloak.replicas=0", show_only="templates/services/keycloak.yaml"
            ),
            "Deployment",
        )
        assert deployments, "keycloak Deployment did not render"
        assert deployments[0]["spec"]["replicas"] == 0

    def test_disabling_gitea_is_not_a_quiesce_because_it_drops_the_volume(self):
        """Guard, not a preference. `gitea.enabled` gates the whole template
        including the PVC, so reaching for it to quiet Gitea during a cutover
        deletes the data volume. If this ever stops being true the runbook's
        warning should be revisited -- until then, keep the warning."""
        enabled = _render(show_only="templates/services/gitea.yaml")
        assert _kinds(enabled, "PersistentVolumeClaim"), "expected a PVC when enabled"

        disabled = subprocess.run(
            _template_command(
                "gitea.enabled=false", show_only="templates/services/gitea.yaml"
            ),
            capture_output=True,
            text=True,
        )
        # helm errors when --show-only matches nothing, which is itself the proof.
        rendered = [d for d in yaml.safe_load_all(disabled.stdout) if d]
        assert not _kinds(rendered, "PersistentVolumeClaim")
        assert not _kinds(rendered, "StatefulSet")

    # --- the llm-seed hook, which used to make the quiesce impossible -------

    def test_the_llm_seed_hook_renders_while_the_orchestrator_runs(self):
        """Control for the two tests below: with the flag on and replicas at the
        chart default, both halves of the hook are present."""
        objects = _seed_objects("llm.seed.enabled=true")
        assert sorted(objects) == ["ConfigMap", "Job"]

    def test_a_quiesced_orchestrator_renders_no_seed_job(self):
        """The deadlock this whole file exists to avoid. The seed Job is a
        post-upgrade hook that waits on the orchestrator's health endpoint, so
        at replicas 0 it can never finish -- Helm keeps waiting, the release
        stays `pending-upgrade`, and every later apply is blocked, including the
        one that would scale the orchestrator back up. Observed twice on
        2026-08-23. A quiesce apply must therefore carry no hook at all.

        Also a regression test for Helm's `default`, which treats 0 as empty: a
        guard written as `.Values.orchestrator.replicas | default 1` renders the
        Job straight through the quiesce."""
        objects = _seed_objects("llm.seed.enabled=true", "orchestrator.replicas=0")
        assert "Job" not in objects

    def test_the_seed_payload_configmap_survives_the_quiesce(self):
        """Deliberate asymmetry, not an oversight. The ConfigMap is a
        hook-weight-0 hook that Helm applies without waiting for anything, so it
        cannot wedge an apply. Leaving it rendered keeps it in step with the Job
        for the apply that restores the replicas."""
        objects = _seed_objects("llm.seed.enabled=true", "orchestrator.replicas=0")
        assert "ConfigMap" in objects

    def test_the_seed_flag_switches_the_whole_hook_off(self):
        """The explicit lever, for an operator who wants a hook-free apply
        without touching replicas. Off is also the chart default."""
        assert _seed_objects("llm.seed.enabled=false") == {}
        assert _seed_objects() == {}

    def test_the_orchestrator_wait_terminates_and_fails_the_hook(self):
        """The wait was `until wget ...; do sleep 3; done` with no ceiling, so a
        rollout that never became Ready held Helm open forever. Run the rendered
        script with a probe that always fails and the sleep removed: it must
        stop, and it must stop non-zero.

        Non-zero on purpose. A failed hook marks the release `failed`, which is
        re-appliable; `pending-upgrade` is not, which is what made the original
        bug a deadlock rather than a slow rollout. Exiting 0 would hand the seed
        job a schema the orchestrator never applied."""
        job = _seed_objects("llm.seed.enabled=true")["Job"]
        waits = [
            c
            for c in job["spec"]["template"]["spec"]["initContainers"]
            if c["name"] == "wait-for-orchestrator"
        ]
        assert len(waits) == 1
        script = waits[0]["command"][-1]

        ceiling = int(re.search(r"-ge (\d+)", script).group(1))
        neutered = re.sub(r"until wget .*; do", "until false; do", script).replace(
            "sleep 3", "true"
        )
        assert "until false; do" in neutered, "probe substitution missed the loop"

        result = subprocess.run(
            ["sh", "-c", neutered], capture_output=True, text=True, timeout=30
        )
        assert result.returncode == 1, result.stdout
        assert "orchestrator not ready" in result.stdout
        # One attempt short of the ceiling, then the bail-out -- pins the
        # arithmetic, not just the fact that it stops.
        assert result.stdout.count("waiting for orchestrator") == ceiling - 1

    def test_the_seed_job_is_capped_end_to_end(self):
        """backoffLimit is not a ceiling: it retries the pod, so the bounded
        init container alone still allows ~3x the wait. activeDeadlineSeconds
        is what bounds the hook, and therefore the apply."""
        job = _seed_objects("llm.seed.enabled=true")["Job"]["spec"]
        assert job["activeDeadlineSeconds"] == 600
        assert job["backoffLimit"] == 2
