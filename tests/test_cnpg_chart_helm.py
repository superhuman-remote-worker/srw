"""Render assertions for the CloudNativePG data-tier templates.

Phase 2 of the CNPG migration ships **inert**: every database's ``engine``
defaults to ``statefulset``, so a default render is byte-identical to the
StatefulSet-only chart. These tests pin both halves — that the switch does
nothing until flipped, and that flipping it produces a correct ``Cluster``.

Spec:  knowledge-base/knowledge/superpowers/specs/2026-08-21-cnpg-data-tier-ha-design.md
Plan:  knowledge-base/knowledge/superpowers/plans/2026-08-21-cnpg-chart-foundation.md
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

RELEASE = "cnpg-test"
CHART_NAME = yaml.safe_load((CHART / "Chart.yaml").read_text())["name"]
# `srw.fullname` is "<release>-<chart name>" absent an override.
FULLNAME = f"{RELEASE}-{CHART_NAME}"

# values key -> (cluster/component name, database, owner)
DATABASES = {
    "postgres": ("postgres", "srw", "srw"),
    "vector": ("pgvector", "srw_vector", "srw"),
    "audit": ("auditdb", "srw_audit", "srw"),
    "gitea": ("giteadb", "gitea", "gitea"),
    "keycloak": ("keycloakdb", "keycloak", "keycloak"),
    "nextcloud": ("nextclouddb", "nextcloud", "nextcloud"),
}

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


def _cluster(*settings: str) -> list[dict]:
    return _kinds(
        _render(*settings, show_only="templates/databases/cnpg-cluster.yaml"), "Cluster"
    )


def _all_cnpg() -> list[str]:
    return [f"databases.{key}.engine=cnpg" for key in DATABASES]


def test_default_release_renders_no_cluster_at_all():
    """Phase 2 must be inert: engine defaults to statefulset everywhere."""
    assert _kinds(_render(), "Cluster") == []


def test_default_release_still_renders_the_five_statefulsets():
    names = {d["metadata"]["name"] for d in _kinds(_render(), "StatefulSet")}
    for component, _, _ in DATABASES.values():
        assert f"{FULLNAME}-{component}" in names


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_engine_cnpg_swaps_statefulset_for_cluster(key):
    component, _, _ = DATABASES[key]
    documents = _render(f"databases.{key}.engine=cnpg")
    statefulsets = {d["metadata"]["name"] for d in _kinds(documents, "StatefulSet")}
    clusters = {d["metadata"]["name"] for d in _kinds(documents, "Cluster")}
    assert f"{FULLNAME}-{component}" not in statefulsets
    assert f"{FULLNAME}-{component}" in clusters


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_engine_migrating_renders_both(key):
    """Phase 4 imports from the live Service, so both must exist at once."""
    component, _, _ = DATABASES[key]
    documents = _render(f"databases.{key}.engine=migrating")
    assert f"{FULLNAME}-{component}" in {
        d["metadata"]["name"] for d in _kinds(documents, "StatefulSet")
    }
    assert f"{FULLNAME}-{component}" in {
        d["metadata"]["name"] for d in _kinds(documents, "Cluster")
    }


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_cluster_carries_database_owner_and_inherited_labels(key):
    component, database, owner = DATABASES[key]
    cluster = [
        c
        for c in _cluster(f"databases.{key}.engine=cnpg")
        if c["metadata"]["name"] == f"{FULLNAME}-{component}"
    ][0]
    initdb = cluster["spec"]["bootstrap"]["initdb"]
    assert initdb["database"] == database
    assert initdb["owner"] == owner
    labels = cluster["spec"]["inheritedMetadata"]["labels"]
    assert labels["app.kubernetes.io/component"] == component
    assert labels["app.kubernetes.io/instance"] == RELEASE


def test_cluster_name_equals_todays_service_name():
    """Phase 4's helper flip is then a pure "-rw" suffix, nothing else."""
    statefulset_services = {d["metadata"]["name"] for d in _kinds(_render(), "Service")}
    for component, _, _ in DATABASES.values():
        assert f"{FULLNAME}-{component}" in statefulset_services
    cluster_names = {c["metadata"]["name"] for c in _cluster(*_all_cnpg())}
    assert cluster_names == {f"{FULLNAME}-{c}" for c, _, _ in DATABASES.values()}


def test_profile_single_is_one_instance_everywhere():
    clusters = _cluster(*_all_cnpg())
    assert {c["spec"]["instances"] for c in clusters} == {1}


def test_profile_ha_is_two_instances_everywhere():
    clusters = _cluster("databases.profile=ha", *_all_cnpg())
    assert {c["spec"]["instances"] for c in clusters} == {2}


def test_per_database_instances_overrides_the_profile():
    clusters = _cluster(
        "databases.profile=ha", "databases.postgres.instances=3", *_all_cnpg()
    )
    by_name = {c["metadata"]["name"]: c["spec"]["instances"] for c in clusters}
    assert by_name[f"{FULLNAME}-postgres"] == 3
    assert by_name[f"{FULLNAME}-auditdb"] == 2


def test_anti_affinity_derives_from_resolved_count_not_profile_name():
    """A generated file says instances: 3 without mentioning profile. It must
    still get hard anti-affinity, or replicas silently stack on one node."""
    cluster = _cluster(
        "databases.postgres.engine=cnpg", "databases.postgres.instances=3"
    )[0]
    affinity = cluster["spec"]["affinity"]
    assert affinity["enablePodAntiAffinity"] is True
    assert affinity["podAntiAffinityType"] == "required"
    assert affinity["topologyKey"] == "kubernetes.io/hostname"


def test_single_instance_uses_preferred_anti_affinity():
    cluster = _cluster("databases.postgres.engine=cnpg")[0]
    assert cluster["spec"]["affinity"]["podAntiAffinityType"] == "preferred"


def test_topology_key_is_overridable_for_zonal_hyperscaler_volumes():
    cluster = _cluster(
        "databases.postgres.engine=cnpg",
        "databases.postgres.instances=3",
        "databases.ha.topologyKey=topology.kubernetes.io/zone",
    )[0]
    assert cluster["spec"]["affinity"]["topologyKey"] == "topology.kubernetes.io/zone"


def test_pgvector_comes_from_the_operand_image_not_an_image_volume():
    """Phase 2 declared pgvector as a CNPG ImageVolume extension. Phase 4
    replaced that with an operand image carrying pgvector, because the only
    official extension images are PostgreSQL 18 while the operand is 16 --
    mounting one would be a major-version mismatch. Still only the vector
    database needs it at all: the app DB creates btree_gist and uuid-ossp."""
    for cluster in _cluster(*_all_cnpg()):
        assert "extensions" not in cluster["spec"]["postgresql"], cluster["metadata"][
            "name"
        ]


def test_operand_image_never_inherits_the_statefulset_tag():
    """CNPG parses imageName's tag as a PostgreSQL version and rejects
    `pgvector/pgvector:pg15` with "invalid version tag" -- verified against the
    live CRD. Phase 2 omitted the field; Phase 4 pins a purpose-built operand.
    Either way it must never be the StatefulSet's own image."""
    values = yaml.safe_load((CHART / "values.yaml").read_text())["databases"]
    legacy = {values[key]["image"] for key in DATABASES}
    for cluster in _cluster(*_all_cnpg()):
        assert cluster["spec"].get("imageName") not in legacy, cluster["metadata"][
            "name"
        ]


def test_storage_and_connection_defaults():
    cluster = _cluster("databases.postgres.engine=cnpg")[0]
    assert cluster["spec"]["storage"]["size"] == "16Gi"
    postgresql = cluster["spec"]["postgresql"]
    assert postgresql is not None, "postgresql must never render as an empty mapping"
    assert postgresql["parameters"]["max_connections"] == "100"
    # Async by default: a sick standby must not stall every write on the primary.
    assert "synchronous" not in postgresql


def test_invalid_engine_is_rejected():
    result = subprocess.run(
        _template_command("databases.postgres.engine=bogus"),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "engine" in result.stderr or "bogus" in result.stderr


def test_operand_image_is_used_when_pinned():
    cluster = _cluster(
        "databases.postgres.engine=cnpg",
        "databases.postgres.cnpgImage=ghcr.io/cloudnative-pg/postgresql:16.10",
    )[0]
    assert cluster["spec"]["imageName"] == "ghcr.io/cloudnative-pg/postgresql:16.10"


# --- credential projection -------------------------------------------------
#
# The chart has three secret modes and the projection needs a different
# mechanism in each. `helm/ci/test-values.yaml` runs the External Secrets mode,
# which is also how dev is configured.

CREDENTIALS = "templates/databases/cnpg-credentials.yaml"
CREATE_MODE = ("externalSecrets.enabled=false", "secrets.create=true")


def _credentials(*settings: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in _render(*settings, show_only=CREDENTIALS)}


def _render_fails(*settings: str) -> str:
    result = subprocess.run(
        _template_command(*settings), capture_output=True, text=True
    )
    assert result.returncode != 0, "expected the render to fail"
    return result.stderr


def test_no_credential_object_when_inert():
    result = subprocess.run(
        _template_command(show_only=CREDENTIALS), capture_output=True, text=True
    )
    # Helm errors when --show-only matches nothing rendered; either way, nothing.
    assert result.returncode != 0 or "kind:" not in result.stdout


@pytest.mark.parametrize("key", sorted(DATABASES))
def test_external_secrets_mode_projects_into_a_basic_auth_shape(key):
    component, _, owner = DATABASES[key]
    external = _credentials(f"databases.{key}.engine=cnpg")[
        f"{FULLNAME}-{component}-app"
    ]
    assert external["kind"] == "ExternalSecret"
    template = external["spec"]["target"]["template"]
    assert template["type"] == "kubernetes.io/basic-auth"
    assert template["data"]["username"] == owner
    # Left for the ESO controller to substitute, not rendered away by Helm.
    assert template["data"]["password"] == "{{ .password }}"


@pytest.mark.parametrize(
    "key,password_key",
    [
        ("postgres", "POSTGRES_PASSWORD"),
        ("vector", "VECTOR_POSTGRES_PASSWORD"),
        ("audit", "AUDIT_POSTGRES_PASSWORD"),
        ("gitea", "GITEA_DB_PASSWORD"),
        ("keycloak", "KC_DB_PASSWORD"),
        ("nextcloud", "NEXTCLOUD_DB_PASSWORD"),
    ],
)
def test_external_secrets_mode_reads_the_same_vault_property_consumers_do(
    key, password_key
):
    component, _, _ = DATABASES[key]
    external = _credentials(f"databases.{key}.engine=cnpg")[
        f"{FULLNAME}-{component}-app"
    ]
    entry = external["spec"]["data"][0]
    assert entry["secretKey"] == "password"
    assert entry["remoteRef"]["property"] == password_key


def test_create_mode_projects_the_operators_own_value():
    secret = _credentials(
        *CREATE_MODE,
        "secrets.values.POSTGRES_PASSWORD=s3cret",
        "databases.postgres.engine=cnpg",
    )[f"{FULLNAME}-postgres-app"]
    assert secret["kind"] == "Secret"
    assert secret["type"] == "kubernetes.io/basic-auth"
    assert secret["stringData"] == {"username": "srw", "password": "s3cret"}


def test_create_mode_refuses_to_invent_a_password():
    """An empty password bootstraps a database nothing can log into, while
    looking configured. Fail at render instead."""
    stderr = _render_fails(*CREATE_MODE, "databases.postgres.engine=cnpg")
    assert "secrets.values.POSTGRES_PASSWORD is unset" in stderr


def test_create_mode_rejects_a_username_that_disagrees_with_the_owner():
    """CNPG creates the owner role. A consumer authenticating as a different
    role would own nothing -- and would find out at the first write."""
    stderr = _render_fails(
        *CREATE_MODE,
        "secrets.values.POSTGRES_PASSWORD=s3cret",
        "secrets.values.POSTGRES_USER=someoneelse",
        "databases.postgres.engine=cnpg",
    )
    assert "would own nothing" in stderr


def test_existing_secret_mode_names_the_secret_the_operator_must_create():
    stderr = _render_fails(
        "externalSecrets.enabled=false",
        "secrets.create=false",
        "secrets.existingSecret=my-secret",
        "databases.postgres.engine=cnpg",
    )
    assert f"{FULLNAME}-postgres-app" in stderr
    assert "kubernetes.io/basic-auth" in stderr


def test_credential_name_matches_what_the_cluster_references():
    cluster = _cluster("databases.postgres.engine=cnpg")[0]
    referenced = cluster["spec"]["bootstrap"]["initdb"]["secret"]["name"]
    assert referenced in _credentials("databases.postgres.engine=cnpg")


def test_every_rendered_cluster_gets_a_credential_object():
    clusters = {c["metadata"]["name"] for c in _cluster(*_all_cnpg())}
    credentials = set(_credentials(*_all_cnpg()))
    assert {f"{name}-app" for name in clusters} == credentials


# --- network policies ------------------------------------------------------
#
# There is no giteadb NetworkPolicy in the chart -- a pre-existing gap, not one
# this phase introduces. Nothing restricts that database today, so CNPG
# replication is not blocked there either; adding a policy would be a new
# restriction and belongs in its own change.
POLICIES = "templates/databases/network-policies.yaml"
# helm/values.yaml: databases.operator.namespace
OPERATOR_NAMESPACE = "cnpg-system"
POLICY_COMPONENTS = {
    "postgres": "postgres",
    "vector": "pgvector",
    "audit": "auditdb",
    "keycloak": "keycloakdb",
    "nextcloud": "nextclouddb",
}


def _policies(*settings: str) -> dict[str, dict]:
    documents = _render(*settings, show_only=POLICIES)
    return {d["metadata"]["name"]: d for d in _kinds(documents, "NetworkPolicy")}


def _sources(policy: dict, kind: str) -> list[dict]:
    return [
        rule[kind]
        for entry in policy["spec"]["ingress"]
        for rule in entry.get("from", [])
        if kind in rule
    ]


@pytest.mark.parametrize("key,component", sorted(POLICY_COMPONENTS.items()))
def test_cnpg_pods_are_allowed_in_by_their_own_label(key, component):
    """The policy must select CNPG pods by `cnpg.io/cluster`, not by the
    chart's component labels. During `migrating` those chart labels are
    deliberately absent -- otherwise the legacy Service captures the pods --
    so a component-label rule denies the IMPORT its read of the source
    database. Verified live on dev 2026-08-22: Calico rejected pg_dump with
    'connection refused' and the import failed in 25 seconds.

    It is also tighter: only pods of that one cluster, rather than anything
    wearing the component label."""
    policy = _policies(f"databases.{key}.engine=migrating")[f"{FULLNAME}-{component}"]
    selectors = _sources(policy, "podSelector")
    assert any(
        s["matchLabels"].get("cnpg.io/cluster") == f"{FULLNAME}-{component}"
        for s in selectors
    ), f"nothing admits the {component} cluster's own pods: {selectors}"


@pytest.mark.parametrize("key,component", sorted(POLICY_COMPONENTS.items()))
def test_replication_between_instances_is_allowed(key, component):
    """Multi-instance CNPG streams WAL between its own pods on 5432, and the
    same cnpg.io/cluster rule carries it -- the from-list otherwise names only
    orchestrator/agent/etc, so replication would be silently blocked."""
    policy = _policies(f"databases.{key}.engine=cnpg", f"databases.{key}.instances=2")[
        f"{FULLNAME}-{component}"
    ]
    selectors = _sources(policy, "podSelector")
    assert any(
        s["matchLabels"].get("cnpg.io/cluster") == f"{FULLNAME}-{component}"
        for s in selectors
    ), "the database must accept connections from its own cluster's pods"


@pytest.mark.parametrize("key,component", sorted(POLICY_COMPONENTS.items()))
def test_operator_namespace_is_allowed_to_reach_instances(key, component):
    """If the operator cannot poll instance health it cannot fail over --
    which is the entire feature."""
    policy = _policies(f"databases.{key}.engine=cnpg")[f"{FULLNAME}-{component}"]
    namespaces = _sources(policy, "namespaceSelector")
    assert namespaces, "no namespaceSelector allows the CNPG operator in"
    assert any(
        n["matchLabels"].get("kubernetes.io/metadata.name") == "cnpg-system"
        for n in namespaces
    )


def test_operator_namespace_is_configurable():
    policy = _policies(
        "databases.postgres.engine=cnpg", "databases.operator.namespace=pg-ops"
    )[f"{FULLNAME}-postgres"]
    assert any(
        n["matchLabels"].get("kubernetes.io/metadata.name") == "pg-ops"
        for n in _sources(policy, "namespaceSelector")
    )


@pytest.mark.parametrize("key,component", sorted(POLICY_COMPONENTS.items()))
def test_statefulset_policy_is_unchanged_when_inert(key, component):
    policy = _policies()[f"{FULLNAME}-{component}"]
    components = [
        s["matchLabels"].get("app.kubernetes.io/component")
        for s in _sources(policy, "podSelector")
    ]
    assert component not in components
    # Scoped to the operator namespace rather than asserting no namespaceSelector
    # at all. These renders carry helm/ci/test-values.yaml, which enables the
    # stateless pool's KEDA autoscaling, and that feature owns a namespaceSelector
    # of its own on the postgres policy (the scaler queries 5432 from the KEDA
    # namespace) at every engine. "Inert" is a claim about THIS migration -- no
    # CNPG operator ingress until an engine is flipped -- not a claim that no
    # other feature may ever allow a namespace in.
    assert OPERATOR_NAMESPACE not in [
        n["matchLabels"].get("kubernetes.io/metadata.name")
        for n in _sources(policy, "namespaceSelector")
    ]


# --- disruption budgets ----------------------------------------------------

CNPG_PDB = "templates/databases/cnpg-pdb.yaml"


def test_no_database_pdb_at_a_single_instance():
    result = subprocess.run(
        _template_command("databases.postgres.engine=cnpg", show_only=CNPG_PDB),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 or "kind: PodDisruptionBudget" not in result.stdout


def test_database_pdb_appears_once_replicated():
    documents = _render(
        "databases.postgres.engine=cnpg",
        "databases.postgres.instances=2",
        show_only=CNPG_PDB,
    )
    pdb = _kinds(documents, "PodDisruptionBudget")[0]
    assert pdb["spec"]["minAvailable"] == 1
    assert (
        pdb["spec"]["selector"]["matchLabels"]["app.kubernetes.io/component"]
        == "postgres"
    )


def test_every_replicated_database_gets_a_pdb():
    documents = _render("databases.profile=ha", *_all_cnpg(), show_only=CNPG_PDB)
    names = {d["metadata"]["name"] for d in _kinds(documents, "PodDisruptionBudget")}
    assert names == {
        f"{FULLNAME}-{component}-pdb" for component, _, _ in DATABASES.values()
    }


def test_statefulset_engine_never_gets_a_database_pdb():
    """A CNPG PDB selects by component label, which the StatefulSet pod also
    carries -- so rendering one for an unmigrated database would freeze it."""
    result = subprocess.run(
        _template_command("databases.profile=ha", show_only=CNPG_PDB),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0 or "kind: PodDisruptionBudget" not in result.stdout


# --- external operator contract -------------------------------------------


def test_application_chart_does_not_own_the_cnpg_operator():
    chart = yaml.safe_load((CHART / "Chart.yaml").read_text())
    assert not [
        dependency
        for dependency in chart["dependencies"]
        if dependency["name"] == "cloudnative-pg"
    ]


def test_chart_lock_does_not_vendor_the_cnpg_operator():
    lock = yaml.safe_load((CHART / "Chart.lock").read_text())
    assert "cloudnative-pg" not in {
        dependency["name"] for dependency in lock["dependencies"]
    }


def test_operator_values_are_namespace_only():
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["databases"]["operator"] == {"namespace": "cnpg-system"}


def test_removed_operator_install_true_is_rejected_fail_safe():
    result = subprocess.run(
        _template_command("databases.operator.install=true"),
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0
    assert "operator" in result.stderr
    assert "install" in result.stderr


def test_stored_operator_install_false_remains_upgrade_compatible():
    _render("databases.operator.install=false")


class TestInitdbLocale:
    """The locale the target cluster is initialised under.

    CNPG defaults `--lc-collate`/`--lc-ctype` to `C` because the operand image
    sets no LANG. Every legacy SRW database is en_US.utf8, because the
    StatefulSet runs docker.io/library/postgres, which does. Migrating without
    pinning this re-initialises under `C`, which silently changes text sort
    order and upper()/lower() on non-ASCII -- and, because non-ASCII stops
    being alphabetic, re-tokenises full text hard enough to breach the 1MB
    tsvector cap. That is a restore failure, not a cosmetic difference:
    `string is too long for tsvector (1237068 bytes, max 1048575 bytes)`.
    Verified on one PG16.15 binary: identical rows, `C` fails, en_US.utf8
    builds the index.
    """

    # The locale docker.io/library/postgres initdb's under, hence the locale of
    # every database this chart migrates. Hardcoded so that weakening the
    # default fails here rather than during a restore.
    LEGACY_LOCALE = "en_US.utf8"

    def test_every_cluster_pins_all_three_initdb_locale_fields(self):
        clusters = _cluster(*_all_cnpg())
        assert len(clusters) == len(DATABASES)
        for cluster in clusters:
            initdb = cluster["spec"]["bootstrap"]["initdb"]
            name = cluster["metadata"]["name"]
            assert initdb["encoding"] == "UTF8", name
            assert initdb["localeCollate"] == self.LEGACY_LOCALE, name
            assert initdb["localeCType"] == self.LEGACY_LOCALE, name

    def test_locale_is_pinned_while_migrating(self):
        """The engine that actually performs the import.

        `bootstrap` is read only at creation, so a Cluster created on the
        `migrating` engine keeps whatever locale it was born with forever.
        """
        for key, (component, _, _) in DATABASES.items():
            clusters = _cluster(f"databases.{key}.engine=migrating")
            initdb = clusters[0]["spec"]["bootstrap"]["initdb"]
            assert "import" in initdb, component
            assert initdb["localeCollate"] == self.LEGACY_LOCALE, component
            assert initdb["localeCType"] == self.LEGACY_LOCALE, component

    def test_default_is_not_the_cnpg_default(self):
        """Negative control: the bug was inheriting CNPG's own default."""
        initdb = _cluster("databases.postgres.engine=cnpg")[0]["spec"]["bootstrap"][
            "initdb"
        ]
        assert initdb["localeCollate"] != "C"
        assert initdb["localeCType"] != "C"

    def test_locale_is_overridable(self):
        initdb = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.locale.collate=C.UTF-8",
            "databases.locale.ctype=C.UTF-8",
            "databases.locale.encoding=LATIN1",
        )[0]["spec"]["bootstrap"]["initdb"]
        assert initdb["localeCollate"] == "C.UTF-8"
        assert initdb["localeCType"] == "C.UTF-8"
        assert initdb["encoding"] == "LATIN1"

    def test_blank_locale_omits_the_fields_rather_than_passing_empty_strings(self):
        """`--lc-collate=""` is not the same as not passing it: initdb rejects it."""
        initdb = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.locale.collate=",
            "databases.locale.ctype=",
            "databases.locale.encoding=",
        )[0]["spec"]["bootstrap"]["initdb"]
        assert "localeCollate" not in initdb
        assert "localeCType" not in initdb
        assert "encoding" not in initdb


class TestOperatorStatusPort:
    """The operator polls instance health on 8000, not 5432.

    A policy that admits `cnpg-system` but only opens 5432 leaves the Cluster
    reporting `Ready=False` -- "unable to receive the status from all the ready
    instances" -- while the database serves normally. CNPG makes failover and
    switchover decisions from that status, so beyond one instance it is not
    cosmetic. Observed live on srw-postgres and srw-keycloakdb, 2026-08-23,
    immediately after cutover.

    It cannot be caught by a rehearsal: while `migrating`, CNPG pods do not
    carry the chart's component labels, so the policy does not select them.
    """

    OPERATOR_NS = "cnpg-system"

    @staticmethod
    def _operator_rules(policy: dict) -> list[dict]:
        rules = []
        for rule in policy["spec"].get("ingress", []):
            for source in rule.get("from", []):
                labels = source.get("namespaceSelector", {}).get("matchLabels", {})
                if labels.get("kubernetes.io/metadata.name") == "cnpg-system":
                    rules.append(rule)
                    break
        return rules

    def test_operator_can_reach_the_status_port(self):
        for key, (component, _, _) in DATABASES.items():
            if component == "giteadb":
                continue  # deliberately has no policy
            policies = _policies(f"databases.{key}.engine=cnpg")
            policy = policies.get(f"{FULLNAME}-{component}")
            assert policy is not None, f"no policy for {component}"
            rules = self._operator_rules(policy)
            assert rules, f"{component}: operator namespace not admitted at all"
            ports = {p.get("port") for rule in rules for p in rule.get("ports", [])}
            assert 8000 in ports, f"{component}: status port missing, got {ports}"
            assert 5432 in ports, f"{component}: postgres port missing, got {ports}"

    def test_status_port_is_not_open_to_application_workloads(self):
        """8000 is the instance manager's API. Only the operator gets it."""
        policy = _policies("databases.postgres.engine=cnpg")[f"{FULLNAME}-postgres"]
        for rule in policy["spec"]["ingress"]:
            ports = {p.get("port") for p in rule.get("ports", [])}
            if 8000 not in ports:
                continue
            for source in rule.get("from", []):
                labels = source.get("namespaceSelector", {}).get("matchLabels", {})
                assert labels.get("kubernetes.io/metadata.name") == self.OPERATOR_NS, (
                    f"port 8000 exposed to a non-operator source: {source}"
                )

    def test_no_operator_rule_while_on_the_statefulset_engine(self):
        """Negative control: nothing CNPG-shaped leaks into a pre-migration render."""
        policy = _policies()[f"{FULLNAME}-postgres"]
        assert not self._operator_rules(policy)


class TestCredentialsPreCreated:
    """`existingSecret` + `engine: cnpg` was uninstallable before this flag.

    CloudNativePG reads each database's credentials from a per-cluster
    basic-auth Secret. With `secrets.existingSecret` the chart never sees the
    password, so it cannot build that Secret -- and it refuses to render rather
    than install a Cluster whose owner credentials do not exist. Correct, but it
    also meant the entire self-hosted HA path could not be installed at all,
    since that is exactly the mode the config generator emits.
    """

    EXISTING = (
        "externalSecrets.enabled=false",
        "secrets.create=false",
        "secrets.existingSecret=srw-secrets",
        "databases.postgres.engine=cnpg",
    )

    def test_existing_secret_mode_still_refuses_without_the_flag(self):
        message = _render_fails(*self.EXISTING)
        assert "cannot see the database password" in message
        # The error must name the way out, or it is just a wall.
        assert "credentialsPreCreated" in message

    def test_the_flag_lets_the_cluster_render(self):
        clusters = _cluster(*self.EXISTING, "databases.credentialsPreCreated=true")
        assert [c["metadata"]["name"] for c in clusters] == [f"{FULLNAME}-postgres"]

    def test_the_flag_renders_no_credential_resource_of_its_own(self):
        """It is an assertion that the Secret exists, not a way to make one."""
        documents = _render(*self.EXISTING, "databases.credentialsPreCreated=true")
        for kind in ("Secret", "ExternalSecret"):
            names = [
                d["metadata"]["name"]
                for d in _kinds(documents, kind)
                if d["metadata"]["name"].endswith("-app")
            ]
            assert names == [], f"expected no {kind} ending in -app, got {names}"

    def test_the_flag_does_not_disturb_the_modes_that_can_build_it(self):
        """secrets.create and externalSecrets can see the value; both still do."""
        created = _render(
            "externalSecrets.enabled=false",
            "secrets.create=true",
            "secrets.values.POSTGRES_PASSWORD=pw",
            "databases.postgres.engine=cnpg",
            "databases.credentialsPreCreated=true",
            show_only=CREDENTIALS,
        )
        secrets = _kinds(created, "Secret")
        assert [s["metadata"]["name"] for s in secrets] == [f"{FULLNAME}-postgres-app"]
        assert secrets[0]["type"] == "kubernetes.io/basic-auth"

    def test_default_is_false_so_nothing_changes_for_existing_installs(self):
        values = yaml.safe_load((CHART / "values.yaml").read_text())
        assert values["databases"]["credentialsPreCreated"] is False


class TestSmartShutdownTimeout:
    """What a node drain or rolling upgrade actually costs.

    Smart shutdown waits for client SESSIONS to end, not transactions. Every SRW
    component reaches Postgres through a pool, and a pool holds its sessions
    open by design, so the wait cannot succeed -- it runs out. CNPG will not
    promote a replica until the old primary is down, so this timer is paid in
    full on every planned maintenance event.

    Measured on dev 2026-08-23: `kubectl delete pod` on a primary cost 195s of
    write unavailability against CNPG's 180s default. The same cluster,
    hard-killed, promoted in 9s.
    """

    def test_every_cluster_pins_it(self):
        clusters = _cluster(*_all_cnpg())
        assert len(clusters) == len(DATABASES)
        for cluster in clusters:
            assert cluster["spec"]["smartShutdownTimeout"] == 30, cluster["metadata"][
                "name"
            ]

    def test_the_default_is_not_cnpgs(self):
        """Negative control. Inheriting 180 is the bug, not the baseline."""
        values = yaml.safe_load((CHART / "values.yaml").read_text())
        assert values["databases"]["smartShutdownTimeout"] != 180

    def test_it_is_overridable(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg", "databases.smartShutdownTimeout=120"
        )[0]
        assert cluster["spec"]["smartShutdownTimeout"] == 120


def _parameters(*settings: str) -> dict[str, dict[str, str]]:
    """component name -> its rendered postgresql.parameters."""
    return {
        c["metadata"]["name"]: c["spec"]["postgresql"]["parameters"]
        for c in _cluster(*settings)
    }


class TestDerivedMemoryGucs:
    """shared_buffers and effective_cache_size scale with limits.memory.

    CloudNativePG ships a FIXED shared_buffers of 128MB and never scales it
    with the memory limit, so raising limits.memory on its own buys the
    database nothing. That is not hypothetical: measured on dev 2026-08-24,
    all five clusters ran 128MB of buffers against limits between 512Mi and
    12Gi, and the two LARGEST databases had the worst cache hit ratios for it
    (pgvector 19 GB / 12Gi / 84.2%, auditdb 26 GB / 4Gi / 90.6%, against 99%+
    on the two small ones).

    These tests exist so that combination is unreachable.
    """

    def test_shared_buffers_is_a_quarter_of_the_limit(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=4Gi",
        )[0]
        assert cluster["spec"]["postgresql"]["parameters"]["shared_buffers"] == "1024MB"

    def test_effective_cache_size_is_three_quarters(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=4Gi",
        )[0]
        parameters = cluster["spec"]["postgresql"]["parameters"]
        assert parameters["effective_cache_size"] == "3072MB"

    def test_it_tracks_the_limit(self):
        """The regression this whole mechanism exists for.

        Doubling the limit must double the buffers. A chart where these two
        move independently is the state we shipped and had to diagnose.
        """
        small = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=2Gi",
        )[0]["spec"]["postgresql"]["parameters"]["shared_buffers"]
        large = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=4Gi",
        )[0]["spec"]["postgresql"]["parameters"]["shared_buffers"]
        assert (small, large) == ("512MB", "1024MB")

    def test_no_cluster_is_left_on_the_operand_default(self):
        """Negative control: 128MB is CNPG's default, i.e. nobody chose it."""
        for name, parameters in _parameters(*_all_cnpg()).items():
            assert "shared_buffers" in parameters, name
            if not name.endswith(("giteadb", "keycloakdb")):
                # The two small ones legitimately land on 128MB from a 512Mi
                # limit; every other cluster must have moved off it.
                assert parameters["shared_buffers"] != "128MB", name

    def test_mebibyte_limits_parse(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=512Mi",
        )[0]
        assert cluster["spec"]["postgresql"]["parameters"]["shared_buffers"] == "128MB"

    def test_an_explicit_parameter_wins(self):
        """Deriving is a floor for people who never think about it."""
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=4Gi",
            "databases.postgres.parameters.shared_buffers=333MB",
        )[0]
        assert cluster["spec"]["postgresql"]["parameters"]["shared_buffers"] == "333MB"

    def test_zero_percent_omits_the_guc(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.tuning.sharedBuffersPercent=0",
        )[0]
        parameters = cluster["spec"]["postgresql"]["parameters"]
        assert "shared_buffers" not in parameters
        # The other derived GUC is unaffected.
        assert "effective_cache_size" in parameters

    def test_max_connections_still_renders(self):
        """The parameters map was rebuilt; its original occupant must survive."""
        for name, parameters in _parameters(*_all_cnpg()).items():
            assert parameters["max_connections"] == "100", name


class TestMemoryFitsGuard:
    """A memory budget that cannot fit its limit fails the render.

    The failure being prevented is an OOM kill, not a slow query, and the
    arithmetic is genuinely non-obvious: autovacuum_work_mem DEFAULTS to
    maintenance_work_mem and is allocated once per autovacuum_max_workers, so
    a 2GB maintenance_work_mem silently authorises 6GB of autovacuum on top of
    shared_buffers -- a number that appears nowhere in the config.

    This guard caught exactly that in the shipping chart: pgvector's
    maintenance_work_mem of 2GB against an 8Gi limit needed 10240Mi.
    """

    def test_an_impossible_budget_fails(self):
        stderr = _render_fails(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=1Gi",
            "databases.postgres.parameters.maintenance_work_mem=4GB",
        )
        assert "memory limit" in stderr

    def test_the_failure_names_the_database(self):
        """A guard that does not say which of five clusters is wrong is noise."""
        stderr = _render_fails(
            "databases.audit.engine=cnpg",
            "databases.audit.cnpgResources.limits.memory=1Gi",
            "databases.audit.parameters.maintenance_work_mem=4GB",
        )
        assert "auditdb" in stderr

    def test_autovacuum_multiplication_is_counted(self):
        """The whole point: this fits if you forget it runs once per worker."""
        fits = (
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=8Gi",
            "databases.postgres.parameters.maintenance_work_mem=2GB",
        )
        stderr = _render_fails(*fits)
        assert "x 3 workers" in stderr
        # Pinning autovacuum_work_mem is what makes the same budget legal.
        cluster = _cluster(
            *fits, "databases.postgres.parameters.autovacuum_work_mem=256MB"
        )[0]
        assert (
            cluster["spec"]["postgresql"]["parameters"]["maintenance_work_mem"] == "2GB"
        )

    def test_a_unitless_value_is_refused_not_guessed(self):
        """8kB blocks for shared_buffers, kB for maintenance_work_mem.

        There is no way to tell which was meant from the digits alone, so
        guessing would be wrong roughly half the time and silently so.
        """
        stderr = _render_fails(
            "databases.postgres.engine=cnpg",
            "databases.postgres.parameters.maintenance_work_mem=200000",
        )
        assert "no unit" in stderr

    def test_the_shipped_defaults_pass(self):
        """Guard against a guard that only ever fires on hypotheticals."""
        clusters = _cluster(*_all_cnpg())
        assert len(clusters) == len(DATABASES)


class TestMemoryRequestParity:
    """requests.memory == limits.memory on every CNPG cluster.

    A memory limit is a hard wall -- cross it and the container is OOM-killed,
    and PostgreSQL has no graceful degradation past that point. Meanwhile the
    kubelet ranks memory-pressure evictions by how far a pod has run ABOVE ITS
    REQUEST. Setting the request to the limit makes the pod unable to exceed
    its own request, which removes it from that ranking. Measured on dev
    2026-08-24, node memory LIMITS were overcommitted 110-163% while requests
    sat at 31-55%, which is precisely the gap this closes.
    """

    def test_every_cluster_has_memory_parity(self):
        clusters = _cluster(*_all_cnpg())
        assert len(clusters) == len(DATABASES)
        for cluster in clusters:
            resources = cluster["spec"]["resources"]
            assert resources["requests"]["memory"] == resources["limits"]["memory"], (
                cluster["metadata"]["name"]
            )

    def test_cpu_is_deliberately_not_pinned(self):
        """Negative control.

        CPU parity would buy the `Guaranteed` QoS label, whose only remaining
        benefit is the kernel OOM killer's victim scoring. The cost is a
        reserved core per instance and a hard throttle on vacuum, index builds
        and gzip base backups exactly when they need to burst. A CPU limit
        throttles; it does not kill.
        """
        for cluster in _cluster(*_all_cnpg()):
            resources = cluster["spec"]["resources"]
            assert resources["requests"]["cpu"] != resources["limits"]["cpu"], cluster[
                "metadata"
            ]["name"]

    def test_parity_holds_when_the_limit_is_overridden(self):
        cluster = _cluster(
            "databases.postgres.engine=cnpg",
            "databases.postgres.cnpgResources.limits.memory=4Gi",
            "databases.postgres.cnpgResources.requests.memory=4Gi",
        )[0]
        resources = cluster["spec"]["resources"]
        assert resources["requests"]["memory"] == resources["limits"]["memory"] == "4Gi"
