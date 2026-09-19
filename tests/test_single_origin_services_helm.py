"""Public subpaths must not leak into cluster-internal authentication calls."""

import json
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
ORIGIN = "https://192.0.2.10:30443"

SINGLE_ORIGIN_PRESETS = [
    ROOT / "deployment/values-local-single-origin.yaml",
    ROOT / "deployment/values-single-origin-server.yaml.example",
    ROOT / "helm/ci/single-origin-local-values.yaml",
    ROOT / "helm/ci/single-origin-server-values.yaml",
    ROOT / "helm/ci/installer-single-origin-local-values.yaml",
    ROOT / "helm/ci/installer-single-origin-server-values.yaml",
]


@pytest.fixture(scope="module")
def manifests():
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(ROOT / "helm"),
            "--namespace",
            "srw",
            "--set",
            "license.acceptTerms=true",
            "--set",
            "fullnameOverride=srw",
            "--set",
            "secrets.create=true",
            "--set",
            "exposure.mode=single-origin",
            "--set",
            "exposure.singleOrigin.address=192.0.2.10",
            "--set",
            "opencloud.enabled=false",
            "--set",
            "nextcloud.enabled=true",
            "--set",
            "nextcloud.objectStore.enabled=false",
            "--set",
            "databases.neo4j.boltTls.enabled=false",
            "--set",
            "nextcloud.protectedEffect.enabled=true",
            "--set",
            "nextcloud.protectedEffect.hmacVaultPath=test/protected-effect",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return {
        (d["kind"], d["metadata"]["name"]): d
        for d in yaml.safe_load_all(result.stdout)
        if d
    }


def container(manifests, deployment, name):
    workload = (
        manifests.get(("Deployment", deployment))
        or manifests["StatefulSet", deployment]
    )
    pod = workload["spec"]["template"]["spec"]
    return next(c for c in pod["containers"] if c["name"] == name)


def env(c):
    return {v["name"]: v.get("value") for v in c["env"]}


def render_preset(path):
    values = [str(path)]
    if path.name == "values-local-single-origin.yaml":
        values.insert(0, str(ROOT / "deployment/values-local.yaml.example"))
    args = ["helm", "template", "srw", str(ROOT / "helm"), "--namespace", "srw"]
    for value_file in values:
        args.extend(["--values", value_file])
    result = subprocess.run(args, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    return {
        (document["kind"], document["metadata"]["name"]): document
        for document in yaml.safe_load_all(result.stdout)
        if document
    }


@pytest.mark.parametrize("preset", SINGLE_ORIGIN_PRESETS, ids=lambda path: path.name)
def test_single_origin_presets_stage_installation_attestation_without_enabling_protected_cloud(
    preset,
):
    values = yaml.safe_load(preset.read_text())
    assert values["nextcloud"]["protectedEffect"]["enabled"] is True
    assert str(values.get("agent", {}).get("protectedCloudModeEnabled", "false")).lower() != "true"


@pytest.mark.parametrize(
    "preset,expected_secret,chart_managed",
    [
        (ROOT / "helm/ci/single-origin-local-values.yaml", "srw-protected-effect", True),
        (
            ROOT / "deployment/values-single-origin-server.yaml.example",
            "srw-protected-effect",
            False,
        ),
    ],
    ids=["chart-managed-secret", "operator-owned-secret"],
)
def test_single_origin_presets_render_stable_nextcloud_installation_attestation(
    preset, expected_secret, chart_managed
):
    rendered = render_preset(preset)
    nextcloud = rendered["Deployment", "srw-nextcloud"]
    assert {container["name"] for container in nextcloud["spec"]["template"]["spec"]["containers"]} == {
        "nextcloud",
        "nextcloud-protected-effect-fpm",
        "nextcloud-protected-effect-nginx",
    }
    assert ("Service", "srw-protected-effect") in rendered
    assert ("NetworkPolicy", "srw-protected-effect") in rendered

    config = rendered["ConfigMap", "srw-config"]["data"]
    assert config["NEXTCLOUD_PROTECTED_EFFECT_URL"] == "http://srw-protected-effect"
    assert len(config["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]) == 64
    assert config["PROTECTED_CLOUD_MODE_ENABLED"] == "false"

    orchestrator = container(rendered, "srw-orchestrator", "orchestrator")
    effect_env = {
        item["name"]: item for item in orchestrator["env"]
        if item["name"].startswith("NEXTCLOUD_PROTECTED_EFFECT_")
    }
    assert set(effect_env) == {
        "NEXTCLOUD_PROTECTED_EFFECT_URL",
        "NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256",
        "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
    }
    assert effect_env["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": expected_secret,
        "key": "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
        "optional": False,
    }

    secret = rendered.get(("Secret", expected_secret))
    if chart_managed:
        assert secret["immutable"] is True
        assert "IDE_CREDENTIAL_KEY" in rendered["Secret", "srw"]["stringData"]
        assert "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY" in secret["stringData"]
    else:
        assert secret is None


def test_identity_context_backchannels_and_health(manifests):
    assert manifests["Deployment", "srw-keycloak"]["spec"]["strategy"] == {"type": "Recreate", "rollingUpdate": None}
    kc = container(manifests, "srw-keycloak", "keycloak")
    values = env(kc)
    assert values["KC_HOSTNAME"] == ORIGIN + "/identity"
    assert values["KC_HTTP_RELATIVE_PATH"] == "/identity"
    assert values["KC_HTTP_MANAGEMENT_RELATIVE_PATH"] == "/"
    assert values["KC_HOSTNAME_BACKCHANNEL_DYNAMIC"] == "true"
    for probe in ["livenessProbe", "readinessProbe"]:
        assert kc[probe]["httpGet"]["path"].startswith("/health/")
        assert kc[probe]["httpGet"]["port"] == 9000
    hook = kc["lifecycle"]["postStart"]["exec"]["command"][-1]
    assert "--server http://localhost:8080/identity" in hook
    assert f"{ORIGIN}/git/user/oauth2/Keycloak/callback" in hook
    assert f"{ORIGIN}/cloud/apps/user_oidc/code" in hook
    assert f'webOrigins=["{ORIGIN}/git"]' not in hook
    assert f'webOrigins=["{ORIGIN}/cloud"]' not in hook


def test_realm_clients_use_origin_without_path_and_prefixed_callbacks(manifests):
    realm = json.loads(
        manifests["ConfigMap", "srw-keycloak-realm"]["data"]["srw-realm.json"]
    )
    clients = {c["clientId"]: c for c in realm["clients"]}
    for name in ["cockpit", "cockpit-bff", "gitea", "nextcloud"]:
        assert clients[name]["webOrigins"] == [ORIGIN]
    assert clients["cockpit-bff"]["redirectUris"] == [ORIGIN + "/auth/callback"]
    assert clients["gitea"]["redirectUris"] == [
        ORIGIN + "/git/user/oauth2/Keycloak/callback"
    ]
    assert clients["nextcloud"]["redirectUris"] == [
        ORIGIN + "/cloud/apps/user_oidc/code"
    ]


def test_gitea_public_base_and_internal_oidc_reconciliation(manifests):
    git = container(manifests, "srw-gitea", "gitea")
    assert env(git)["GITEA__server__ROOT_URL"] == ORIGIN + "/git/"
    assert "lifecycle" not in git
    # The image entrypoint prepares app.ini before these arguments run. OIDC
    # discovery and auth reconciliation must precede the long-lived web server.
    assert "command" not in git
    bootstrap = git["args"][-1]
    assert 'KI="http://srw-keycloak:8080/identity"' in bootstrap
    assert f'KP="{ORIGIN}/identity"' in bootstrap
    assert '"issuer"' in bootstrap
    assert bootstrap.index("gitea migrate") < bootstrap.index("gitea admin auth")
    assert bootstrap.index("gitea admin auth update-oauth") < bootstrap.index("exec gitea web")
    assert "--use-custom-urls" not in bootstrap  # ignored by the OpenID provider
    assert "|| true" not in bootstrap
    assert git["startupProbe"]["failureThreshold"] * git["startupProbe"]["periodSeconds"] >= 300


def test_cloud_overwrites_and_existing_pvc_reconciliation(manifests):
    cloud = container(manifests, "srw-nextcloud", "nextcloud")
    values = env(cloud)
    assert values["NEXTCLOUD_TRUSTED_DOMAINS"] == "srw-nextcloud 192.0.2.10:30443"
    assert "NEXTCLOUD_TRUSTED_PROXIES" not in values
    assert not values.get("TRUSTED_PROXIES")
    for name in ["nextcloud", "nextcloud-protected-effect-fpm"]:
        values = env(container(manifests, "srw-nextcloud", name))
        assert values["OVERWRITEHOST"] == "192.0.2.10:30443"
        assert values["OVERWRITEPROTOCOL"] == "https"
        assert values["OVERWRITEWEBROOT"] == "/cloud"
        assert values["OVERWRITECLIURL"] == ORIGIN + "/cloud"
    assert (
        env(cloud)["NEXTCLOUD_OIDC_DISCOVERY_URI"]
        == "http://srw-keycloak:8080/identity/realms/srw/.well-known/openid-configuration"
    )
    hook = manifests["ConfigMap", "srw-nextcloud-hooks"]["data"]["setup-nextcloud.sh"]
    assert "config:system:set trusted_domains --type=json" in hook
    assert "config:system:delete trusted_proxies" in hook
    assert "if occ user_oidc:providers" not in hook
    assert '--discoveryuri="$DISCOVERY_URI"' in hook
    assert "httpclient.allowselfsigned" not in hook


def test_bootstrap_users_have_valid_email_without_a_domain(manifests):
    realm = json.loads(
        manifests["ConfigMap", "srw-keycloak-realm"]["data"]["srw-realm.json"]
    )
    admin = next(user for user in realm["users"] if user["username"] == "test")
    assert admin["email"] == "admin@srw.invalid"


def test_legacy_keycloak_keeps_rolling_update_strategy():
    rendered = subprocess.run(
        ["helm", "template", "srw", str(ROOT / "helm"), "-f", str(ROOT / "helm/ci/test-values.yaml")],
        check=True, capture_output=True, text=True,
    )
    deployments = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc and doc["kind"] == "Deployment"]
    keycloak = next(doc for doc in deployments if doc["metadata"]["name"].endswith("-keycloak"))
    assert keycloak["spec"].get("strategy", {}).get("type", "RollingUpdate") == "RollingUpdate"
