"""Deployment contract for Nextcloud's bounded protected-effect lane."""

from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
EFFECT_FILES = CHART / "files" / "nextcloud-protected-effect"


def _render(*extra: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    return subprocess.run(
        [
            "helm",
            "template",
            "protected-effect-proof",
            str(CHART),
            "-f",
            str(CHART / "ci" / "test-values.yaml"),
            *extra,
        ],
        check=check,
        capture_output=True,
        text=True,
    )


def _enabled_args() -> tuple[str, ...]:
    return (
        "--set",
        "opencloud.enabled=false",
        "--set",
        "nextcloud.enabled=true",
        "--set",
        "nextcloud.replicas=1",
        "--set",
        "nextcloud.protectedEffect.enabled=true",
        "--set",
        "nextcloud.protectedEffect.hmacVaultPath=test/protected-effect",
    )


def _documents(*extra: str) -> list[dict]:
    return [
        document
        for document in yaml.safe_load_all(_render(*extra).stdout)
        if isinstance(document, dict)
    ]


def test_protected_effect_lane_is_dark_by_default() -> None:
    documents = _documents()
    rendered = yaml.safe_dump_all(documents)

    assert "nextcloud-protected-effect-fpm" not in rendered
    assert "NEXTCLOUD_PROTECTED_EFFECT_URL" not in rendered
    assert not any(
        document.get("kind") == "Service"
        and document.get("metadata", {}).get("name", "").endswith("-protected-effect")
        for document in documents
    )


def test_enabled_lane_binds_one_config_to_server_client_and_bundle() -> None:
    documents = _documents(*_enabled_args())
    hooks = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and document.get("metadata", {}).get("name", "").endswith("-nextcloud-hooks")
    )
    app_config = next(
        document
        for document in documents
        if document.get("kind") == "ConfigMap"
        and "NEXTCLOUD_PROTECTED_EFFECT_URL" in (document.get("data") or {})
    )
    nextcloud = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and any(
            container.get("name") == "nextcloud"
            for container in document["spec"]["template"]["spec"]["containers"]
        )
    )
    orchestrator_deployment = next(
        document
        for document in documents
        if document.get("kind") == "Deployment"
        and any(
            container.get("name") == "orchestrator"
            for container in document["spec"]["template"]["spec"]["containers"]
        )
    )

    config_json = hooks["data"]["protected-effect-config.json"].strip()
    config = json.loads(config_json)
    digest = hashlib.sha256(config_json.encode("utf-8")).hexdigest()
    assert app_config["data"]["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"] == digest
    env_example = (ROOT / ".env.example").read_text()
    assert f"# NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256={digest}" in env_example
    assert set(config) == {
        "version",
        "queue_bound_seconds",
        "handler_bound_seconds",
        "clock_skew_bound_seconds",
        "safety_margin_seconds",
        "capability_max_age_seconds",
        "max_children",
        "max_body_bytes",
        "common_sha256",
        "prepend_sha256",
        "capability_sha256",
        "fpm_launcher_sha256",
        "nginx_sha256",
    }
    assert config["version"] == 1
    assert config["queue_bound_seconds"] == 30
    assert config["handler_bound_seconds"] == 10
    assert config["clock_skew_bound_seconds"] == 2
    assert config["safety_margin_seconds"] == 5
    assert config["capability_max_age_seconds"] == 5
    assert config["max_children"] == 2
    assert config["max_body_bytes"] == 65536
    for config_key, filename in {
        "common_sha256": "common.php",
        "prepend_sha256": "prepend.php",
        "capability_sha256": "capability.php",
        "fpm_launcher_sha256": "start-fpm.sh",
        "nginx_sha256": "nginx.conf",
    }.items():
        assert (
            config[config_key]
            == hashlib.sha256((EFFECT_FILES / filename).read_bytes()).hexdigest()
        )
        assert hooks["data"][filename].rstrip("\n") == (
            EFFECT_FILES / filename
        ).read_text().rstrip("\n")

    containers = {
        container["name"]: container
        for container in nextcloud["spec"]["template"]["spec"]["containers"]
    }
    assert nextcloud["spec"]["strategy"] == {"type": "Recreate"}
    assert set(containers) >= {
        "nextcloud",
        "nextcloud-protected-effect-fpm",
        "nextcloud-protected-effect-nginx",
    }
    fpm = containers["nextcloud-protected-effect-fpm"]
    nginx = containers["nextcloud-protected-effect-nginx"]
    assert nginx["ports"] == [
        {"name": "effect-http", "containerPort": 8080, "protocol": "TCP"}
    ]
    assert len(nginx["ports"][0]["name"]) <= 15
    fpm_env = {entry["name"]: entry for entry in fpm["env"]}
    assert fpm_env["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]["value"] == digest
    assert (
        fpm_env["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"]["secretKeyRef"][
            "optional"
        ]
        is False
    )
    effect_secret_name = fpm_env["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"][
        "secretKeyRef"
    ]["name"]
    assert effect_secret_name.endswith("-protected-effect")
    assert {mount["name"]: mount["mountPath"] for mount in fpm["volumeMounts"]} == {
        "data": "/var/www/html",
        "setup-hook": "/opt/srw-protected-effect",
        "protected-effect-run": "/run/srw-nextcloud",
        # The shared PHP ini drop-in (APCu size), not a lane file.
        "server-config": "/usr/local/etc/php/conf.d/zz-srw.ini",
    }
    for probe_name in ("readinessProbe", "livenessProbe"):
        probe = nginx[probe_name]
        command = "\n".join(probe["exec"]["command"])
        assert "wget -q -O /dev/null" in command
        assert "X-SRW-Backend-Instance:" in command
        assert (
            "http://127.0.0.1:8080/index.php/apps/"
            "srw_protected_effect/api/v1/capability" in command
        )
    nginx_config = hooks["data"]["nginx.conf"]
    assert "server_name localhost;" in nginx_config
    # Both allowlisted Nextcloud front-controller locations receive one
    # trusted canonical Host; the capability script does not need it.
    assert nginx_config.count("fastcgi_param HTTP_HOST localhost;") == 2

    service = next(
        document
        for document in documents
        if document.get("kind") == "Service"
        and document.get("metadata", {}).get("name", "").endswith("-protected-effect")
    )
    assert service["spec"]["type"] == "ClusterIP"
    assert service["spec"]["ports"] == [
        {
            "name": "http",
            "port": 80,
            "targetPort": "effect-http",
            "protocol": "TCP",
        }
    ]
    assert app_config["data"]["NEXTCLOUD_PROTECTED_EFFECT_URL"] == (
        f"http://{service['metadata']['name']}"
    )

    network_policy = next(
        document
        for document in documents
        if document.get("kind") == "NetworkPolicy"
        and document.get("metadata", {}).get("name") == service["metadata"]["name"]
    )
    assert (
        network_policy["spec"]["podSelector"]["matchLabels"]
        == service["spec"]["selector"]
    )
    ordinary, protected = network_policy["spec"]["ingress"]
    assert ordinary == {"ports": [{"protocol": "TCP", "port": 80}]}
    assert protected["ports"] == [{"protocol": "TCP", "port": 8080}]
    assert protected["from"] == [
        {
            "podSelector": {
                "matchLabels": {
                    "app.kubernetes.io/name": "superhuman-remote-worker",
                    "app.kubernetes.io/instance": "protected-effect-proof",
                    "app.kubernetes.io/component": "orchestrator",
                }
            }
        }
    ]

    orchestrator = next(
        container
        for container in orchestrator_deployment["spec"]["template"]["spec"][
            "containers"
        ]
        if container["name"] == "orchestrator"
    )
    orchestrator_env = {entry["name"]: entry for entry in orchestrator["env"]}
    assert set(orchestrator_env) >= {
        "NEXTCLOUD_PROTECTED_EFFECT_URL",
        "NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256",
        "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
    }
    assert (
        orchestrator_env["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"][
            "secretKeyRef"
        ]["optional"]
        is False
    )
    assert (
        orchestrator_env["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]["valueFrom"][
            "secretKeyRef"
        ]["name"]
        == effect_secret_name
    )
    external = next(
        document
        for document in documents
        if document.get("kind") == "ExternalSecret"
        and document.get("metadata", {}).get("name") == effect_secret_name
    )
    assert external["spec"]["refreshPolicy"] == "CreatedOnce"
    assert external["spec"]["target"] == {
        "name": effect_secret_name,
        "creationPolicy": "Orphan",
        "immutable": True,
    }
    assert external["spec"]["data"] == [
        {
            "secretKey": "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
            "remoteRef": {
                "key": "test/protected-effect",
                "property": "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY",
            },
        }
    ]
    effect_key_sources = [
        document
        for document in documents
        if "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY" in yaml.safe_dump(document)
        and document.get("kind") in {"Secret", "ExternalSecret"}
    ]
    assert effect_key_sources == [external]


def test_chart_managed_effect_key_is_stable_length_and_not_plain_config() -> None:
    documents = _documents(
        *_enabled_args(),
        "--set",
        "externalSecrets.enabled=false",
        "--set",
        "secrets.create=true",
    )
    secret = next(
        document
        for document in documents
        if document.get("kind") == "Secret"
        and "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY" in (document.get("stringData") or {})
    )
    key = secret["stringData"]["NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY"]
    assert isinstance(key, str) and len(key.encode("utf-8")) >= 32
    assert secret["immutable"] is True
    shared = next(
        document
        for document in documents
        if document.get("kind") == "Secret"
        and "APP_ENCRYPTION_KEY" in (document.get("stringData") or {})
    )
    assert "NEXTCLOUD_PROTECTED_EFFECT_HMAC_KEY" not in shared["stringData"]
    configmaps = [
        document for document in documents if document.get("kind") == "ConfigMap"
    ]
    assert all(key not in yaml.safe_dump(configmap) for configmap in configmaps)


@pytest.mark.parametrize(
    "extra",
    [
        ("--set", "nextcloud.protectedEffect.enabled=true"),
        (
            "--set",
            "opencloud.enabled=false",
            "--set",
            "nextcloud.enabled=true",
            "--set",
            "nextcloud.replicas=1",
            "--set",
            "agent.protectedCloudModeEnabled=true",
            "--set",
            "nextcloud.protectedEffect.enabled=false",
        ),
        (
            "--set",
            "agent.protectedCloudModeEnabled=true",
            "--set",
            "cloud.externalBackend=nextcloud",
        ),
        (
            *_enabled_args(),
            "--set",
            "externalSecrets.enabled=false",
            "--set",
            "secrets.create=false",
        ),
        (
            *_enabled_args(),
            "--set",
            "nextcloud.protectedEffect.handlerBoundSeconds=61",
        ),
    ],
)
def test_lane_refuses_external_or_unbounded_server_configuration(
    extra: tuple[str, ...],
) -> None:
    result = _render(*extra, check=False)

    assert result.returncode != 0
    assert "protectedEffect" in result.stderr or "protected-effect" in result.stderr


def test_lane_is_derived_from_the_protected_cloud_feature_flag() -> None:
    """The lane is not a second decision: protected cloud mode implies it.

    `agent.protectedCloudModeEnabled` is inert without the server lane —
    NextcloudBackend raises NOT_SUPPORTED on every capability request when the
    effect client is unconfigured — so a bundled, internal Nextcloud derives
    `nextcloud.protectedEffect.enabled` from the feature flag rather than making
    an operator assert the same decision in two places.
    """

    bundled = (
        "--set",
        "opencloud.enabled=false",
        "--set",
        "nextcloud.enabled=true",
        "--set",
        "nextcloud.replicas=1",
        "--set",
        "nextcloud.protectedEffect.hmacVaultPath=test/protected-effect",
    )

    derived = _render(*bundled, "--set", "agent.protectedCloudModeEnabled=true")
    assert "nextcloud-protected-effect-fpm" in derived.stdout

    # Explicit true still stands the lane up ahead of the flag, so the
    # adoption-once HMAC root can be created and verified as its own step.
    staged = _render(*bundled, "--set", "nextcloud.protectedEffect.enabled=true")
    assert "nextcloud-protected-effect-fpm" in staged.stdout

    # And an unset lane stays off while the feature flag is off.
    off = _render(*bundled)
    assert "nextcloud-protected-effect-fpm" not in off.stdout


def test_server_bundle_verifies_before_nextcloud_and_exposes_only_five_posts() -> None:
    common = (EFFECT_FILES / "common.php").read_text()
    capability = (EFFECT_FILES / "capability.php").read_text()
    prepend = (EFFECT_FILES / "prepend.php").read_text()
    launcher = (EFFECT_FILES / "start-fpm.sh").read_text()
    nginx = (EFFECT_FILES / "nginx.conf").read_text()

    assert (
        'const SRW_EFFECT_CAPABILITY_DOMAIN = "srw-nextcloud-effect-capability-v1\\0";'
        in common
    )
    assert (
        'const SRW_EFFECT_REQUEST_DOMAIN = "srw-nextcloud-effect-request-v1\\0";'
        in common
    )
    assert (
        "const SRW_MAIN_CLOUD_INSTALLATION_DOMAIN = "
        '"srw-main-cloud-installation-v1\\0";' in capability
    )
    assert (
        "const SRW_INSTALLATION_ATTESTATION_DOMAIN = "
        '"srw-nextcloud-installation-attestation-v1\\0";' in capability
    )
    assert "'/var/www/html/config/config.php'" in capability
    assert (
        "'installation_proof_sha256' => "
        "srw_effect_installation_proof_sha256()" in capability
    )
    assert "echo $remoteIdentity" not in capability
    assert "json_encode($remoteIdentity" not in capability
    assert "srw_effect_verify_request(" in prepend
    assert "($_SERVER['SRW_PROTECTED_EFFECT_CAPABILITY'] ?? '') === '1'" in prepend
    assert "/var/www/html/index.php" not in prepend
    assert (
        "request_terminate_timeout = ${NEXTCLOUD_PROTECTED_EFFECT_HANDLER_BOUND_SECONDS}s"
        in launcher
    )
    assert "request_terminate_timeout_track_finished = yes" in launcher
    assert (
        "php_admin_value[auto_prepend_file] = /opt/srw-protected-effect/prepend.php"
        in launcher
    )
    assert nginx.count("limit_except POST { deny all; }") == 2
    assert "limit_except POST PUT" not in nginx
    assert "location / {\n            return 404;\n        }" in nginx
    assert (
        nginx.count("fastcgi_pass unix:/run/srw-nextcloud/protected-effect.sock") == 3
    )
    assert nginx.count("fastcgi_param SRW_PROTECTED_EFFECT_CAPABILITY 1;") == 1
    assert nginx.count("fastcgi_param HTTP_ACCEPT $http_accept;") == 2
    assert nginx.count("fastcgi_param HTTP_OCS_APIREQUEST $http_ocs_apirequest;") == 2
