"""Deployment contract for the bundled Nextcloud's background jobs and server
settings: the cron container, the retention pins that must precede it, and the
setup hook's server configuration."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

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
SECRETS_THE_CRON_CONTAINER_NEVER_GETS = {
    "NEXTCLOUD_ADMIN_PASSWORD",
    "NEXTCLOUD_AGENT_PASSWORD",
    "NEXTCLOUD_OIDC_CLIENT_SECRET",
}
PHP_INI_MOUNT = {
    "name": "server-config",
    "mountPath": "/usr/local/etc/php/conf.d/zz-srw.ini",
    "subPath": "php-srw.ini",
    "readOnly": True,
}
MPM_MOUNT = {
    "name": "server-config",
    "mountPath": "/etc/apache2/conf-enabled/zz-srw-mpm-prefork.conf",
    "subPath": "apache-mpm-prefork.conf",
    "readOnly": True,
}


def _render(*extra: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    if shutil.which("helm") is None:
        pytest.skip("helm is not installed")
    return subprocess.run(
        [
            "helm",
            "template",
            "nextcloud-server-proof",
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


def _named(documents: list[dict], kind: str, suffix: str) -> dict:
    return next(
        document
        for document in documents
        if document.get("kind") == kind
        and document["metadata"]["name"].endswith(suffix)
    )


def _pod(documents: list[dict]) -> dict:
    return _named(documents, "Deployment", "-nextcloud")["spec"]["template"]["spec"]


def _containers(documents: list[dict]) -> dict[str, dict]:
    return {container["name"]: container for container in _pod(documents)["containers"]}


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env") or []}


def _hook(documents: list[dict]) -> str:
    return _named(documents, "ConfigMap", "-nextcloud-hooks")["data"][
        "setup-nextcloud.sh"
    ]


def test_cron_container_runs_the_image_cron_on_the_data_volume_by_default() -> None:
    documents = _documents(*BUNDLED)
    containers = _containers(documents)
    nextcloud = containers["nextcloud"]
    cron = containers["cron"]

    assert cron["image"] == nextcloud["image"]
    assert cron["command"] == ["/cron.sh"]
    # Code and data, and the PHP ini drop-in: the setup hook (and its
    # secrets) stay with Apache.
    assert cron["volumeMounts"] == [
        {"name": "data", "mountPath": "/var/www/html"},
        PHP_INI_MOUNT,
    ]
    assert "ports" not in cron
    assert cron["resources"] == {
        "requests": {"memory": "128Mi", "cpu": "10m"},
        "limits": {"memory": "768Mi", "cpu": "500m"},
    }


def test_cron_env_is_a_secret_free_subset_of_the_apache_env() -> None:
    for extra in ((), ("--set", "nextcloud.objectStore.enabled=false")):
        containers = _containers(_documents(*BUNDLED, *extra))
        apache_env = _env(containers["nextcloud"])
        cron_env = _env(containers["cron"])

        for name, entry in cron_env.items():
            assert apache_env[name] == entry, name
        assert not SECRETS_THE_CRON_CONTAINER_NEVER_GETS & set(cron_env)
        secret_refs = {name for name, entry in cron_env.items() if "valueFrom" in entry}
        assert secret_refs <= {"OBJECTSTORE_S3_KEY", "OBJECTSTORE_S3_SECRET"}
        # The image's config/*.config.php read these in every PHP process.
        assert {"TRUSTED_PROXIES", "OVERWRITEPROTOCOL"} <= set(cron_env)
        if extra:
            assert not any(name.startswith("OBJECTSTORE_") for name in cron_env)
        else:
            assert secret_refs == {"OBJECTSTORE_S3_KEY", "OBJECTSTORE_S3_SECRET"}


def test_retention_is_pinned_in_env_for_every_process_that_runs_jobs() -> None:
    """The cron container starts alongside Apache, so its first cron.php can
    run before the setup hook pins retention in config.php. NC_* env wins
    over config.php in every process that carries it."""

    containers = _containers(_documents(*BUNDLED))
    for name in ("nextcloud", "cron"):
        env = _env(containers[name])
        assert env["NC_trashbin_retention_obligation"]["value"] == "disabled"
        assert env["NC_versions_retention_obligation"]["value"] == "disabled"


def test_hook_pins_retention_and_share_expiry_before_choosing_the_mode() -> None:
    hook = _hook(_documents(*BUNDLED))

    pins = [
        "occ config:system:set trashbin_retention_obligation --value=disabled",
        "occ config:system:set versions_retention_obligation --value=disabled",
        "occ config:app:set core shareapi_default_internal_expire_date --value=no",
        "occ config:app:set core shareapi_enforce_internal_expire_date --value=no",
    ]
    for pin in pins:
        assert pin in hook
    mode = hook.index("occ background:cron")
    assert all(hook.index(pin) < mode for pin in pins)
    # Before the slow OIDC and app steps, which can fail or take a while.
    assert mode < hook.index("1. OIDC provider registration")
    assert "occ background:ajax" not in hook


def test_without_the_cron_container_jobs_fall_back_to_ajax() -> None:
    documents = _documents(*BUNDLED, "--set", "nextcloud.cron.enabled=false")
    pod = _pod(documents)
    hook = _hook(documents)

    assert "cron" not in _containers(documents)
    assert "terminationGracePeriodSeconds" not in pod
    # Retention stays pinned: ajax mode runs the same jobs.
    assert "trashbin_retention_obligation --value=disabled" in hook
    assert "versions_retention_obligation --value=disabled" in hook
    assert "occ background:cron" not in hook
    # Only a mode left at "cron" is moved; ajax/webcron/none stay as set.
    assert (
        'if [ "$(occ config:app:get core backgroundjobs_mode 2>/dev/null)" = "cron" ];'
        in hook
    )
    assert "occ background:ajax" in hook


def test_cron_waits_for_a_running_cron_php_within_the_grace_period() -> None:
    documents = _documents(*BUNDLED)
    pod = _pod(documents)
    cron = _containers(documents)["cron"]

    command = cron["lifecycle"]["preStop"]["exec"]["command"]
    assert command[:2] == ["/bin/sh", "-c"]
    script = command[2]
    # The bracket keeps pgrep from matching this shell's own command line.
    assert "pgrep -f '/var/www/html/cron[.]php'" in script
    assert not re.search(r"cron\.php", script)
    bound = int(re.search(r'\[ "\$i" -lt (\d+) \]', script).group(1))
    assert 0 < bound < pod["terminationGracePeriodSeconds"]


def test_cron_and_server_settings_leave_the_protected_effect_lane_alone() -> None:
    """Invariant: the lane digest covers only lane values and lane files."""

    def digest(*extra: str) -> str:
        documents = _documents(*BUNDLED, *PROTECTED, *extra)
        fpm = _containers(documents)["nextcloud-protected-effect-fpm"]
        return _env(fpm)["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]["value"]

    baseline = digest()
    assert baseline == digest("--set", "nextcloud.cron.enabled=false")
    assert baseline == digest("--set", "nextcloud.apache.maxRequestWorkers=48")


def test_server_config_sizes_apcu_and_caps_apache_workers() -> None:
    documents = _documents(*BUNDLED)
    data = _named(documents, "ConfigMap", "-nextcloud-server-config")["data"]

    assert re.search(r"^apc\.shm_size=128M$", data["php-srw.ini"], re.MULTILINE)
    mpm = data["apache-mpm-prefork.conf"]
    assert re.search(
        r"<IfModule mpm_prefork_module>\s+MaxRequestWorkers 32\s+</IfModule>", mpm
    )

    raised = _named(
        _documents(*BUNDLED, "--set", "nextcloud.apache.maxRequestWorkers=48"),
        "ConfigMap",
        "-nextcloud-server-config",
    )["data"]["apache-mpm-prefork.conf"]
    assert "MaxRequestWorkers 48" in raised


@pytest.mark.parametrize("workers", ["0", "257"])
def test_schema_bounds_apache_workers(workers: str) -> None:
    result = _render(
        *BUNDLED, "--set", f"nextcloud.apache.maxRequestWorkers={workers}", check=False
    )
    assert result.returncode != 0
    assert "maxRequestWorkers" in result.stderr


def test_php_ini_reaches_every_php_container_and_mpm_only_apache() -> None:
    documents = _documents(*BUNDLED, *PROTECTED)
    containers = _containers(documents)

    assert PHP_INI_MOUNT in containers["nextcloud"]["volumeMounts"]
    assert MPM_MOUNT in containers["nextcloud"]["volumeMounts"]
    for name in ("cron", "nextcloud-protected-effect-fpm"):
        assert PHP_INI_MOUNT in containers[name]["volumeMounts"]
        assert MPM_MOUNT not in containers[name]["volumeMounts"]
    assert not any(
        mount["name"] == "server-config"
        for mount in containers["nextcloud-protected-effect-nginx"]["volumeMounts"]
    )
    volume = next(
        volume
        for volume in _pod(documents)["volumes"]
        if volume["name"] == "server-config"
    )
    assert volume["configMap"]["name"].endswith("-nextcloud-server-config")


def test_a_server_config_change_restarts_nextcloud() -> None:
    """subPath mounts never update in place; the Pod must roll instead."""

    def checksum(*extra: str) -> str:
        template = _named(_documents(*BUNDLED, *extra), "Deployment", "-nextcloud")
        return template["spec"]["template"]["metadata"]["annotations"][
            "checksum/server-config"
        ]

    assert checksum() != checksum("--set", "nextcloud.apache.maxRequestWorkers=48")


def test_s3_bucket_check_is_skipped_only_once_installed_on_s3() -> None:
    line = (
        "occ config:system:set objectstore arguments verify_bucket_exists "
        "--value=false --type=boolean"
    )
    hook = _hook(_documents(*BUNDLED))
    assert line in hook
    # Never creates a partial objectstore entry on a filesystem install.
    guard = "if occ config:system:get objectstore class >/dev/null 2>&1; then"
    assert hook.index(guard) < hook.index(line)
    # The hook only runs on an installed Nextcloud: install has made the bucket.
    assert hook.index('"installed":true') < hook.index(line)

    filesystem = _hook(
        _documents(*BUNDLED, "--set", "nextcloud.objectStore.enabled=false")
    )
    assert "verify_bucket_exists" not in filesystem
