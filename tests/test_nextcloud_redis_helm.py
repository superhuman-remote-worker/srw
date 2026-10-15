"""Deployment contract for the bundled Nextcloud's opt-in Valkey: a cache only
the Nextcloud Pod reaches, shared by every PHP container, and an off switch
that really turns it off."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "nextcloud-redis-proof"
FULLNAME = f"{RELEASE}-{yaml.safe_load((CHART / 'Chart.yaml').read_text())['name']}"
REDIS = f"{FULLNAME}-nextcloud-redis"

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
ON = ("--set", "nextcloud.redis.enabled=true")
PHP_CONTAINERS = ("nextcloud", "cron", "nextcloud-protected-effect-fpm")


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


def _named(documents: list[dict], kind: str, name: str) -> dict:
    return next(
        document
        for document in documents
        if document["kind"] == kind and document["metadata"]["name"] == name
    )


def _nextcloud_pod(documents: list[dict]) -> dict:
    return _named(documents, "Deployment", f"{FULLNAME}-nextcloud")["spec"]["template"][
        "spec"
    ]


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env") or []}


def _hook(documents: list[dict]) -> str:
    return _named(documents, "ConfigMap", f"{FULLNAME}-nextcloud-hooks")["data"][
        "setup-nextcloud.sh"
    ]


def _valkey(documents: list[dict]) -> dict:
    return _named(documents, "Deployment", REDIS)["spec"]["template"]["spec"][
        "containers"
    ][0]


FLAG_OFF = (
    "occ config:system:delete memcache.distributed",
    "occ config:system:delete memcache.locking",
    "occ config:system:delete redis",
)


def test_off_by_default_and_the_hook_clears_what_config_php_froze() -> None:
    documents = _documents(*BUNDLED, *PROTECTED)

    assert not [d for d in documents if d["metadata"]["name"] == REDIS]
    pod = _nextcloud_pod(documents)
    assert "wait-for-nextcloud-redis" not in [c["name"] for c in pod["initContainers"]]
    for container in pod["containers"]:
        assert not any(name.startswith("REDIS_") for name in _env(container))
    # Any config write dumps redis.config.php's values into config.php, so
    # dropping REDIS_HOST alone would leave Nextcloud pointing at nothing.
    hook = _hook(documents)
    guard = hook.index(
        "occ config:system:get memcache.locking 2>/dev/null | grep -q 'Redis'"
    )
    for line in FLAG_OFF:
        assert hook.index(line) > guard


def test_on_renders_a_pinned_cache_that_keeps_nothing() -> None:
    documents = _documents(*BUNDLED, *ON)
    deployment = _named(documents, "Deployment", REDIS)
    valkey = _valkey(documents)

    # Two instances at once would split file locks between them.
    assert deployment["spec"]["strategy"] == {"type": "Recreate"}
    assert deployment["spec"]["replicas"] == 1
    assert re.fullmatch(
        r"docker\.io/valkey/valkey:\d+\.\d+\.\d+-alpine", valkey["image"]
    ), valkey["image"]
    args = valkey["args"]
    assert args[args.index("--save") + 1] == ""
    assert args[args.index("--appendonly") + 1] == "no"
    assert args[args.index("--requirepass") + 1] == "$(NEXTCLOUD_REDIS_PASSWORD)"
    assert args[args.index("--maxmemory-policy") + 1] == "allkeys-lru"
    # 75 % of the 256Mi limit: eviction, not the OOM killer, makes room.
    assert args[args.index("--maxmemory") + 1] == "192mb"
    env = _env(valkey)
    for name in ("NEXTCLOUD_REDIS_PASSWORD", "REDISCLI_AUTH"):
        assert env[name]["valueFrom"]["secretKeyRef"]["key"] == (
            "NEXTCLOUD_REDIS_PASSWORD"
        )
    # valkey-cli exits 0 on NOAUTH; only PONG counts.
    for probe in ("readinessProbe", "livenessProbe"):
        assert "grep -q PONG" in valkey[probe]["exec"]["command"][-1]
    service = _named(documents, "Service", REDIS)
    assert service["spec"]["ports"][0]["port"] == 6379

    assert "0d. Valkey off" not in _hook(documents)


def test_maxmemory_follows_the_memory_limit() -> None:
    valkey = _valkey(
        _documents(
            *BUNDLED, *ON, "--set", "nextcloud.redis.resources.limits.memory=512Mi"
        )
    )
    assert valkey["args"][valkey["args"].index("--maxmemory") + 1] == "384mb"


def test_only_the_nextcloud_pod_reaches_valkey() -> None:
    """Agent Pods envFrom the whole application Secret, password included."""
    policy = _named(_documents(*BUNDLED, *ON), "NetworkPolicy", REDIS)
    assert (
        policy["spec"]["podSelector"]["matchLabels"]["app.kubernetes.io/component"]
        == "nextcloud-redis"
    )
    (rule,) = policy["spec"]["ingress"]
    assert rule["ports"] == [{"port": 6379, "protocol": "TCP"}]
    assert [source["podSelector"]["matchLabels"] for source in rule["from"]] == [
        {
            "app.kubernetes.io/name": "superhuman-remote-worker",
            "app.kubernetes.io/instance": RELEASE,
            "app.kubernetes.io/component": "nextcloud",
        }
    ]


def test_every_php_container_shares_one_valkey() -> None:
    documents = _documents(*BUNDLED, *PROTECTED, *ON)
    pod = _nextcloud_pod(documents)
    containers = {container["name"]: container for container in pod["containers"]}

    expected = {
        "REDIS_HOST": {"name": "REDIS_HOST", "value": REDIS},
        "REDIS_HOST_PORT": {"name": "REDIS_HOST_PORT", "value": "6379"},
        "REDIS_HOST_PASSWORD": {
            "name": "REDIS_HOST_PASSWORD",
            "valueFrom": {
                "secretKeyRef": {
                    "name": FULLNAME,
                    "key": "NEXTCLOUD_REDIS_PASSWORD",
                }
            },
        },
    }
    for name in PHP_CONTAINERS:
        env = _env(containers[name])
        assert {key: env[key] for key in expected} == expected, name
    # Still no admin, OIDC or agent secret on cron: only S3 and Valkey.
    cron_secrets = {
        name for name, entry in _env(containers["cron"]).items() if "valueFrom" in entry
    }
    assert cron_secrets <= {
        "OBJECTSTORE_S3_KEY",
        "OBJECTSTORE_S3_SECRET",
        "REDIS_HOST_PASSWORD",
    }
    # Every PHP request needs it once configured, the setup hook's included.
    assert "wait-for-nextcloud-redis" in [c["name"] for c in pod["initContainers"]]


def test_valkey_leaves_the_protected_lane_digest_alone() -> None:
    def digest(*extra: str) -> str:
        pod = _nextcloud_pod(_documents(*BUNDLED, *PROTECTED, *extra))
        fpm = next(
            c
            for c in pod["containers"]
            if c["name"] == "nextcloud-protected-effect-fpm"
        )
        return _env(fpm)["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]["value"]

    assert digest() == digest(*ON)


def test_secrets_create_generates_the_password_only_when_enabled() -> None:
    create = (
        "--set",
        "externalSecrets.enabled=false",
        "--set",
        "secrets.create=true",
    )

    def app_secret(*extra: str) -> tuple[dict, str]:
        raw = _render(*create, *BUNDLED, *extra).stdout
        secret = next(
            d
            for d in yaml.safe_load_all(raw)
            if isinstance(d, dict)
            and d["kind"] == "Secret"
            and "APP_ENCRYPTION_KEY" in d.get("stringData", {})
        )
        return secret, raw

    assert "NEXTCLOUD_REDIS_PASSWORD" not in app_secret()[0]["stringData"]
    assert len(app_secret(*ON)[0]["stringData"]["NEXTCLOUD_REDIS_PASSWORD"]) == 32
    secret, raw = app_secret(*ON, "--set", "secrets.values.NEXTCLOUD_REDIS_PASSWORD=op")
    assert secret["stringData"]["NEXTCLOUD_REDIS_PASSWORD"] == "op"
    assert raw.count("NEXTCLOUD_REDIS_PASSWORD: ") == 1


def test_no_valkey_without_a_bundled_nextcloud() -> None:
    documents = _documents(
        "--set",
        "opencloud.enabled=false",
        "--set",
        "nextcloud.enabled=true",
        "--set",
        "nextcloud.internal=false",
        *ON,
    )
    assert not [d for d in documents if d["metadata"]["name"] == REDIS]
