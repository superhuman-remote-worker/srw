"""Deployment contract for moving the bundled Nextcloud between releases: pinned
images, one major for every PHP container, a guard against skipped majors, and
an upgrade step no probe can cut short."""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "nextcloud-upgrade-proof"
VALUES = yaml.safe_load((CHART / "values.yaml").read_text())["nextcloud"]
PINNED = re.compile(
    r"^docker\.io/library/nextcloud:(?P<version>\d+\.\d+\.\d+)-(?P<variant>apache|fpm)"
    r"@sha256:[0-9a-f]{64}$"
)
DIGEST = "sha256:" + "0" * 64

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
SECRETS_ONLY_THE_SETUP_HOOK_NEEDS = {
    "NEXTCLOUD_OIDC_CLIENT_SECRET",
    "NEXTCLOUD_AGENT_PASSWORD",
}


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


def _pod(*extra: str) -> dict:
    deployment = next(
        document
        for document in yaml.safe_load_all(_render(*extra).stdout)
        if isinstance(document, dict)
        and document["kind"] == "Deployment"
        and document["metadata"]["name"].endswith("-nextcloud")
    )
    return deployment["spec"]["template"]["spec"]


def _by_name(containers: list[dict]) -> dict[str, dict]:
    return {container["name"]: container for container in containers}


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env") or []}


# --- pinned images ----------------------------------------------------------


def test_both_images_pin_one_release_by_digest() -> None:
    main = PINNED.match(VALUES["image"])
    fpm = PINNED.match(VALUES["protectedEffect"]["fpmImage"])

    assert main and main["variant"] == "apache", VALUES["image"]
    assert fpm and fpm["variant"] == "fpm", VALUES["protectedEffect"]["fpmImage"]
    assert main["version"] == fpm["version"]
    assert VALUES["majorVersion"] is None


def test_every_container_runs_the_pinned_reference() -> None:
    pod = _pod(*BUNDLED, *PROTECTED)
    containers = _by_name(pod["containers"])
    init = _by_name(pod["initContainers"])

    for container in (
        containers["nextcloud"],
        containers["cron"],
        init["nextcloud-version-guard"],
        init["nextcloud-upgrade"],
    ):
        assert container["image"] == VALUES["image"], container["name"]
    assert (
        containers["nextcloud-protected-effect-fpm"]["image"]
        == (VALUES["protectedEffect"]["fpmImage"])
    )


# --- one major for the main image and the protected pool -------------------


def test_a_pool_on_another_major_fails_the_render() -> None:
    result = _render(
        *BUNDLED,
        *PROTECTED,
        "--set",
        f"nextcloud.protectedEffect.fpmImage=docker.io/library/nextcloud:32.0.15-fpm@{DIGEST}",
        check=False,
    )
    assert result.returncode != 0
    assert "nextcloud.image is Nextcloud 31" in result.stderr
    assert "fpmImage is Nextcloud 32" in result.stderr

    # Without the lane there is no pool to disagree with.
    _render(
        *BUNDLED,
        "--set",
        f"nextcloud.protectedEffect.fpmImage=docker.io/library/nextcloud:32.0.15-fpm@{DIGEST}",
    )


@pytest.mark.parametrize(
    ("image", "major", "ok"),
    [
        # A digest-only reference cannot say its major...
        (f"ghcr.io/superhuman-remote-worker/nextcloud@{DIGEST}", None, False),
        # ...so nextcloud.majorVersion has to.
        (f"ghcr.io/superhuman-remote-worker/nextcloud@{DIGEST}", "31", True),
        # A tag that disagrees with majorVersion is a stale value.
        ("docker.io/library/nextcloud:31.0.14-apache", "32", False),
        # A registry port is not a tag.
        ("registry.local:5000/library/nextcloud:31-apache", None, True),
        ("docker.io/library/nextcloud:stable-apache", None, False),
    ],
)
def test_the_major_comes_from_the_tag_or_majorversion(
    image: str, major: str | None, ok: bool
) -> None:
    extra = ["--set", f"nextcloud.image={image}"]
    if major:
        extra += ["--set", f"nextcloud.majorVersion={major}"]
    result = _render(*BUNDLED, *PROTECTED, *extra, check=False)

    assert (result.returncode == 0) is ok, result.stderr


def test_image_references_leave_the_lane_digest_alone() -> None:
    def digest(*extra: str) -> str:
        fpm = _by_name(_pod(*BUNDLED, *PROTECTED, *extra)["containers"])[
            "nextcloud-protected-effect-fpm"
        ]
        return _env(fpm)["NEXTCLOUD_PROTECTED_EFFECT_CONFIG_SHA256"]["value"]

    pinned = digest()
    assert pinned == digest(
        "--set",
        "nextcloud.image=docker.io/library/nextcloud:31-apache",
        "--set",
        "nextcloud.protectedEffect.fpmImage=docker.io/library/nextcloud:31-fpm",
    )
    assert pinned == digest(
        "--set",
        f"nextcloud.image=ghcr.io/superhuman-remote-worker/nextcloud@{DIGEST}",
        "--set",
        f"nextcloud.protectedEffect.fpmImage=ghcr.io/superhuman-remote-worker/nextcloud@{DIGEST}",
        "--set",
        "nextcloud.majorVersion=31",
    )


# --- the version guard ------------------------------------------------------


def _version_php(major: int, minor: int, *, short_array: bool = False) -> str:
    version = f"[{major},0,{minor},1]" if short_array else f"array({major},0,{minor},1)"
    return (
        "<?php \n"
        f"$OC_Version = {version};\n"
        f"$OC_VersionString = '{major}.0.{minor}';\n"
        "$OC_Edition = '';\n"
    )


@pytest.fixture(scope="module")
def guard_script() -> str:
    init = _by_name(_pod(*BUNDLED)["initContainers"])
    guard = init["nextcloud-version-guard"]
    assert guard["command"][:2] == ["sh", "-c"]
    assert guard["volumeMounts"] == [
        {"name": "data", "mountPath": "/var/www/html", "readOnly": True}
    ]
    return guard["command"][2]


@pytest.mark.parametrize(
    ("installed", "image", "ok", "says"),
    [
        (None, 31, True, "nothing to upgrade from"),
        (31, 31, True, "no major upgrade"),
        (31, 32, True, "nextcloud-upgrade runs it"),
        (31, 33, False, "upgrade to 32 first"),
        (32, 31, False, "cannot be downgraded"),
        ("garbage", 31, False, "cannot read the Nextcloud major"),
    ],
)
def test_version_guard_allows_one_major_at_a_time(
    guard_script: str, tmp_path: Path, installed, image, ok: bool, says: str
) -> None:
    if shutil.which("sh") is None:
        pytest.skip("no POSIX shell")
    data = tmp_path / "data"
    data.mkdir()
    image_php = tmp_path / "image-version.php"
    image_php.write_text(_version_php(image, 9))
    if installed == "garbage":
        (data / "version.php").write_text("<?php // not a version file\n")
    elif installed is not None:
        (data / "version.php").write_text(_version_php(installed, 14, short_array=True))
    script = guard_script.replace(
        "/var/www/html/version.php", str(data / "version.php")
    ).replace("/usr/src/nextcloud/version.php", str(image_php))

    result = subprocess.run(["sh", "-c", script], capture_output=True, text=True)

    assert (result.returncode == 0) is ok, result.stdout + result.stderr
    assert says in result.stdout + result.stderr


# --- the upgrade step -------------------------------------------------------


def test_upgrade_runs_after_every_wait_and_the_guard() -> None:
    names = [
        container["name"]
        for container in _pod(*BUNDLED, "--set", "nextcloud.redis.enabled=true")[
            "initContainers"
        ]
    ]
    assert names[-2:] == ["nextcloud-version-guard", "nextcloud-upgrade"]
    assert names.index("wait-for-nextclouddb") < names.index("nextcloud-version-guard")
    assert names.index("wait-for-nextcloud-redis") < names.index(
        "nextcloud-version-guard"
    )


def test_upgrade_is_the_entrypoint_with_no_server_and_no_probe() -> None:
    pod = _pod(*BUNDLED, *PROTECTED, "--set", "nextcloud.redis.enabled=true")
    upgrade = _by_name(pod["initContainers"])["nextcloud-upgrade"]
    apache = _by_name(pod["containers"])["nextcloud"]

    # The image ENTRYPOINT stays; `true` replaces apache2-foreground.
    assert "command" not in upgrade
    assert upgrade["args"] == ["true"]
    assert not {"startupProbe", "livenessProbe", "readinessProbe"} & set(upgrade)
    env = _env(upgrade)
    assert env["NEXTCLOUD_UPDATE"]["value"] == "1"
    # What install and upgrade read, exactly as Apache has it...
    apache_env = _env(apache)
    for name in (
        "NEXTCLOUD_ADMIN_USER",
        "NEXTCLOUD_ADMIN_PASSWORD",
        "POSTGRES_HOST",
        "POSTGRES_DB",
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "NC_setup_create_db_user",
        "NEXTCLOUD_TRUSTED_DOMAINS",
        "OVERWRITEPROTOCOL",
        "OBJECTSTORE_S3_BUCKET",
        "OBJECTSTORE_S3_KEY",
        "REDIS_HOST",
        "REDIS_HOST_PASSWORD",
        "NC_trashbin_retention_obligation",
    ):
        assert env[name] == apache_env[name], name
    # ...but not the setup hook's secrets: the hook stays in the Apache
    # container, and the upgrade step mounts no hook.
    assert not SECRETS_ONLY_THE_SETUP_HOOK_NEEDS & set(env)
    assert {mount["name"] for mount in upgrade["volumeMounts"]} == {
        "data",
        "server-config",
    }
    assert upgrade["resources"] == apache["resources"]


def test_upgrade_installs_sqlite_on_the_sqlite_opt_out() -> None:
    upgrade = _by_name(
        _pod(*BUNDLED, "--set", "nextcloud.database.type=sqlite")["initContainers"]
    )["nextcloud-upgrade"]
    env = _env(upgrade)
    assert env["SQLITE_DATABASE"]["value"] == "nextcloud"
    assert not any(name.startswith("POSTGRES_") for name in env)


def test_every_php_container_has_its_own_serverid() -> None:
    """33 derives job and preview ids from serverid, else from the hostname
    every container in the Pod shares."""
    pod = _pod(*BUNDLED, *PROTECTED)
    containers = {**_by_name(pod["containers"]), **_by_name(pod["initContainers"])}
    ids = {
        name: _env(containers[name])["NC_serverid"]["value"]
        for name in (
            "nextcloud",
            "cron",
            "nextcloud-protected-effect-fpm",
            "nextcloud-upgrade",
        )
    }
    assert len(set(ids.values())) == len(ids), ids
    assert all(1 <= int(value) <= 511 for value in ids.values()), ids
