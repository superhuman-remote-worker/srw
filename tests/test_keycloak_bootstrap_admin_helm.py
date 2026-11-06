"""The initial SRW administrator stays consistent across import and restarts."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _render(tmp_path: Path, keycloak: dict) -> subprocess.CompletedProcess:
    values = tmp_path / "values.yaml"
    values.write_text(
        yaml.safe_dump({"fullnameOverride": "srw", "keycloak": keycloak}),
        encoding="utf-8",
    )
    return subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(CHART),
            "--namespace",
            "srw",
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(values),
            "--show-only",
            "templates/services/keycloak.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


def _resources(result: subprocess.CompletedProcess) -> dict:
    assert result.returncode == 0, result.stderr
    return {
        document["kind"]: document
        for document in yaml.safe_load_all(result.stdout)
        if document
    }


@pytest.mark.parametrize("username", ["srwadmin", "Team.Admin_1-test", "a" * 64])
def test_chosen_username_imports_as_the_srw_administrator(tmp_path, username):
    resources = _resources(
        _render(tmp_path, {"bootstrapAdmin": {"username": username}})
    )
    realm = json.loads(resources["ConfigMap"]["data"]["srw-realm.json"])
    administrator = realm["users"][0]
    assert administrator["username"] == username
    assert "admin" in administrator["realmRoles"]
    assert administrator["credentials"] == [
        {
            "type": "password",
            "value": "${KC_REALM_ADMIN_PASSWORD}",
            "temporary": False,
        }
    ]


@pytest.mark.parametrize("keycloak", [{}, {"bootstrapAdmin": {"username": ""}}])
def test_omitting_username_preserves_the_historical_initial_login(tmp_path, keycloak):
    resources = _resources(_render(tmp_path, keycloak))
    realm = json.loads(resources["ConfigMap"]["data"]["srw-realm.json"])
    assert realm["users"][0]["username"] == "test"


@pytest.mark.parametrize(
    "username",
    ["with space", "-admin", "admin'", "${USER}", "$(id)", "a" * 65, "ümlaut", 123],
)
def test_invalid_usernames_fail_before_deployment(tmp_path, username):
    result = _render(tmp_path, {"bootstrapAdmin": {"username": username}})
    assert result.returncode != 0
    assert "bootstrapAdmin" in result.stderr


def test_bootstrap_admin_cannot_collide_with_enabled_development_users(tmp_path):
    result = _render(
        tmp_path,
        {
            "bootstrapAdmin": {"username": "DEV-ADMIN-1"},
            "devUsers": {"enabled": True},
        },
    )
    assert result.returncode != 0
    assert "devUsers" in result.stderr


ADMIN_ID = "6f1c7a52-0d1e-4c3b-9a55-1f0e2d3c4b5a"
REALM_ADMIN_PASSWORD = "long password 'quoted' $literal \\ ü"


def _admin_lookup_json(created_ms: int | None) -> str:
    """What `kcadm get users --fields id,createdTimestamp` prints (pretty JSON)."""
    if created_ms is None:
        return "[ ]\n"
    return (
        "[ {\n"
        f'  "id" : "{ADMIN_ID}",\n'
        f'  "createdTimestamp" : {created_ms}\n'
        "} ]\n"
    )


def _run_post_start(
    tmp_path: Path, *, created_ms: int | None, fail: tuple[str, ...] = ()
) -> tuple[subprocess.CompletedProcess, list[list[str]]]:
    """Run the real postStart hook against a kcadm double.

    The CLI is the external boundary: the double records every argv, answers
    the realm admin lookup with a user created at ``created_ms`` (or no user),
    and fails the steps named in ``fail`` the way kcadm does (stderr, exit 1).
    """
    resources = _resources(
        _render(tmp_path, {"bootstrapAdmin": {"username": "Team.Admin_1"}})
    )
    container = resources["Deployment"]["spec"]["template"]["spec"]["containers"][0]
    script = container["lifecycle"]["postStart"]["exec"]["command"][-1]
    log = tmp_path / "kcadm.jsonl"
    kcadm = tmp_path / "kcadm"
    kcadm.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "args = sys.argv[1:]\n"
        "with open(os.environ['KCADM_LOG'], 'a') as out:\n"
        "    out.write(json.dumps(args) + '\\n')\n"
        "fail = os.environ.get('KCADM_FAIL', '').split(',')\n"
        "if args[:2] == ['get', 'users'] and any('createdTimestamp' in a for a in args):\n"
        "    if 'admin-lookup' in fail:\n"
        "        sys.stderr.write('HTTP 503 Service Unavailable\\n')\n"
        "        sys.exit(1)\n"
        "    sys.stdout.write(os.environ['KCADM_ADMIN_JSON'])\n"
        "elif args[:1] == ['set-password'] and 'set-password' in fail:\n"
        "    sys.stderr.write('invalidPasswordMinLengthMessage\\n')\n"
        "    sys.exit(1)\n",
        encoding="utf-8",
    )
    kcadm.chmod(0o755)
    env = {
        "PATH": os.environ["PATH"],
        "KCADM_LOG": str(log),
        "KCADM_ADMIN_JSON": _admin_lookup_json(created_ms),
        "KCADM_FAIL": ",".join(fail),
    }
    env.update(
        {item["name"]: item["value"] for item in container["env"] if "value" in item}
    )
    env.update(
        {
            "KEYCLOAK_ADMIN": "master-admin",
            "KEYCLOAK_ADMIN_PASSWORD": "master-password",
            "KC_REALM_ADMIN_PASSWORD": REALM_ADMIN_PASSWORD,
            "OPENCLOUD_KEYCLOAK_CLIENT_SECRET": "opencloud-test-secret",
        }
    )
    with socket.socket() as ready:
        ready.bind(("127.0.0.1", 0))
        ready.listen()
        script = script.replace("/opt/keycloak/bin/kcadm.sh", str(kcadm)).replace(
            "/dev/tcp/localhost/9000",
            f"/dev/tcp/127.0.0.1/{ready.getsockname()[1]}",
        )
        result = subprocess.run(
            ["bash", "-c", script], env=env, capture_output=True, text=True, timeout=20
        )
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    return result, calls


def _now_ms() -> int:
    return int(time.time() * 1000)


def test_first_start_sets_the_initial_password_of_the_admin_it_imported(tmp_path):
    # The realm import created the admin after this container started.
    result, calls = _run_post_start(tmp_path, created_ms=_now_ms() + 60_000)
    assert result.returncode == 0, result.stderr
    assert "WARN" not in result.stdout
    lookup = next(
        call
        for call in calls
        if call[:2] == ["get", "users"] and any("createdTimestamp" in a for a in call)
    )
    assert "username=Team.Admin_1" in lookup
    assert "exact=true" in lookup
    # Matched by the id of the user the lookup found, not by name again.
    assert [call for call in calls if call[0] == "set-password"] == [
        [
            "set-password",
            "-r",
            "srw",
            "--userid",
            ADMIN_ID,
            "--new-password",
            REALM_ADMIN_PASSWORD,
        ]
    ]
    # Every lookup of the chosen admin is exact, OpenCloud membership included.
    for call in calls:
        if "username=Team.Admin_1" in call:
            assert "exact=true" in call
    credentials = next(call for call in calls if call[:2] == ["config", "credentials"])
    assert credentials[credentials.index("--user") + 1] == "master-admin"


def test_later_starts_keep_the_password_the_admin_chose(tmp_path):
    # The admin was created a day before this start: a restart, an upgrade or
    # a rotated Secret must not reset the password.
    result, calls = _run_post_start(tmp_path, created_ms=_now_ms() - 86_400_000)
    assert result.returncode == 0, result.stderr
    assert [call for call in calls if call[0] == "set-password"] == []
    assert "WARN" not in result.stdout


def test_a_renamed_admin_is_logged_instead_of_silently_skipped(tmp_path):
    result, calls = _run_post_start(tmp_path, created_ms=None)
    assert result.returncode == 0, result.stderr
    assert [call for call in calls if call[0] == "set-password"] == []
    assert "WARN: realm admin Team.Admin_1 not found in realm srw" in result.stdout


@pytest.mark.parametrize(
    "fail,message",
    [
        (
            ("admin-lookup",),
            "WARN: kcadm realm admin lookup for Team.Admin_1 failed: "
            "HTTP 503 Service Unavailable",
        ),
        (
            ("set-password",),
            "WARN: kcadm set-password for new realm admin Team.Admin_1 failed: "
            "invalidPasswordMinLengthMessage",
        ),
    ],
)
def test_failures_are_logged_and_never_fail_the_hook(tmp_path, fail, message):
    # A non-zero postStart exit kills the Keycloak container, so a failed
    # lookup or set is reported and the hook carries on.
    result, calls = _run_post_start(
        tmp_path, created_ms=_now_ms() + 60_000, fail=fail
    )
    assert result.returncode == 0, result.stderr
    assert message in result.stdout
    assert REALM_ADMIN_PASSWORD not in result.stdout
    # The steps after the password still run.
    assert any(call[:2] == ["update", "realms/srw"] for call in calls)


def _render_existing_identity(
    tmp_path: Path, username: str, existing: dict, extra: dict | None = None
):
    chart = tmp_path / "helper-chart"
    templates = chart / "templates"
    templates.mkdir(parents=True)
    (chart / "Chart.yaml").write_text(
        "apiVersion: v2\nname: bootstrap-test\nversion: 0.1.0\n"
    )
    shutil.copyfile(CHART / "templates/_helpers.tpl", templates / "_helpers.tpl")
    (chart / "values.yaml").write_text(
        yaml.safe_dump(
            {
                "keycloak": {
                    "bootstrapAdmin": {"username": username},
                    "devUsers": {"enabled": False},
                },
                "existingRealmConfigMap": existing,
                **(extra or {}),
            }
        )
    )
    (templates / "identity.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: bootstrap-result\ndata:\n"
        '  username: {{ include "srw.keycloakBootstrapAdminUsername" '
        '(dict "context" . "existingRealmConfigMap" .Values.existingRealmConfigMap) | quote }}\n'
    )
    return subprocess.run(
        ["helm", "template", "bootstrap-test", str(chart)],
        capture_output=True,
        text=True,
        timeout=30,
    )


def _existing_realm(username: str) -> dict:
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": "srw-keycloak-realm"},
        "data": {
            "srw-realm.json": json.dumps(
                {"realm": "srw", "users": [{"username": username}]}
            )
        },
    }


@pytest.mark.parametrize(
    "requested,existing,expected",
    [
        ("", "test", "test"),
        ("", "admin", "admin"),
        ("admin", "admin", "admin"),
    ],
)
def test_existing_bootstrap_identity_is_retained(
    tmp_path, requested, existing, expected
):
    result = _render_existing_identity(tmp_path, requested, _existing_realm(existing))
    resources = _resources(result)
    assert resources["ConfigMap"]["data"]["username"] == expected


def test_existing_bootstrap_identity_cannot_be_renamed_through_values(tmp_path):
    result = _render_existing_identity(tmp_path, "admin", _existing_realm("test"))
    assert result.returncode != 0
    assert "initial install" in result.stderr


@pytest.mark.parametrize("realm", ["not json", "{}", '{"users": []}'])
def test_unreadable_existing_identity_fails_instead_of_resetting_to_test(
    tmp_path, realm
):
    existing = _existing_realm("admin")
    existing["data"]["srw-realm.json"] = realm
    result = _render_existing_identity(tmp_path, "", existing)
    assert result.returncode != 0
    assert "cannot read existing bootstrap username" in result.stderr


# --- Names Gitea reserves --------------------------------------------------
#
# reservedUsernames and reservedUserPatterns in models/user/user.go of the Gitea
# release helm/values.yaml pins (gitea/gitea:1.22-rootless), copied from
# https://github.com/go-gitea/gitea/blob/v1.22.6/models/user/user.go
# (identical in v1.22.0). Gitea lowercases and trims a name before comparing.

GITEA_1_22_RESERVED_NAMES = [
    ".",
    "..",
    ".well-known",
    "admin",
    "api",
    "assets",
    "attachments",
    "avatar",
    "avatars",
    "captcha",
    "commits",
    "debug",
    "error",
    "explore",
    "favicon.ico",
    "ghost",
    "issues",
    "login",
    "manifest.json",
    "metrics",
    "milestones",
    "new",
    "notifications",
    "org",
    "pulls",
    "raw",
    "repo",
    "repo-avatars",
    "robots.txt",
    "search",
    "serviceworker.js",
    "ssh_info",
    "swagger.v1.json",
    "user",
    "v2",
    "gitea-actions",
]
GITEA_1_22_RESERVED_PATTERNS = ["*.keys", "*.gpg", "*.rss", "*.atom", "*.png"]


def test_the_reserved_list_belongs_to_the_pinned_gitea_release():
    # Bumping gitea.image past 1.22 means re-reading Gitea's list.
    values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
    assert values["gitea"]["image"].startswith("gitea/gitea:1.22")


def _reserved_hits(tmp_path: Path, names: list[str]) -> dict[str, str]:
    """Render srw.giteaReservedUsername for each name through a helper chart."""
    chart = tmp_path / "reserved-chart"
    templates = chart / "templates"
    templates.mkdir(parents=True)
    (chart / "Chart.yaml").write_text(
        "apiVersion: v2\nname: reserved-test\nversion: 0.1.0\n"
    )
    shutil.copyfile(CHART / "templates/_helpers.tpl", templates / "_helpers.tpl")
    (chart / "values.yaml").write_text(yaml.safe_dump({"names": names}))
    (templates / "hits.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: hits\ndata:\n"
        "  hits: |\n"
        "    {{- range .Values.names }}\n"
        '    {{ printf "%s=%s" . (include "srw.giteaReservedUsername" .) }}\n'
        "    {{- end }}\n"
    )
    result = subprocess.run(
        ["helm", "template", "reserved-test", str(chart)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    hits = _resources(result)["ConfigMap"]["data"]["hits"]
    return dict(line.split("=", 1) for line in hits.splitlines())


def test_every_gitea_reservation_is_recognised_in_any_case(tmp_path):
    pattern_examples = {
        f"Team{pattern[1:]}": pattern for pattern in GITEA_1_22_RESERVED_PATTERNS
    }
    names = [*GITEA_1_22_RESERVED_NAMES, "ADMIN", "Ghost", *pattern_examples]
    hits = _reserved_hits(tmp_path, names)
    for name in GITEA_1_22_RESERVED_NAMES:
        assert hits[name] == f'name "{name}"'
    assert hits["ADMIN"] == 'name "admin"'
    assert hits["Ghost"] == 'name "ghost"'
    for example, pattern in pattern_examples.items():
        assert hits[example] == f'pattern "{pattern}"'


def test_near_misses_are_not_reserved(tmp_path):
    names = ["srwadmin", "admin1", "administrator", "test", "keys", "png", "a.keysx"]
    assert _reserved_hits(tmp_path, names) == {name: "" for name in names}


def _render_chart(
    tmp_path: Path, values: dict, *flags: str
) -> subprocess.CompletedProcess:
    overrides = tmp_path / "overrides.yaml"
    overrides.write_text(
        yaml.safe_dump({"fullnameOverride": "srw", **values}), encoding="utf-8"
    )
    return subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(CHART),
            "--namespace",
            "srw",
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(overrides),
            *flags,
            "--show-only",
            "templates/services/keycloak.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )


@pytest.mark.parametrize(
    "username,reservation",
    [
        ("admin", 'name "admin"'),
        ("Admin", 'name "admin"'),
        ("explore", 'name "explore"'),
        ("deploy.keys", 'pattern "*.keys"'),
    ],
)
def test_fresh_install_with_bundled_gitea_refuses_a_reserved_name(
    tmp_path, username, reservation
):
    result = _render_chart(
        tmp_path, {"keycloak": {"bootstrapAdmin": {"username": username}}}
    )
    assert result.returncode != 0
    assert f'keycloak.bootstrapAdmin.username "{username}" is reserved in Gitea' in (
        result.stderr
    )
    assert reservation in result.stderr
    assert "Git sign-in and repository sharing" in result.stderr


@pytest.mark.parametrize(
    "values,flags",
    [
        # An upgrade: the realm and this user already exist; the chart cannot
        # rename them, so it warns in NOTES.txt instead of failing.
        pytest.param({}, ("--is-upgrade",), id="upgrade"),
        # The chart does not run an external Git server and cannot know its
        # reserved names; NOTES.txt warns.
        pytest.param({"gitea": {"internal": False}}, (), id="external-git"),
        pytest.param({"gitea": {"enabled": False}}, (), id="no-git"),
    ],
)
def test_a_reserved_name_does_not_fail_where_the_chart_cannot_fix_it(
    tmp_path, values, flags
):
    result = _render_chart(
        tmp_path,
        {"keycloak": {"bootstrapAdmin": {"username": "admin"}}, **values},
        *flags,
    )
    realm = json.loads(_resources(result)["ConfigMap"]["data"]["srw-realm.json"])
    assert realm["users"][0]["username"] == "admin"


BUNDLED_GITEA = {"gitea": {"enabled": True, "internal": True}}


def test_an_existing_realm_keeps_a_reserved_name_even_on_install(tmp_path):
    # The realm ConfigMap is there, so the realm is too: the chart cannot
    # rename the user, and failing would leave the operator nothing to change.
    result = _render_existing_identity(
        tmp_path, "", _existing_realm("admin"), BUNDLED_GITEA
    )
    assert _resources(result)["ConfigMap"]["data"]["username"] == "admin"


def test_without_a_realm_the_helper_refuses_a_reserved_name(tmp_path):
    result = _render_existing_identity(tmp_path, "admin", {}, BUNDLED_GITEA)
    assert result.returncode != 0
    assert "reserved in Gitea" in result.stderr


@pytest.fixture(scope="module")
def notes_probe_chart(tmp_path_factory) -> Path:
    """The real chart plus one manifest that emits the NOTES warning.

    `helm template` does not render NOTES.txt, so the warning is a named
    template rendered here into a ConfigMap (as tests/test_cnpg_backup_helm.py
    does for srw.backupNotes).
    """
    chart = tmp_path_factory.mktemp("reserved-notes") / "chart"
    shutil.copytree(CHART, chart)
    (chart / "templates" / "zz-reserved-notes-probe.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: notes-probe\ndata:\n"
        "  notes: |\n"
        '{{ include "srw.keycloakReservedUsernameNotes" . | indent 4 }}\n'
    )
    return chart


def _reserved_notes(chart: Path, tmp_path: Path, values: dict, *flags: str) -> str:
    overrides = tmp_path / "notes-overrides.yaml"
    overrides.write_text(yaml.safe_dump({"fullnameOverride": "srw", **values}))
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(chart),
            "--namespace",
            "srw",
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(overrides),
            *flags,
            "--show-only",
            "templates/zz-reserved-notes-probe.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    return _resources(result)["ConfigMap"]["data"].get("notes") or ""


def test_notes_txt_carries_the_reserved_name_warning():
    notes = (CHART / "templates/NOTES.txt").read_text(encoding="utf-8")
    assert 'include "srw.keycloakReservedUsernameNotes" .' in notes


def test_upgrade_notes_point_a_reserved_name_at_the_recovery_steps(
    notes_probe_chart, tmp_path
):
    notes = _reserved_notes(
        notes_probe_chart,
        tmp_path,
        {"keycloak": {"bootstrapAdmin": {"username": "admin"}}},
        "--is-upgrade",
    )
    assert "WARNING: the initial administrator's name is reserved in Gitea" in notes
    assert 'keycloak.bootstrapAdmin.username is "admin"' in notes
    assert "cannot rename that user" in notes
    assert "docs/troubleshooting.md#the-first-administrator-has-no-git-account" in notes


def test_external_git_notes_warn_on_install(notes_probe_chart, tmp_path):
    notes = _reserved_notes(
        notes_probe_chart,
        tmp_path,
        {
            "keycloak": {"bootstrapAdmin": {"username": "Admin"}},
            "gitea": {"internal": False},
        },
    )
    assert 'keycloak.bootstrapAdmin.username is "Admin"' in notes
    assert "external Git server" in notes
    assert "cannot rename that user" not in notes


@pytest.mark.parametrize(
    "values,flags",
    [
        pytest.param(
            {"keycloak": {"bootstrapAdmin": {"username": "srwadmin"}}},
            ("--is-upgrade",),
            id="allowed-name",
        ),
        pytest.param(
            {
                "keycloak": {"bootstrapAdmin": {"username": "admin"}},
                "gitea": {"enabled": False},
            },
            ("--is-upgrade",),
            id="no-git",
        ),
    ],
)
def test_notes_stay_quiet_without_a_reserved_name_or_git(
    notes_probe_chart, tmp_path, values, flags
):
    assert _reserved_notes(notes_probe_chart, tmp_path, values, *flags).strip() == ""
