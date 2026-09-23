"""The initial SRW administrator stays consistent across import and restarts."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
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


@pytest.mark.parametrize("username", ["admin", "Team.Admin_1-test", "a" * 64])
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


def test_restart_reconciles_password_and_opencloud_membership_for_chosen_user(tmp_path):
    resources = _resources(
        _render(tmp_path, {"bootstrapAdmin": {"username": "Team.Admin_1"}})
    )
    container = resources["Deployment"]["spec"]["template"]["spec"]["containers"][0]
    script = container["lifecycle"]["postStart"]["exec"]["command"][-1]
    log = tmp_path / "kcadm.jsonl"
    kcadm = tmp_path / "kcadm"
    # The CLI is the external boundary. Execute the entire real shell hook and
    # capture its actual argv, including quoting and the selected lookup filter.
    kcadm.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['KCADM_LOG'], 'a') as out:\n"
        "    out.write(json.dumps(sys.argv[1:]) + '\\n')\n",
        encoding="utf-8",
    )
    kcadm.chmod(0o755)
    env = {"PATH": os.environ["PATH"], "KCADM_LOG": str(log)}
    env.update(
        {item["name"]: item["value"] for item in container["env"] if "value" in item}
    )
    env.update(
        {
            "KEYCLOAK_ADMIN": "master-admin",
            "KEYCLOAK_ADMIN_PASSWORD": "master-password",
            "KC_REALM_ADMIN_PASSWORD": "long password 'quoted' $literal \\ ü",
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
    assert result.returncode == 0, result.stderr
    calls = [json.loads(line) for line in log.read_text().splitlines()]
    password_call = next(call for call in calls if call[0] == "set-password")
    assert password_call == [
        "set-password",
        "-r",
        "srw",
        "--username",
        "Team.Admin_1",
        "--new-password",
        "long password 'quoted' $literal \\ ü",
    ]
    admin_lookup = next(
        call for call in calls if call[:2] == ["get", "users"] and "-q" in call
    )
    assert "username=Team.Admin_1" in admin_lookup
    assert "exact=true" in admin_lookup
    credentials = next(call for call in calls if call[:2] == ["config", "credentials"])
    assert credentials[credentials.index("--user") + 1] == "master-admin"


def _render_existing_identity(tmp_path: Path, username: str, existing: dict):
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
