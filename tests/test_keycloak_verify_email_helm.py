"""Keycloak asks a new user to verify their email only when it can send mail."""

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
EVALUATION = CHART / "ci/installer-evaluation-values.yaml"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)

MAIL = {"enabled": True, "smtp": {"host": "smtp.example.com"}}
CASES = [
    pytest.param({}, False, id="no-mail"),
    pytest.param({"email": MAIL}, True, id="smtp-host"),
    # SMTP_HOST reaches the Keycloak pod only with email.enabled.
    pytest.param(
        {"email": {"enabled": False, "smtp": {"host": "smtp.example.com"}}},
        False,
        id="host-without-email-enabled",
    ),
    pytest.param(
        {"email": {"enabled": True, "smtp": {"host": ""}}},
        False,
        id="email-enabled-without-host",
    ),
]


def _keycloak(tmp_path: Path, overrides: dict) -> dict:
    values = tmp_path / "overrides.yaml"
    values.write_text(yaml.safe_dump(overrides), encoding="utf-8")
    result = subprocess.run(
        [
            "helm",
            "template",
            "srw",
            str(CHART),
            "--namespace",
            "srw",
            "-f",
            str(EVALUATION),
            "-f",
            str(values),
            "--show-only",
            "templates/services/keycloak.yaml",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return {
        document["kind"]: document
        for document in yaml.safe_load_all(result.stdout)
        if document
    }


def _post_start_calls(tmp_path: Path, deployment: dict, env: dict) -> list[list[str]]:
    """Run the real postStart hook against a kcadm double; return its argv list."""
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    script = container["lifecycle"]["postStart"]["exec"]["command"][-1]
    log = tmp_path / "kcadm.jsonl"
    kcadm = tmp_path / "kcadm"
    kcadm.write_text(
        f"#!{sys.executable}\n"
        "import json, os, sys\n"
        "with open(os.environ['KCADM_LOG'], 'a') as out:\n"
        "    out.write(json.dumps(sys.argv[1:]) + '\\n')\n",
        encoding="utf-8",
    )
    kcadm.chmod(0o755)
    run_env = {"PATH": os.environ["PATH"], "KCADM_LOG": str(log)}
    run_env.update(
        {item["name"]: item["value"] for item in container["env"] if "value" in item}
    )
    run_env.update(
        {
            "KEYCLOAK_ADMIN": "master-admin",
            "KEYCLOAK_ADMIN_PASSWORD": "master-password",
            "KC_REALM_ADMIN_PASSWORD": "a-long-realm-admin-password",
            **env,
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
            ["bash", "-c", script],
            env=run_env,
            capture_output=True,
            text=True,
            timeout=20,
        )
    assert result.returncode == 0, result.stderr
    return [json.loads(line) for line in log.read_text().splitlines()]


@pytest.mark.parametrize("overrides,expected", CASES)
def test_first_import_verifies_email_only_with_mail(tmp_path, overrides, expected):
    realm = json.loads(
        _keycloak(tmp_path, overrides)["ConfigMap"]["data"]["srw-realm.json"]
    )
    assert realm["verifyEmail"] is expected
    # Self-registration stays; SRW's admin approval gates the new account.
    assert realm["registrationAllowed"] is True


@pytest.mark.parametrize("overrides,expected", CASES)
def test_every_start_reasserts_verify_email_for_existing_realms(
    tmp_path, overrides, expected
):
    deployment = _keycloak(tmp_path, overrides)["Deployment"]
    # SMTP_HOST comes from the ConfigMap at runtime; give the hook what the
    # pod would see, so the SMTP sync runs exactly when mail is configured.
    smtp = (
        {"SMTP_HOST": "smtp.example.com", "SMTP_USER": "u", "SMTP_PASSWORD": "p"}
        if expected
        else {}
    )
    calls = _post_start_calls(tmp_path, deployment, smtp)
    realm_updates = [call for call in calls if call[:2] == ["update", "realms/srw"]]
    verify = [call for call in realm_updates if any("verifyEmail" in a for a in call)]
    assert verify == [
        ["update", "realms/srw", "-s", f"verifyEmail={str(expected).lower()}"]
    ]
    smtp_updates = [
        index
        for index, call in enumerate(calls)
        if call[:2] == ["update", "realms/srw"]
        and any(a.startswith("smtpServer.host=") for a in call)
    ]
    if expected:
        # Mail is configured before the realm starts asking for verification.
        assert smtp_updates and smtp_updates[0] < calls.index(verify[0])
    else:
        assert smtp_updates == []
