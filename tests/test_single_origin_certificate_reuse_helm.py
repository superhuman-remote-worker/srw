"""An upgrade keeps the single-origin gateway's self-signed certificate.

Regression for CHART-UPG-1 (university upgrade test 2026-10-14): the gateway
template called ``genSelfSignedCert`` on every render, so every ``helm upgrade``
wrote a new key pair and restarted the gateway. Every browser had to accept a
new warning, and every Git/WebDAV/MCP client that pinned the certificate broke.

``srw.singleOriginCertificate`` now receives the live Secret (``lookup``) and
returns its stored pair while the Secret's annotations show it was issued for
the same address and validity with more than 30 days left. The helper takes
the lookup result as a parameter, so the choice is rendered offline here.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import yaml

from tests.test_single_origin_helm import _one, _single_origin_values, render

CHART = Path(__file__).resolve().parents[1] / "helm"
EXPIRES = "srw.io/single-origin-expires"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


def _choose(tmp_path: Path, address: str, days: int, existing: dict | None) -> dict:
    chart = tmp_path / "helper-chart"
    templates = chart / "templates"
    templates.mkdir(parents=True, exist_ok=True)
    (chart / "Chart.yaml").write_text(
        "apiVersion: v2\nname: cert-test\nversion: 0.1.0\n"
    )
    shutil.copyfile(CHART / "templates/_exposure.tpl", templates / "_exposure.tpl")
    (chart / "values.yaml").write_text(
        yaml.safe_dump(
            {
                "exposure": {
                    "singleOrigin": {"address": address, "tls": {"validityDays": days}}
                },
                "existing": existing or {},
            }
        )
    )
    (templates / "choice.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: choice\ndata:\n"
        '  result: {{ include "srw.singleOriginCertificate" '
        '(dict "context" . "existing" .Values.existing) | quote }}\n'
    )
    rendered = subprocess.run(
        ["helm", "template", "cert-test", str(chart)],
        capture_output=True,
        text=True,
        timeout=30,
        check=True,
    )
    return json.loads(yaml.safe_load(rendered.stdout)["data"]["result"])


@pytest.fixture(scope="module")
def issued() -> dict:
    """The Secret a first install writes for 192.0.2.10 with 365 days."""
    return _one(
        render(_single_origin_values("192.0.2.10")), "Secret", "-single-origin-tls"
    )


def _pem(secret: dict, key: str) -> str:
    from base64 import b64decode

    return b64decode(secret["data"][key]).decode()


def _with(secret: dict, **annotations: str | None) -> dict:
    copy = json.loads(json.dumps(secret))
    for name, value in annotations.items():
        key = f"srw.io/single-origin-{name.replace('_', '-')}"
        if value is None:
            copy["metadata"]["annotations"].pop(key, None)
        else:
            copy["metadata"]["annotations"][key] = value
    return copy


def test_first_install_records_what_it_issued(issued):
    annotations = issued["metadata"]["annotations"]
    assert annotations["srw.io/single-origin-address"] == "192.0.2.10"
    assert annotations["srw.io/single-origin-validity-days"] == "365"
    expires = datetime.fromisoformat(annotations[EXPIRES].replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) + timedelta(days=365)
    assert abs(expires - expected) < timedelta(minutes=5)


def test_an_upgrade_with_the_same_address_keeps_the_certificate(tmp_path, issued):
    choice = _choose(tmp_path, "192.0.2.10", 365, issued)
    assert choice["cert"] == _pem(issued, "tls.crt")
    assert choice["key"] == _pem(issued, "tls.key")
    assert choice["expires"] == issued["metadata"]["annotations"][EXPIRES]


@pytest.mark.parametrize(
    ("address", "days", "change"),
    [
        ("192.0.2.11", 365, {}),  # the address moved: the SAN no longer fits
        ("192.0.2.10", 90, {}),  # the operator changed the validity
        (
            "192.0.2.10",
            365,
            {
                "expires": (datetime.now(timezone.utc) + timedelta(days=10)).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
            },
        ),  # inside the 30-day renewal window
        ("192.0.2.10", 365, {"expires": None}),  # a Secret from an older chart
        ("192.0.2.10", 365, {"expires": "not-a-date"}),
    ],
)
def test_a_new_certificate_when_the_stored_one_no_longer_fits(
    tmp_path, issued, address, days, change
):
    choice = _choose(tmp_path, address, days, _with(issued, **change))
    assert choice["cert"] != _pem(issued, "tls.crt")
    assert choice["key"] != _pem(issued, "tls.key")
    expires = datetime.fromisoformat(choice["expires"].replace("Z", "+00:00"))
    expected = datetime.now(timezone.utc) + timedelta(days=days)
    assert abs(expires - expected) < timedelta(minutes=5)


def test_a_secret_without_a_key_pair_is_replaced(tmp_path, issued):
    incomplete = json.loads(json.dumps(issued))
    del incomplete["data"]["tls.key"]
    assert _choose(tmp_path, "192.0.2.10", 365, incomplete)["cert"] != _pem(
        issued, "tls.crt"
    )
