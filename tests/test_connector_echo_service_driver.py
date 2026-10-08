"""The development echo service driver (connector drivers D5 item 8).

``srw.echo-service/v1`` is the service-plane driver the k3d gate runs: a fake
secret behind a lease, a declared egress host from its config, and a pod
running SRW's ``srw-driver-echo`` image. The image itself is tested in Go
(drivers/echo); its chart and Tilt wiring in
tests/test_connector_service_hosting_helm.py.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.services import connector_credential_leases as leases
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
    SupportsCredentialLease,
)
from orchestrator.services.connector_drivers.echo_service import EchoServiceDriver
from orchestrator.services.connector_drivers.matrix import (
    HostingStatus,
    capability_matrix,
)
from shared.connectors.builtin import (
    DEVELOPMENT_SPECS,
    ECHO_SERVICE_SPEC,
    LEGACY_TYPE_IDS,
    spec_for_type,
)
from shared.connectors.contract import validate_spec

CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
IMAGE = "srw-registry:5000/srw-driver-echo:tilt-1@sha256:" + "ab" * 32


def _draft(**over: Any) -> ConnectorDraft:
    fields: dict[str, Any] = dict(
        name="Echo",
        connection_url=None,
        credentials=None,
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    fields.update(over)
    return ConnectorDraft(**fields)


def test_the_spec_is_a_valid_service_driver_behind_a_lease():
    assert validate_spec(ECHO_SERVICE_SPEC) == []
    assert ECHO_SERVICE_SPEC.plane == "service"
    assert ECHO_SERVICE_SPEC.credential_delivery == "lease"
    assert ECHO_SERVICE_SPEC.service.callers == ("harness", "workspace")
    assert ECHO_SERVICE_SPEC.egress[0].host == "${config.host}"
    assert ECHO_SERVICE_SPEC.needs_dns is None
    assert ECHO_SERVICE_SPEC in DEVELOPMENT_SPECS
    # Its stored type resolves (an agent must read what it is sent), but no
    # catalogue lists it.
    assert spec_for_type("echo_service") is ECHO_SERVICE_SPEC
    assert "echo_service" not in LEGACY_TYPE_IDS


def test_the_driver_is_installed_only_with_an_image():
    assert builtin_connector_drivers().for_type("echo_service") is None
    registry = builtin_connector_drivers(echo_service_image=IMAGE)
    driver = registry.for_type("echo_service")
    assert isinstance(driver, EchoServiceDriver)
    assert isinstance(driver, SupportsCredentialLease)
    assert driver.image_reference == IMAGE
    assert registry.drivers()[-1] is driver
    both = builtin_connector_drivers(lease_probe=True, echo_service_image=IMAGE)
    assert [d.spec.name for d in both.drivers()[-2:]] == [
        "srw.lease-probe/v1",
        "srw.echo-service/v1",
    ]


@pytest.mark.asyncio
async def test_create_stores_the_secret_host_and_port():
    normalized = await EchoServiceDriver(IMAGE).validate(
        _draft(
            credentials={"secret": "s3cret"},
            config={"host": "one.one.one.one", "port": 443, "message": "hi"},
        ),
        existing=None,
        ctx=MagicMock(),
    )
    assert normalized.credentials == {"secret": "s3cret"}
    assert normalized.config == {
        "host": "one.one.one.one",
        "port": 443,
        "message": "hi",
    }
    assert normalized.connection_url is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "over",
    [
        {"credentials": None, "config": {"host": "a.example", "port": 1}},
        {"credentials": {"secret": ""}, "config": {"host": "a.example", "port": 1}},
        {"credentials": {"secret": "x", "k": "v"}, "config": {"host": "a", "port": 1}},
        {"credentials": {"secret": "x"}},
        {"credentials": {"secret": "x"}, "config": {"host": "a.example"}},
        {"credentials": {"secret": "x"}, "config": {"host": "a.example", "port": 0}},
        {"credentials": {"secret": "x"}, "config": {"host": "a.example", "port": True}},
        {"credentials": {"secret": "x"}, "config": {"host": "${x}", "port": 1}},
        {"credentials": {"secret": "x"}, "config": {"host": "https://a", "port": 1}},
        {
            "credentials": {"secret": "x"},
            "config": {"host": "a.example", "port": 1, "other": 1},
        },
        {
            "credentials": {"secret": "x"},
            "config": {"host": "a.example", "port": 1},
            "connection_url": "https://a",
        },
    ],
)
async def test_create_refuses_anything_else(over):
    with pytest.raises(HTTPException) as exc:
        await EchoServiceDriver(IMAGE).validate(
            _draft(**over), existing=None, ctx=MagicMock()
        )
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_an_address_is_a_host_too():
    normalized = await EchoServiceDriver(IMAGE).validate(
        _draft(credentials={"secret": "x"}, config={"host": "1.1.1.1", "port": 443}),
        existing=None,
        ctx=MagicMock(),
    )
    assert normalized.config["host"] == "1.1.1.1"


def test_bind_never_carries_the_secret_and_leases_it():
    row = {
        "id": CONNECTOR,
        "type": "echo_service",
        "name": "Echo",
        "credentials": {"secret": "s3cret"},
        "config": {"host": "one.one.one.one", "port": 443},
        "project_read_only": False,
    }
    ctx = BindContext(
        gates=DeploymentGates(lambda: False, lambda: False),
        logger=MagicMock(),
        default_known_hosts="",
    )
    entry = EchoServiceDriver(IMAGE).bind(row, row["credentials"], ctx=ctx)
    assert entry["credentials"] == {}
    assert entry["datasource_id"] == CONNECTOR
    assert "s3cret" not in repr(entry)
    assert leases.lease_spec(entry) is ECHO_SERVICE_SPEC
    assert leases.needs_leases([entry])


def test_the_upstream_is_the_secret_at_the_configured_destination():
    driver = EchoServiceDriver(IMAGE)
    assert driver.lease_upstream(
        {"credentials": {"secret": "s"}, "config": {"host": "h.example", "port": 443}}
    ) == {"credential": "s", "allowed_upstream": ["h.example:443"]}
    with pytest.raises(ValueError):
        driver.lease_upstream({"credentials": {}, "config": {}})


def test_the_matrix_shows_its_image_and_service():
    registry = builtin_connector_drivers(echo_service_image=IMAGE)
    entry = capability_matrix(registry, hosting=HostingStatus(enabled=True))["drivers"][
        -1
    ]
    assert entry["name"] == "srw.echo-service/v1"
    assert entry["plane"] == "service"
    # SRW's own development driver: its claims are SRW's, never trusted.
    assert entry["trust"] == {
        "tier": "development",
        "trusted": False,
        "image": IMAGE,
        "claims_declared_by_author": False,
    }
    assert entry["service"]["port"] == 8080
    assert entry["service"]["callers"] == ["harness", "workspace"]
    assert entry["egress"]["enforced"]["reason"] == "pinned_per_pod"
    assert entry["holds_upstream_credentials"] is True


@pytest.mark.asyncio
async def test_lease_delivery_binds_a_service_driver_to_an_image(monkeypatch):
    """deliver_connector_leases resolves the image of a service driver's new
    binding and stamps its digest on the lease."""
    seen: dict[str, Any] = {}

    async def bind(conn, *, spec, connector_id, owner):
        seen["bind"] = (spec.name, connector_id, owner)
        return "sha256:" + "cd" * 32

    async def issue(
        _conn, *, owner, connector_id, driver, access, image_digest, ttl_seconds
    ):
        seen["issue"] = (driver, access, image_digest)
        return MagicMock(id="l1", connector_id=connector_id, token="scl_x")

    from orchestrator.services import connector_service_images

    monkeypatch.setattr(connector_service_images, "bind_service_image", bind)
    monkeypatch.setattr(leases, "issue_or_redeliver", issue)
    entries = [
        {"type": "echo_service", "name": "E", "datasource_id": CONNECTOR},
        {
            "type": "lease_probe",
            "name": "P",
            "datasource_id": CONNECTOR.replace("6", "1"),
        },
    ]
    owner = leases.LeaseOwner.thread("t")
    assert await leases.deliver_connector_leases(object(), entries, owner=owner) == 2
    assert seen["bind"] == ("srw.echo-service/v1", CONNECTOR, owner)
    assert seen["issue"][2] == "sha256:" + "cd" * 32
    assert entries[0]["credentials"]["lease"]["token"] == "scl_x"
