"""Managed MCP drivers on the control plane (connector drivers D5a).

The connector (its config and its token), its installation from the chart,
what a binding delivers (a lease token and the endpoint URL, never the
token), and the pod a managed MCP server runs in: the stock image beside
SRW's front, with no identity, request file or credential in the server.
"""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException

from orchestrator.application import connectors as connectors_composition
from orchestrator.application.settings import DeploymentSettings
from orchestrator.services import connector_credential_leases as leases
from orchestrator.services import connector_service_images as images
from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
    SupportsCredentialLease,
)
from orchestrator.services.connector_drivers.managed_mcp import ManagedMcpDriver
from orchestrator.services.connector_drivers.matrix import (
    HostingStatus,
    capability_matrix,
)
from orchestrator.services.connector_egress import EgressPins, PinnedHost
from orchestrator.services.connector_service_hosting import credential_generation
from orchestrator.services.connector_service_launch import (
    ServiceLaunchError,
    ServiceLaunchPolicy,
    ServicePodIdentity,
    build_service_launch,
    endpoint_service_name,
)
from shared.connectors.builtin import (
    GITEA_MCP_SPEC,
    MCP_STDIO_TEST_SPEC,
    MCP_TEST_SPEC,
)

CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
DIGEST = "sha256:" + "ab" * 32
GITEA_IMAGE = "docker.gitea.com/gitea-mcp-server:1.8.0@" + DIGEST
TEST_IMAGE = "srw-registry:5000/srw-driver-mcp-test:tilt-1"
TOKEN = "gitea-token-0123456789abcdef"
FRONT = "srw-registry:5000/srw-driver-mcp-front@sha256:" + "cd" * 32
IDENTITY = "sdi_" + "A" * 49


def _draft(**over: Any) -> ConnectorDraft:
    fields: dict[str, Any] = dict(
        name="Gitea",
        connection_url=None,
        credentials=None,
        config=None,
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    fields.update(over)
    return ConnectorDraft(**fields)


def _gitea() -> ManagedMcpDriver:
    return ManagedMcpDriver(GITEA_MCP_SPEC, GITEA_IMAGE)


# =============================================================================
# The connector
# =============================================================================


@pytest.mark.asyncio
async def test_a_gitea_connector_stores_its_url_and_derives_what_its_pod_pins():
    normalized = await _gitea().validate(
        _draft(
            credentials={"token": TOKEN},
            config={"url": "https://Gitea.Example.com/", "access": "ReadOnly"},
        ),
        existing=None,
        ctx=MagicMock(),
    )
    assert normalized.credentials == {"token": TOKEN}
    assert normalized.connection_url is None
    assert normalized.config == {
        "url": "https://Gitea.Example.com",
        "host": "gitea.example.com",
        "port": 443,
        "access": "ReadOnly",
    }
    # A port in the URL is the one pinned; mirrors a caller sends are SRW's.
    normalized = await _gitea().validate(
        _draft(
            credentials={"token": TOKEN},
            config={"url": "http://git.lan:3000", "host": "evil.example", "port": 1},
        ),
        existing=None,
        ctx=MagicMock(),
    )
    assert (normalized.config["host"], normalized.config["port"]) == ("git.lan", 3000)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "over",
    [
        {"credentials": None, "config": {"url": "https://g.example"}},
        {"credentials": {"token": ""}, "config": {"url": "https://g.example"}},
        {"credentials": {"token": " padded "}, "config": {"url": "https://g.example"}},
        {"credentials": {"token": "x", "extra": "y"}, "config": {"url": "https://g"}},
        {"credentials": {"token": "x"}, "config": {}},
        {"credentials": {"token": "x"}, "config": {"url": "ftp://g.example"}},
        {"credentials": {"token": "x"}, "config": {"url": "https://g.example/sub"}},
        {"credentials": {"token": "x"}, "config": {"url": "https://u:p@g.example"}},
        {"credentials": {"token": "x"}, "config": {"url": "https://g", "x": 1}},
        {
            "credentials": {"token": "x"},
            "config": {"url": "https://g", "access": "Root"},
        },
        {
            "credentials": {"token": "x"},
            "config": {"url": "https://g.example"},
            "connection_url": "https://g.example",
        },
    ],
)
async def test_a_gitea_connector_refuses_anything_else(over):
    with pytest.raises(HTTPException) as refused:
        await _gitea().validate(_draft(**over), existing=None, ctx=MagicMock())
    assert refused.value.status_code == 400
    assert "padded" not in str(refused.value.detail) or "token" in str(
        refused.value.detail
    )


@pytest.mark.asyncio
async def test_an_edit_without_a_token_keeps_the_stored_one():
    normalized = await _gitea().validate(
        _draft(credentials={}, config={"url": "https://g.example"}),
        existing={"credentials": {"token": TOKEN}, "config": {}},
        ctx=MagicMock(),
    )
    assert normalized.credentials is None  # keep what is stored
    assert normalized.config["host"] == "g.example"


def test_a_binding_never_carries_the_token_and_is_leased():
    row = {
        "id": CONNECTOR,
        "type": "gitea_mcp",
        "name": "Gitea",
        "credentials": {"token": TOKEN},
        "config": {"url": "https://g.example", "host": "g.example", "port": 443},
        "project_read_only": False,
    }
    ctx = BindContext(
        gates=DeploymentGates(lambda: False),
        logger=MagicMock(),
        default_known_hosts="",
    )
    entry = _gitea().bind(row, row["credentials"], ctx=ctx)
    assert entry["credentials"] == {} and entry["connection_url"] is None
    assert entry["config"]["url"] == "https://g.example"
    assert entry["datasource_id"] == CONNECTOR
    assert TOKEN not in json.dumps(entry)
    assert leases.lease_spec(entry) is GITEA_MCP_SPEC
    driver = _gitea()
    assert isinstance(driver, SupportsCredentialLease)
    assert driver.lease_upstream(row) == {
        "credential": TOKEN,
        "allowed_upstream": ["https://g.example"],
    }
    with pytest.raises(ValueError):
        driver.lease_upstream({"credentials": {}, "config": {}})
    # The token is the one secret of the connector's resource.
    assert driver.secret_leaves({"token": TOKEN}) == [(("token",), "token")]
    assert driver.credential_config({"token": TOKEN}) == {}


@pytest.mark.asyncio
async def test_test_runs_when_a_session_attaches_the_server():
    result = await _gitea().check({}, {}, ctx=MagicMock())
    assert result["status"] == "unsupported"


# =============================================================================
# Installation
# =============================================================================


def test_a_managed_server_is_installed_only_with_its_image():
    registry = builtin_connector_drivers()
    assert registry.for_type("gitea_mcp") is None
    assert registry.for_type("mcp_test") is None
    registry = builtin_connector_drivers(
        managed_mcp_images={"srw.gitea-mcp/v1": GITEA_IMAGE, "srw.mcp-test/v1": ""}
    )
    driver = registry.for_type("gitea_mcp")
    assert isinstance(driver, ManagedMcpDriver)
    assert driver.image_reference == GITEA_IMAGE
    assert registry.for_type("mcp_test") is None
    with pytest.raises(ValueError, match="no managed MCP drivers"):
        builtin_connector_drivers(managed_mcp_images={"srw.echo-service/v1": "x"})


def test_the_images_and_the_front_come_from_the_deployment(monkeypatch):
    monkeypatch.setenv(
        "CONNECTOR_MANAGED_MCP_IMAGES",
        json.dumps({"srw.gitea-mcp/v1": GITEA_IMAGE, "srw.mcp-test/v1": " "}),
    )
    monkeypatch.setenv("CONNECTOR_MCP_FRONT_IMAGE", f" {FRONT} ")
    settings = DeploymentSettings.from_environment()
    assert settings.connector_managed_mcp_images == {"srw.gitea-mcp/v1": GITEA_IMAGE}
    assert settings.connector_mcp_front_image == FRONT
    resources = SimpleNamespace(
        settings=settings,
        connector_drivers=builtin_connector_drivers(
            managed_mcp_images=settings.connector_managed_mcp_images
        ),
        postgres_db=object(),
    )
    image_settings = connectors_composition.service_image_settings(resources)
    assert image_settings.references["srw.gitea-mcp/v1"] == GITEA_IMAGE
    # Hosting off: no endpoint a binding could carry.
    assert image_settings.service_namespace == ""


def test_the_matrix_lists_an_installed_server_with_its_service():
    registry = builtin_connector_drivers(
        managed_mcp_images={"srw.gitea-mcp/v1": GITEA_IMAGE}
    )
    entry = next(
        item
        for item in capability_matrix(registry, hosting=HostingStatus(enabled=True))[
            "drivers"
        ]
        if item["name"] == "srw.gitea-mcp/v1"
    )
    assert entry["plane"] == "service"
    assert entry["service"]["callers"] == ["harness"]
    assert entry["holds_upstream_credentials"] is True
    # SRW curates it and its front enforces the levels, but the image is
    # Gitea's: managed, never trusted, its claims SRW's.
    assert entry["trust"] == {
        "tier": "managed",
        "trusted": False,
        "image": GITEA_IMAGE,
        "claims_declared_by_author": False,
    }


def test_a_development_server_stays_development():
    registry = builtin_connector_drivers(
        managed_mcp_images={"srw.mcp-test/v1": TEST_IMAGE}
    )
    (entry,) = (
        item
        for item in capability_matrix(registry)["drivers"]
        if item["name"] == "srw.mcp-test/v1"
    )
    assert entry["trust"]["tier"] == "development"
    assert entry["trust"]["trusted"] is False


# =============================================================================
# Delivery: a lease token and the endpoint URL
# =============================================================================


@pytest.mark.asyncio
async def test_a_binding_gets_its_endpoint_and_a_lease_token(monkeypatch):
    seen: dict[str, Any] = {}

    async def bind(conn, *, spec, connector_id, owner):
        return DIGEST

    async def issue(
        _conn, *, owner, connector_id, driver, access, image_digest, ttl_seconds
    ):
        seen["issue"] = (driver, access, image_digest)
        return MagicMock(id="l1", connector_id=connector_id, token="scl_x")

    monkeypatch.setattr(images, "bind_service_image", bind)
    monkeypatch.setattr(leases, "issue_or_redeliver", issue)
    images.configure_service_images(
        images.ServiceImageSettings(service_namespace="srw-connectors")
    )
    try:
        entry = {
            "type": "gitea_mcp",
            "name": "G",
            "datasource_id": CONNECTOR,
            "credentials": {},
            "config": {"access": "ReadOnly"},
            "connection_url": None,
        }
        owner = leases.LeaseOwner.thread("t")
        assert await leases.deliver_connector_leases(object(), [entry], owner=owner)
        assert seen["issue"] == ("srw.gitea-mcp/v1", "ReadOnly", DIGEST)
        assert entry["credentials"] == {
            "lease": {"id": "l1", "connector_id": CONNECTOR, "token": "scl_x"}
        }
        assert entry["connection_url"] == (
            f"http://{endpoint_service_name(CONNECTOR, DIGEST)}"
            ".srw-connectors.svc.cluster.local:8080/mcp"
        )
        # Without hosting there is no endpoint: the delivery fails, so the
        # caller refuses rather than sending a server the agent cannot reach.
        images.configure_service_images(images.ServiceImageSettings())
        entry["credentials"] = {}
        with pytest.raises(leases.LeaseDeliveryError, match="hosting is off"):
            await leases.deliver_connector_leases(object(), [entry], owner=owner)
    finally:
        images.configure_service_images(images.ServiceImageSettings())


# =============================================================================
# The pod: the server image beside SRW's front
# =============================================================================

POLICY = ServiceLaunchPolicy(
    namespace="srw-connectors",
    release_namespace="srw",
    shim_image="srw-registry:5000/srw-driver-shim@sha256:" + "ef" * 32,
    exchange_host="srw-orchestrator.srw.svc",
    exchange_address="10.43.0.20",
    exchange_port=8088,
    orchestrator_labels={"app.kubernetes.io/component": "orchestrator"},
    front_image=FRONT,
)
PINS = EgressPins(
    hosts=(PinnedHost("g.example", ("203.0.113.7",), (443,)),),
    resolved_at=datetime(2026, 10, 8, tzinfo=timezone.utc),
)
CONFIG = {"url": "https://g.example", "host": "g.example", "port": 443}


def _identity(spec=GITEA_MCP_SPEC) -> ServicePodIdentity:
    return ServicePodIdentity(
        identity_id="11111111-2222-4333-8444-555555555555",
        connector_id=CONNECTOR,
        driver=spec.name,
        digest=DIGEST,
        generation="hmac-sha256:" + "cd" * 32,
    )


def _plan(spec=GITEA_MCP_SPEC, **over):
    values = dict(
        spec=spec,
        image=f"docker.gitea.com/gitea-mcp-server@{DIGEST}",
        entrypoint=[],
        cmd=["/app/gitea-mcp"],
        config=CONFIG,
        credentials={"token": TOKEN},
        identity_token=IDENTITY,
        pins=PINS,
        policy=POLICY,
    )
    values.update(over)
    return build_service_launch(_identity(spec), **values)


def test_the_server_runs_as_itself_with_nothing_of_srws():
    plan = _plan()
    spec = plan.pod["spec"]
    assert [c["name"] for c in spec["initContainers"]] == ["canary-wait"]
    server, front = spec["containers"]
    assert server["name"] == "driver" and front["name"] == "front"
    assert server["image"] == f"docker.gitea.com/gitea-mcp-server@{DIGEST}"
    assert server["command"] == ["/app/gitea-mcp"]
    assert server["args"] == ["-b", "127.0.0.1", "-p", "8091"]
    assert server["env"] == [
        {"name": "GITEA_HOST", "value": "https://g.example"},
        {"name": "MCP_MODE", "value": "http"},
    ]
    # No identity, no request file, no shim, no named port, no credential.
    assert server["volumeMounts"] == [{"name": "tmp", "mountPath": "/tmp"}]
    assert "ports" not in server
    assert server["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    # It writes nowhere but its /tmp emptyDir: no file a tool writes stays
    # in the shared pod's image.
    assert server["securityContext"]["readOnlyRootFilesystem"] is True
    assert server["securityContext"]["allowPrivilegeEscalation"] is False
    assert TOKEN not in json.dumps(plan.pod) and IDENTITY not in json.dumps(plan.pod)
    assert {v["name"] for v in spec["volumes"]} == {"delivery", "tmp"}


def test_the_front_holds_the_identity_serves_the_port_and_probes_readiness():
    front = _plan().pod["spec"]["containers"][1]
    assert front["image"] == FRONT
    assert front["command"] == ["/srw-mcp-front"] and front["args"] == ["serve"]
    assert front["ports"] == [
        {"name": "srw-driver", "containerPort": 8080, "protocol": "TCP"}
    ]
    assert {m["mountPath"] for m in front["volumeMounts"]} == {
        "/run/srw/request.json",
        "/run/srw/identity",
    }
    assert front["readinessProbe"]["httpGet"] == {
        "path": "/readyz",
        "port": "srw-driver",
    }
    assert front["securityContext"]["runAsNonRoot"] is True
    assert front["securityContext"]["readOnlyRootFilesystem"] is True
    env = {item["name"]: item["value"] for item in front["env"]}
    assert env["SRW_EXCHANGE_URL"] == "http://srw-orchestrator.srw.svc:8088"
    # The front's buffers fit its limit: its heap is held below it.
    assert env["GOMEMLIMIT"] == "200MiB"
    assert front["resources"]["limits"]["memory"] == "256Mi"


def test_the_pods_secret_holds_no_upstream_credential_and_the_front_block():
    plan = _plan()
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request["credentials"] == {}
    assert TOKEN not in json.dumps(request)
    assert request["mcp"]["upstream"] == "http://127.0.0.1:8091/mcp"
    assert request["mcp"]["access"]["ReadOnly"] == ["read"]
    assert "get_me" in request["mcp"]["tools"]["read"]
    assert base64.b64decode(plan.secret["data"]["identity"]).decode() == IDENTITY
    # A token change is no new pod: the pod never held it.
    connector = {"config": CONFIG, "credentials": {"token": TOKEN}}
    rotated = {"config": CONFIG, "credentials": {"token": TOKEN + "2"}}
    assert credential_generation(
        GITEA_MCP_SPEC, connector, private_allowed=False
    ) == credential_generation(GITEA_MCP_SPEC, rotated, private_allowed=False)
    # Nor an access change: the front reads each lease's access.
    read_only = {"config": {**CONFIG, "access": "ReadOnly"}, "credentials": {}}
    assert credential_generation(
        GITEA_MCP_SPEC, connector, private_allowed=False
    ) == credential_generation(GITEA_MCP_SPEC, read_only, private_allowed=False)
    given = _plan(config={**CONFIG, "access": "ReadOnly"})
    request = json.loads(base64.b64decode(given.secret["data"]["request.json"]))
    assert request["connector"]["config"] == CONFIG


def test_only_agent_pods_and_the_orchestrator_reach_the_front():
    ingress = _plan().network_policy["spec"]["ingress"]
    assert len(ingress) == 1
    peers = ingress[0]["from"]
    apps = peers[0]["podSelector"]["matchExpressions"][0]["values"]
    assert set(apps) == {"srw-agent", "srw-persistent-agent", "srw-agent-stateless"}
    assert peers[1]["podSelector"]["matchLabels"] == {
        "app.kubernetes.io/component": "orchestrator"
    }
    assert ingress[0]["ports"] == [{"protocol": "TCP", "port": "srw-driver"}]
    assert not any(
        "agent-workspace" in json.dumps(peer)
        for rule in ingress
        for peer in rule["from"]
    )


def test_a_dollar_in_the_config_is_never_expanded():
    plan = _plan(config={**CONFIG, "url": "https://g.example/$(SECRET)"})
    server = plan.pod["spec"]["containers"][0]
    assert {"name": "GITEA_HOST", "value": "https://g.example/$$(SECRET)"} in server[
        "env"
    ]


def test_a_pod_without_a_pinned_front_or_a_config_value_is_refused():
    import dataclasses

    with pytest.raises(ServiceLaunchError, match="front image"):
        _plan(policy=dataclasses.replace(POLICY, front_image="front:latest"))
    with pytest.raises(ServiceLaunchError, match="config.url"):
        _plan(config={"host": "g.example", "port": 443})


def test_the_test_server_runs_its_own_program_with_its_arguments():
    plan = _plan(
        MCP_TEST_SPEC,
        image=f"srw-registry:5000/srw-driver-mcp-test@{DIGEST}",
        entrypoint=["/srw-mcp-test"],
        cmd=[],
        config={"message": "d5a-0123456789"},
        # It declares no egress: its pod reaches the exchange and nothing else.
        pins=EgressPins(hosts=(), resolved_at=PINS.resolved_at),
    )
    server = plan.pod["spec"]["containers"][0]
    assert server["command"] == ["/srw-mcp-test"]
    assert server["args"] == ["-listen", "127.0.0.1:8091"]
    # The connector's message, which whoami reports: configuration only.
    assert server["env"] == [{"name": "MCP_TEST_MESSAGE", "value": "d5a-0123456789"}]
    # An HTTP server's pod has no stdio bridge.
    assert [c["name"] for c in plan.pod["spec"]["initContainers"]] == ["canary-wait"]
    assert "srw-bin" not in {v["name"] for v in plan.pod["spec"]["volumes"]}
    assert plan.network_policy["spec"]["egress"] == [
        {
            "to": [
                {
                    "namespaceSelector": {
                        "matchLabels": {"kubernetes.io/metadata.name": "srw"}
                    },
                    "podSelector": {
                        "matchLabels": {"app.kubernetes.io/component": "orchestrator"}
                    },
                }
            ],
            "ports": [{"protocol": "TCP", "port": 8088}],
        }
    ]


# =============================================================================
# A stdio server's pod: the stock image behind SRW's stdio bridge (D5b)
# =============================================================================

MEMORY_IMAGE = f"docker.io/mcp/memory@{DIGEST}"
NO_EGRESS = EgressPins(hosts=(), resolved_at=PINS.resolved_at)
#: The capabilities Pod Security baseline allows a container to add.
BASELINE_CAPABILITIES = frozenset(
    {
        "AUDIT_WRITE",
        "CHOWN",
        "DAC_OVERRIDE",
        "FOWNER",
        "FSETID",
        "KILL",
        "MKNOD",
        "NET_BIND_SERVICE",
        "SETFCAP",
        "SETGID",
        "SETPCAP",
        "SETUID",
        "SYS_CHROOT",
    }
)


def _stdio_plan(spec=MCP_STDIO_TEST_SPEC, **over):
    values = dict(
        image=MEMORY_IMAGE,
        # Docker's mcp/memory: ENTRYPOINT ["node", "dist/index.js"], no CMD.
        entrypoint=["node", "dist/index.js"],
        cmd=[],
        config={},
        pins=NO_EGRESS,
    )
    values.update(over)
    return _plan(spec, **values)


def _stdio_spec(**mcp_over):
    import dataclasses

    service = MCP_STDIO_TEST_SPEC.service
    return dataclasses.replace(
        MCP_STDIO_TEST_SPEC,
        service=dataclasses.replace(service, mcp={**service.mcp, **mcp_over}),
    )


def test_the_bridge_is_installed_from_the_front_image_after_the_canary():
    spec = _stdio_plan().pod["spec"]
    canary, install = spec["initContainers"]
    assert canary["name"] == "canary-wait"
    assert install["name"] == "install-bridge"
    # The bridge ships in the front's image: the two are pinned together.
    assert install["image"] == FRONT
    assert install["command"] == ["/srw-mcp-bridge"]
    assert install["args"] == ["install", "/srw/bin"]
    assert install["volumeMounts"] == [{"name": "srw-bin", "mountPath": "/srw/bin"}]
    assert install["securityContext"]["runAsNonRoot"] is True
    assert install["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert {"name": "srw-bin", "emptyDir": {"sizeLimit": "32Mi"}} in spec["volumes"]


def test_the_stock_image_runs_its_own_program_behind_the_bridge():
    spec = _stdio_plan().pod["spec"]
    server, front = spec["containers"]
    assert server["image"] == MEMORY_IMAGE
    assert server["command"] == [
        "/srw/bin/srw-mcp-bridge",
        "serve",
        "--socket",
        "/srw/bridge/bridge.sock",
        "--socket-group",
        "65532",
        "--uid-base",
        "20000",
        "--home-root",
        "/srw/home",
        "--path",
        "/mcp",
        "--max-processes",
        "4",
        "--idle",
        "600s",
        "--process-limit",
        "256",
        "--credential-env",
        "MCP_STDIO_TEST_TOKEN",
        "--",
        "node",
        "dist/index.js",
    ]
    assert server["args"] == []
    # ${binding.home} is the bridge's to fill in, per process ($$ is $).
    assert server["env"] == [
        {"name": "MEMORY_FILE_PATH", "value": "$${binding.home}/memory.json"},
        {"name": "NODE_OPTIONS", "value": "--max-old-space-size=64"},
    ]
    assert server["volumeMounts"] == [
        {"name": "tmp", "mountPath": "/tmp"},
        {"name": "srw-bin", "mountPath": "/srw/bin", "readOnly": True},
        {"name": "srw-bridge", "mountPath": "/srw/bridge"},
        {"name": "srw-home", "mountPath": "/srw/home"},
    ]
    assert "ports" not in server
    # The front is the pod's only named port, as for an HTTP server; it
    # reaches the bridge on the socket, read-only.
    assert front["ports"] == [
        {"name": "srw-driver", "containerPort": 8080, "protocol": "TCP"}
    ]
    assert {
        "name": "srw-bridge",
        "mountPath": "/srw/bridge",
        "readOnly": True,
    } in front["volumeMounts"]
    for volume in (
        {"name": "srw-bridge", "emptyDir": {"sizeLimit": "1Mi"}},
        {"name": "srw-home", "emptyDir": {"sizeLimit": "256Mi"}},
    ):
        assert volume in spec["volumes"]


def test_the_bridge_runs_as_root_with_its_own_capabilities_alone():
    """The server container is root for the bridge, which runs each
    binding's process as a user of its own: SETUID and SETGID to switch,
    KILL, CHOWN, DAC_OVERRIDE and FOWNER for that user's processes and
    files. Pod Security baseline allows each; nothing escalates."""
    server, front = _stdio_plan().pod["spec"]["containers"]
    security = server["securityContext"]
    assert (security["runAsUser"], security["runAsGroup"]) == (0, 0)
    assert security["runAsNonRoot"] is False
    assert security["capabilities"] == {
        "drop": ["ALL"],
        "add": ["CHOWN", "DAC_OVERRIDE", "FOWNER", "KILL", "SETGID", "SETUID"],
    }
    assert set(security["capabilities"]["add"]) <= BASELINE_CAPABILITIES
    assert security["allowPrivilegeEscalation"] is False
    assert security["privileged"] is False
    assert security["readOnlyRootFilesystem"] is True
    # The front stays unprivileged, in the socket's group.
    assert front["securityContext"]["runAsUser"] == 65532
    assert front["securityContext"]["runAsGroup"] == 65532
    assert front["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    # An HTTP server's pod keeps every capability dropped, as itself.
    http_server = _plan(
        MCP_TEST_SPEC, image=f"srw-mcp-test@{DIGEST}", config={"message": "m"}
    ).pod["spec"]["containers"][0]
    assert http_server["securityContext"]["capabilities"] == {"drop": ["ALL"]}
    assert "runAsUser" not in http_server["securityContext"]


def test_a_stdio_pod_holds_no_credential_anywhere():
    plan = _stdio_plan()
    request = json.loads(base64.b64decode(plan.secret["data"]["request.json"]))
    assert request["credentials"] == {}
    assert TOKEN not in json.dumps(
        [plan.pod, plan.secret, plan.service, plan.network_policy]
    )
    # The front's block: the bridge's socket and where a binding's process
    # gets its credential (from the front, per binding).
    assert request["mcp"]["transport"] == "stdio"
    assert request["mcp"]["upstream"] == "http://srw-mcp-bridge/mcp"
    assert request["mcp"]["socket"] == "/srw/bridge/bridge.sock"
    assert request["mcp"]["credential"] == {"env": "MCP_STDIO_TEST_TOKEN"}
    server = plan.pod["spec"]["containers"][0]
    assert not any(e["name"] == "MCP_STDIO_TEST_TOKEN" for e in server["env"])
    assert not any(m["name"] == "delivery" for m in server["volumeMounts"])


def test_a_spec_command_replaces_the_image_program_behind_the_bridge():
    server = _stdio_plan(_stdio_spec(command=["/app/server", "--stdio"])).pod["spec"][
        "containers"
    ][0]
    assert server["command"][-3:] == ["--", "/app/server", "--stdio"]


def test_a_config_value_never_injects_an_option_or_a_shell_at_launch():
    templated = _stdio_spec(args=["${config.root}"])
    plan = _stdio_plan(templated, config={"root": "/data"})
    assert plan.pod["spec"]["containers"][0]["args"] == ["/data"]
    with pytest.raises(ServiceLaunchError, match="as an option"):
        _stdio_plan(templated, config={"root": "--allow-write"})
    with pytest.raises(ServiceLaunchError, match="line break"):
        _stdio_plan(templated, config={"root": "/data\n--allow-write"})
    # The image's own program is a shell: no templated argument.
    with pytest.raises(ServiceLaunchError, match="is a shell"):
        _stdio_plan(
            templated, entrypoint=["/bin/sh", "-c"], cmd=[], config={"root": "x"}
        )
    # The launch checks the whole argv as registration does: a wrapper's
    # shell, a code option at the end of the image's program, the script a
    # runner runs, and an entrypoint SRW cannot read.
    for entrypoint, cmd, fragment in (
        (["/sbin/tini", "--"], ["/bin/sh", "-c", "x"], "is a shell"),
        (["python"], ["-c"], "never code"),
        (["node"], ["-e"], "never code"),
        (["npx", "-y"], [], "literal one comes first"),
        (["/docker-entrypoint.sh"], [], "is a shell"),
        (["/server/mcp-server"], ["stdio"], "name the server's program"),
    ):
        with pytest.raises(ServiceLaunchError, match=fragment):
            _stdio_plan(templated, entrypoint=entrypoint, cmd=cmd, config={"root": "x"})
    # A program the spec names is its author's to vouch for.
    named = _stdio_spec(args=["${config.root}"], command=["/server/mcp-server"])
    assert _stdio_plan(named, config={"root": "x"}).pod["spec"]["containers"][0][
        "args"
    ] == ["x"]
    # Kubernetes never expands a $(VAR) in a value.
    plan = _stdio_plan(templated, config={"root": "/data/$(SECRET)"})
    assert plan.pod["spec"]["containers"][0]["args"] == ["/data/$$(SECRET)"]


def test_the_stdio_test_server_is_installed_only_with_its_image():
    assert builtin_connector_drivers().for_type("mcp_stdio_test") is None
    registry = builtin_connector_drivers(
        managed_mcp_images={"srw.mcp-stdio-test/v1": "mcp/memory:latest"}
    )
    driver = registry.for_type("mcp_stdio_test")
    assert isinstance(driver, ManagedMcpDriver)
    assert driver.image_reference == "mcp/memory:latest"
    assert driver.mcp.stdio
