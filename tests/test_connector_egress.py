"""Driver pod egress pinned per pod (connector drivers D5 item 5).

A declared host is resolved once, checked against the cluster's ranges and
the project tier, and written into the pod's NetworkPolicy ipBlocks and
hostAliases; DNS stays off unless the driver declares it needs it.
"""

from __future__ import annotations

import ipaddress
from datetime import datetime, timezone

import pytest

from orchestrator.services.connector_egress import (
    DNS_PEER,
    EgressPolicy,
    EgressRefused,
    expand_rule,
    pin_egress,
    refusal,
)
from shared.connectors.contract import EgressRule

PUBLIC = EgressPolicy()
HOME = EgressPolicy(allow_private=True)
NOW = datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc)


def resolver(answers: dict[str, list[str]]):
    async def resolve(host: str, ipv6: bool):
        if host not in answers:
            raise OSError("Name or service not known")
        return answers[host]

    return resolve


# =============================================================================
# Declared rules for one connector
# =============================================================================


class TestExpandRule:
    def test_config_references_become_the_host_and_ports(self):
        rule = EgressRule("${config.host}", ("${config.port}", 443))
        host, ports = expand_rule(rule, {"host": "DB.Example.com.", "port": 5432})
        assert host == "db.example.com"
        assert ports == (5432, 443)

    def test_a_port_given_as_digits_is_a_port(self):
        _host, ports = expand_rule(
            EgressRule("x.example", ("${config.port}",)), {"port": "8443"}
        )
        assert ports == (8443,)

    @pytest.mark.parametrize(
        ("rule", "config", "fragment"),
        [
            (EgressRule("${config.host}", (443,)), {}, "config.host, which is not set"),
            (EgressRule("${config.host}", (443,)), {"host": 12}, "not a host name"),
            (EgressRule("x.example", ("${config.port}",)), {"port": "a"}, "not a port"),
            (EgressRule("x.example", (0,)), {}, "out of range"),
            (EgressRule("x.example", (True,)), {}, "not a port"),
        ],
    )
    def test_unusable_declarations_are_refused(self, rule, config, fragment):
        with pytest.raises(EgressRefused, match=fragment):
            expand_rule(rule, config)


# =============================================================================
# Which addresses a pod may reach
# =============================================================================


@pytest.mark.parametrize(
    ("address", "reason"),
    [
        ("10.42.3.4", "cluster"),
        ("10.43.0.10", "cluster"),
        ("127.0.0.1", "loopback"),
        ("169.254.169.254", "link-local"),
        ("224.0.0.1", "multicast"),
        ("0.0.0.0", "unspecified"),
        ("::1", "loopback"),
        ("fe80::1", "link-local"),
        ("::ffff:10.42.0.1", "cluster"),
        ("::ffff:127.0.0.1", "loopback"),
        ("64:ff9b::a2a:1", "NAT64"),
        ("64:ff9b:1::1", "NAT64"),
        ("2002:a00:1::1", "6to4"),
        ("::a2b:a0a", "IPv4-compatible"),  # ::10.43.10.10
        ("::ffff:0:a9fe:a9fe", "IPv4-translated"),  # ::ffff:0:169.254.169.254
        ("fd00:ec2::254", "metadata"),
        ("168.63.129.16", "metadata"),
        ("100.100.100.200", "metadata"),  # Alibaba Cloud, inside CGNAT
        ("fec0::1", "site-local"),
    ],
)
def test_cluster_and_special_ranges_are_always_refused(address, reason):
    for policy in (PUBLIC, HOME):
        found = refusal(ipaddress.ip_address(address), policy)
        assert found is not None and reason in found


@pytest.mark.parametrize(
    "address",
    ["10.0.50.7", "172.18.0.4", "192.168.178.20", "100.64.1.1", "fd00::5"],
)
def test_private_addresses_follow_the_project_tier(address):
    assert "network tier" in refusal(ipaddress.ip_address(address), PUBLIC)
    assert refusal(ipaddress.ip_address(address), HOME) is None


def test_public_addresses_are_reachable_on_every_tier():
    for address in ("1.1.1.1", "2606:4700:4700::1111"):
        assert refusal(ipaddress.ip_address(address), PUBLIC) is None


def test_the_real_cluster_ranges_are_configurable():
    policy = EgressPolicy.build(["10.96.0.0/12", "192.168.0.0/16"], allow_private=True)
    assert "cluster" in refusal(ipaddress.ip_address("10.96.0.1"), policy)
    assert "cluster" in refusal(ipaddress.ip_address("192.168.5.5"), policy)
    # k3s's default range is just private here.
    assert refusal(ipaddress.ip_address("10.42.0.1"), policy) is None


def test_nodes_and_load_balancers_stay_refused_where_private_is_allowed():
    """The home tier gives the LAN back, never the k3s nodes (apiserver
    6443, kubelet 10250, etcd) or the MetalLB range."""
    policy = EgressPolicy.build(
        ["10.42.0.0/16", "10.43.0.0/16"],
        allow_private=True,
        refused_cidrs=["10.0.50.0/24", "10.0.51.0/24"],
    )
    for address in ("10.0.50.11", "10.0.51.200"):
        found = refusal(ipaddress.ip_address(address), policy)
        assert found is not None and "nodes and load balancers" in found
    assert refusal(ipaddress.ip_network("10.0.0.0/16"), policy) is not None
    assert refusal(ipaddress.ip_address("10.0.52.1"), policy) is None
    assert refusal(ipaddress.ip_address("192.168.178.20"), policy) is None


@pytest.mark.asyncio
async def test_a_host_resolving_to_a_node_refuses_the_pod():
    policy = EgressPolicy.build(
        ["10.42.0.0/16", "10.43.0.0/16"],
        allow_private=True,
        refused_cidrs=["10.0.50.0/24"],
    )
    with pytest.raises(EgressRefused, match="10.0.50.3"):
        await pin_egress(
            [EgressRule("nas.home.example", (443,))],
            {},
            policy=policy,
            resolver=resolver({"nas.home.example": ["192.168.178.5", "10.0.50.3"]}),
            now=lambda: NOW,
        )


def test_a_network_overlapping_a_refused_range_is_refused():
    assert refusal(ipaddress.ip_network("0.0.0.0/0"), HOME) is not None
    assert (
        refusal(ipaddress.ip_network("10.0.0.0/8"), HOME) is not None
    )  # holds 10.42/16
    assert refusal(ipaddress.ip_network("203.0.113.0/24"), PUBLIC) is None


# =============================================================================
# Pinning
# =============================================================================


@pytest.mark.asyncio
async def test_a_host_is_pinned_into_the_policy_and_host_aliases():
    pins = await pin_egress(
        (EgressRule("${config.host}", (443,)),),
        {"host": "api.example.com"},
        policy=PUBLIC,
        resolver=resolver(
            {"api.example.com": ["93.184.216.34", "93.184.216.35", "93.184.216.34"]}
        ),
        now=lambda: NOW,
    )
    (pinned,) = pins.hosts
    assert pinned.addresses == ("93.184.216.34", "93.184.216.35")
    assert pins.network_policy_egress() == [
        {
            "to": [
                {"ipBlock": {"cidr": "93.184.216.34/32"}},
                {"ipBlock": {"cidr": "93.184.216.35/32"}},
            ],
            "ports": [{"protocol": "TCP", "port": 443}],
        }
    ]
    assert pins.host_aliases() == [
        {"ip": "93.184.216.34", "hostnames": ["api.example.com"]},
        {"ip": "93.184.216.35", "hostnames": ["api.example.com"]},
    ]
    record = pins.record()
    assert record["dns"] == "none"
    assert record["resolved_at"] == NOW.isoformat()
    assert record["hosts"][0]["host"] == "api.example.com"
    assert record["hosts"][0]["many_addresses"] is False


@pytest.mark.asyncio
async def test_two_names_on_one_address_share_a_host_alias():
    pins = await pin_egress(
        (EgressRule("a.example", (443,)), EgressRule("b.example", (80,), "tcp")),
        {},
        policy=PUBLIC,
        resolver=resolver({"a.example": ["1.2.3.4"], "b.example": ["1.2.3.4"]}),
    )
    assert pins.host_aliases() == [
        {"ip": "1.2.3.4", "hostnames": ["a.example", "b.example"]}
    ]
    assert len(pins.network_policy_egress()) == 2


@pytest.mark.asyncio
async def test_dns_stays_off_unless_declared():
    rule = (EgressRule("1.1.1.1", (443,)),)
    off = await pin_egress(rule, {}, policy=PUBLIC, resolver=resolver({}))
    assert all(
        port["port"] != 53 for r in off.network_policy_egress() for port in r["ports"]
    )
    on = await pin_egress(
        rule, {}, policy=PUBLIC, needs_dns="mongodb+srv", resolver=resolver({})
    )
    dns_rule = on.network_policy_egress()[-1]
    assert dns_rule["to"] == [DNS_PEER]
    assert dns_rule["ports"] == [
        {"protocol": "UDP", "port": 53},
        {"protocol": "TCP", "port": 53},
    ]
    assert on.record()["dns"] == "cluster_resolver"
    assert on.record()["dns_reason"] == "mongodb+srv"


@pytest.mark.asyncio
async def test_an_ipv6_literal_is_pinned_only_on_dual_stack():
    rule = (EgressRule("2606:4700:4700::1111", (443,)),)
    with pytest.raises(EgressRefused, match="IPv4 only"):
        await pin_egress(rule, {}, policy=PUBLIC, resolver=resolver({}))
    dual = EgressPolicy(ipv6=True)
    pins = await pin_egress(rule, {}, policy=dual, resolver=resolver({}))
    assert pins.hosts[0].addresses == ("2606:4700:4700::1111",)


@pytest.mark.asyncio
async def test_a_lookup_that_hangs_counts_as_not_resolving(monkeypatch):
    import asyncio

    from orchestrator.services import connector_egress

    async def hang(*args, **kwargs):
        await asyncio.sleep(30)

    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "getaddrinfo", hang)
    monkeypatch.setattr(connector_egress, "RESOLVE_TIMEOUT_SECONDS", 0.05)
    with pytest.raises(OSError, match="no answer"):
        await connector_egress.system_resolver("slow.example", False)
    with pytest.raises(EgressRefused, match="does not resolve"):
        await pin_egress(
            (EgressRule("slow.example", (443,)),),
            {},
            policy=PUBLIC,
            resolver=connector_egress.system_resolver,
        )


@pytest.mark.asyncio
async def test_literal_addresses_and_networks_need_no_lookup_and_no_alias():
    pins = await pin_egress(
        (EgressRule("1.1.1.1", (443,)), EgressRule("203.0.113.0/24", (22,), "tcp")),
        {},
        policy=PUBLIC,
        resolver=resolver({}),
    )
    assert pins.host_aliases() == []
    assert [rule["to"] for rule in pins.network_policy_egress()] == [
        [{"ipBlock": {"cidr": "1.1.1.1/32"}}],
        [{"ipBlock": {"cidr": "203.0.113.0/24"}}],
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("host", "answers", "policy", "fragment"),
    [
        ("db.internal", ["10.43.0.12"], HOME, "inside the cluster"),
        ("nas.home", ["192.168.178.20"], PUBLIC, "network tier"),
        ("meta.example", ["169.254.169.254"], HOME, "link-local"),
        ("missing.example", None, PUBLIC, "does not resolve"),
        ("v6only.example", ["2001:db8::1"], PUBLIC, "does not resolve to an address"),
        ("bad_host!", ["1.1.1.1"], PUBLIC, "not a host name"),
        ("10.0.0.0/8", None, HOME, "cluster"),
    ],
)
async def test_unreachable_declarations_refuse_the_pod(host, answers, policy, fragment):
    with pytest.raises(EgressRefused, match=fragment):
        await pin_egress(
            (EgressRule(host, (443,)),),
            {},
            policy=policy,
            resolver=resolver({host: answers} if answers else {}),
        )


@pytest.mark.asyncio
async def test_a_home_tier_pins_private_hosts():
    pins = await pin_egress(
        (EgressRule("nas.home", (445,)),),
        {},
        policy=HOME,
        resolver=resolver({"nas.home": ["192.168.178.20"]}),
    )
    assert pins.hosts[0].addresses == ("192.168.178.20",)
    assert pins.record()["private_allowed"] is True


@pytest.mark.asyncio
async def test_aaaa_answers_are_pinned_only_on_dual_stack():
    answers = {"dual.example": ["1.2.3.4", "2606:4700::1"]}
    v4 = await pin_egress(
        (EgressRule("dual.example", (443,)),),
        {},
        policy=PUBLIC,
        resolver=resolver(answers),
    )
    assert v4.hosts[0].addresses == ("1.2.3.4",)
    v6 = await pin_egress(
        (EgressRule("dual.example", (443,)),),
        {},
        policy=EgressPolicy(ipv6=True),
        resolver=resolver(answers),
    )
    assert v6.hosts[0].addresses == ("1.2.3.4", "2606:4700::1")
    assert v6.network_policy_egress()[0]["to"][1] == {
        "ipBlock": {"cidr": "2606:4700::1/128"}
    }


def test_the_settings_never_lose_the_cluster_ranges():
    from orchestrator.application.settings import parse_cidr_list

    assert parse_cidr_list(None) == ("10.42.0.0/16", "10.43.0.0/16")
    assert parse_cidr_list("10.96.0.0/12, 10.244.0.0/16") == (
        "10.244.0.0/16",
        "10.96.0.0/12",
    )
    # A typo is dropped; with nothing left the defaults stand.
    assert parse_cidr_list("10.96.0.0/12,not-a-cidr") == ("10.96.0.0/12",)
    assert parse_cidr_list("nope") == ("10.42.0.0/16", "10.43.0.0/16")
    # Refused ranges have no defaults of their own (the chart supplies them).
    assert parse_cidr_list(None, default=()) == ()


def test_the_environment_reaches_the_settings(monkeypatch):
    from orchestrator.application.settings import DeploymentSettings

    monkeypatch.setenv("CONNECTOR_SERVICE_PODS_ENABLED", "true")
    monkeypatch.setenv("CONNECTOR_SERVICE_CLUSTER_CIDRS", "10.96.0.0/12")
    monkeypatch.setenv("CONNECTOR_SERVICE_PRIVATE_TIERS", "home-allowed,lab")
    monkeypatch.setenv("CONNECTOR_SERVICE_IPV6", "true")
    monkeypatch.setenv("CONNECTOR_SERVICE_REFUSED_CIDRS", "10.0.50.0/24,10.0.51.0/24")
    monkeypatch.setenv("CONNECTOR_SERVICE_POD_IP", "10.42.1.7")
    monkeypatch.setenv("CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS", "srw-registry:5000")
    settings = DeploymentSettings.from_environment()
    assert settings.connector_driver_registry_private_hosts == {"srw-registry:5000"}
    assert settings.connector_service_refused_cidrs == (
        "10.0.50.0/24",
        "10.0.51.0/24",
    )
    assert settings.connector_service_pod_ip == "10.42.1.7"
    assert settings.connector_service_pods_enabled is True
    assert settings.connector_service_enforcement_verified is False
    assert settings.connector_service_cluster_cidrs == ("10.96.0.0/12",)
    assert settings.connector_service_private_tiers == {"home-allowed", "lab"}
    assert settings.connector_service_ipv6 is True
    monkeypatch.setenv("CONNECTOR_SERVICE_PRIVATE_TIERS", "")
    assert DeploymentSettings.from_environment().connector_service_private_tiers == (
        frozenset()
    )


@pytest.mark.asyncio
async def test_a_host_with_many_addresses_is_flagged():
    answers = {"cdn.example": [f"1.2.3.{i}" for i in range(1, 20)]}
    pins = await pin_egress(
        (EgressRule("cdn.example", (443,)),),
        {},
        policy=PUBLIC,
        resolver=resolver(answers),
    )
    assert pins.hosts[0].many_addresses is True
    assert pins.record()["hosts"][0]["many_addresses"] is True
