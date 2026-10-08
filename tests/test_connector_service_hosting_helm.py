"""Service-plane driver hosting's chart surface (connector drivers D5).

The connector driver namespace and its baseline (Pod Security baseline, a
static default deny, quotas, a LimitRange, a token-less ServiceAccount and the
orchestrator's Role) render only when ``connectors.servicePods.enabled``; the
chart renders cleanly either way, and the k3d profile turns it on.
"""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)

NAMESPACE = "srw-superhuman-remote-worker-connectors"
EXCHANGE = "orchestrator.connectorLeases.exchangePort=8088"
ON = "connectors.servicePods.enabled=true"


def render(*settings: str, values: tuple[Path, ...] = ()) -> list[dict]:
    command = [
        "helm",
        "template",
        "srw",
        str(ROOT / "helm"),
        "-n",
        "srw",
        "-f",
        str(ROOT / "helm/ci/test-values.yaml"),
    ]
    for path in values:
        command.extend(["-f", str(path)])
    for setting in settings:
        command.extend(["--set", setting])
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    return [doc for doc in yaml.safe_load_all(result.stdout) if doc]


def in_namespace(docs: list[dict], namespace: str = NAMESPACE) -> list[dict]:
    return [
        doc
        for doc in docs
        if doc["metadata"].get("namespace") == namespace
        or (doc["kind"] == "Namespace" and doc["metadata"]["name"] == namespace)
    ]


def one(docs: list[dict], kind: str, name: str | None = None) -> dict:
    found = [
        doc
        for doc in docs
        if doc["kind"] == kind and (name is None or doc["metadata"]["name"] == name)
    ]
    assert len(found) == 1, (kind, name, [d["metadata"]["name"] for d in found])
    return found[0]


def test_off_by_default_renders_no_driver_namespace():
    docs = render()
    assert in_namespace(docs) == []
    assert not any(doc["kind"] == "LimitRange" for doc in docs)


def test_the_baseline_renders_when_enabled():
    docs = in_namespace(render(EXCHANGE, ON))
    namespace = one(docs, "Namespace")
    labels = namespace["metadata"]["labels"]
    assert labels["pod-security.kubernetes.io/enforce"] == "baseline"
    assert labels["pod-security.kubernetes.io/enforce-version"] == "v1.31"
    assert labels["pod-security.kubernetes.io/audit"] == "restricted"
    assert namespace["metadata"]["annotations"] == {"helm.sh/resource-policy": "keep"}

    deny = one(docs, "NetworkPolicy", "srw-connectors-default-deny")
    assert deny["spec"] == {"podSelector": {}, "policyTypes": ["Ingress", "Egress"]}

    account = one(docs, "ServiceAccount", "srw-connector-driver")
    assert account["automountServiceAccountToken"] is False
    # The driver account is bound to nothing.
    assert not any(
        doc["kind"] in {"RoleBinding", "ClusterRoleBinding"}
        and any(
            subject.get("name") == "srw-connector-driver"
            for subject in doc.get("subjects", [])
        )
        for doc in render(EXCHANGE, ON)
    )


def test_quotas_split_service_and_bind_time_pods():
    docs = in_namespace(
        render(EXCHANGE, ON, "connectors.servicePods.maxInstallation=7")
    )
    service = one(docs, "ResourceQuota", "srw-connector-service-pods")
    assert service["spec"] == {"scopes": ["NotTerminating"], "hard": {"pods": "7"}}
    bind_time = one(docs, "ResourceQuota", "srw-connector-bind-time-pods")
    assert bind_time["spec"] == {"scopes": ["Terminating"], "hard": {"pods": "10"}}
    compute = one(docs, "ResourceQuota", "srw-connector-compute")["spec"]["hard"]
    assert {
        "requests.cpu",
        "requests.memory",
        "limits.cpu",
        "limits.memory",
        "requests.ephemeral-storage",
        "count/secrets",
        "count/services",
    } <= set(compute)
    assert compute["services.loadbalancers"] == "0"
    assert compute["services.nodeports"] == "0"
    (limits,) = one(docs, "LimitRange")["spec"]["limits"]
    assert limits["type"] == "Container"
    assert set(limits) == {"type", "defaultRequest", "default", "max"}
    assert limits["max"]["memory"] == "2Gi"


def test_the_orchestrator_role_has_no_exec_attach_or_log():
    docs = in_namespace(render(EXCHANGE, ON))
    role = one(docs, "Role", "srw-connector-hosting")
    resources = {
        resource: set(rule["verbs"])
        for rule in role["rules"]
        for resource in rule["resources"]
    }
    assert set(resources) == {"pods", "secrets", "services", "networkpolicies"}
    assert "list" in resources["pods"] and "list" in resources["networkpolicies"]
    assert not {"pods/exec", "pods/attach", "pods/log"} & set(resources)
    binding = one(docs, "RoleBinding", "srw-connector-hosting")
    (subject,) = binding["subjects"]
    assert subject["kind"] == "ServiceAccount"
    assert subject["namespace"] == "srw"


def test_one_namespace_for_the_baseline_and_the_exchange_policy():
    docs = render(
        EXCHANGE,
        ON,
        "orchestrator.connectorLeases.networkPolicy.driverNamespace=drivers",
    )
    assert one(in_namespace(docs, "drivers"), "Namespace")
    policy = next(
        doc
        for doc in docs
        if doc["kind"] == "NetworkPolicy"
        and doc["metadata"]["name"].endswith("-orchestrator-ingress")
    )
    peer = policy["spec"]["ingress"][1]["from"][0]["namespaceSelector"]
    assert peer["matchLabels"]["kubernetes.io/metadata.name"] == "drivers"
    # The hosting key wins over the exchange's.
    docs = render(
        EXCHANGE,
        ON,
        "orchestrator.connectorLeases.networkPolicy.driverNamespace=drivers",
        "connectors.servicePods.namespace=hosted",
    )
    assert one(in_namespace(docs, "hosted"), "Namespace")
    assert in_namespace(docs, "drivers") == []


@pytest.mark.parametrize(
    "settings",
    [
        # Hosting needs the exchange: driver pods authenticate to it.
        (ON,),
        (EXCHANGE, ON, "connectors.servicePods.namespace=srw"),
        (
            EXCHANGE,
            ON,
            "connectors.servicePods.namespace=same",
            "manifestHosting.namespace=same",
        ),
    ],
)
def test_misconfigurations_fail_at_template_time(settings):
    with pytest.raises(subprocess.CalledProcessError):
        render(*settings)


def test_the_k3d_profile_turns_hosting_on_and_renders():
    example = ROOT / "deployment/values-local.yaml.example"
    values = yaml.safe_load(example.read_text())
    assert values["connectors"]["servicePods"]["enabled"] is True
    docs = in_namespace(render(values=(example,)), "srw-connectors")
    assert one(docs, "Namespace")
    assert one(docs, "NetworkPolicy", "srw-connectors-default-deny")


def orchestrator_env(docs: list[dict]) -> dict[str, str]:
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    container = next(
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    )
    return {item["name"]: item.get("value") for item in container.get("env", [])}


def test_driver_image_resolution_settings_reach_the_orchestrator():
    env = orchestrator_env(render())
    assert env["CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS"] == ""
    assert env["CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS"] == ""
    assert env["CONNECTOR_DRIVER_REGISTRY_TOKEN_HOSTS"] == ""
    assert env["CONNECTOR_DRIVER_RESOLVE_CACHE_SECONDS"] == "60"
    assert env["CONNECTOR_DRIVER_RESOLVE_TIMEOUT_SECONDS"] == "10"
    env = orchestrator_env(
        render(
            "connectors.drivers.registry.insecureHosts[0]=srw-registry:5000",
            "connectors.drivers.registry.insecureHosts[1]=other:5000",
            "connectors.drivers.registry.tokenHosts[0]=tokens.example",
            "connectors.drivers.registry.privateHosts[0]=srw-registry:5000",
        )
    )
    assert env["CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS"] == "srw-registry:5000"
    assert env["CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS"] == (
        "srw-registry:5000,other:5000"
    )
    assert env["CONNECTOR_DRIVER_REGISTRY_TOKEN_HOSTS"] == "tokens.example"


def test_egress_settings_reach_the_orchestrator():
    env = orchestrator_env(render())
    assert env["CONNECTOR_SERVICE_PODS_ENABLED"] == "false"
    assert env["CONNECTOR_SERVICE_ENFORCEMENT_VERIFIED"] == "false"
    assert env["CONNECTOR_SERVICE_CLUSTER_CIDRS"] == "10.42.0.0/16,10.43.0.0/16"
    assert env["CONNECTOR_SERVICE_PRIVATE_TIERS"] == "home-allowed"
    assert env["CONNECTOR_SERVICE_IPV6"] == "false"
    env = orchestrator_env(render(EXCHANGE, ON))
    assert env["CONNECTOR_SERVICE_PODS_ENABLED"] == "true"


def _orchestrator_env_items(docs: list[dict]) -> dict[str, dict]:
    deployment = next(
        doc
        for doc in docs
        if doc["kind"] == "Deployment"
        and doc["metadata"]["name"].endswith("-orchestrator")
    )
    container = next(
        c
        for c in deployment["spec"]["template"]["spec"]["containers"]
        if c["name"] == "orchestrator"
    )
    return {item["name"]: item for item in container.get("env", [])}


def test_refused_ranges_default_to_the_private_tiers_except_lists():
    """Driver pods of a home-allowed connector reach no more than that tier's
    workspaces: never the k3s nodes or the MetalLB range."""
    refused = orchestrator_env(render())["CONNECTOR_SERVICE_REFUSED_CIDRS"]
    ranges = refused.split(",")
    assert "10.0.50.0/24" in ranges  # k3s nodes: apiserver, kubelet, etcd
    assert "10.0.51.0/24" in ranges  # MetalLB
    assert "169.254.0.0/16" in ranges
    # home-allowed gives the home LAN back; internet-only's except is unused.
    assert "192.168.178.0/24" not in ranges
    assert len(ranges) == len(set(ranges))

    explicit = orchestrator_env(
        render("connectors.servicePods.refusedCidrs={10.9.0.0/24,192.0.2.0/24}")
    )
    assert explicit["CONNECTOR_SERVICE_REFUSED_CIDRS"] == "10.9.0.0/24,192.0.2.0/24"

    widened = orchestrator_env(
        render(
            "connectors.servicePods.privateTiers={home-allowed,internet-only}",
        )
    )["CONNECTOR_SERVICE_REFUSED_CIDRS"].split(",")
    assert "192.168.178.0/24" in widened


def test_the_orchestrator_knows_its_own_pod_address():
    item = _orchestrator_env_items(render(EXCHANGE, ON))["CONNECTOR_SERVICE_POD_IP"]
    assert item["valueFrom"] == {"fieldRef": {"fieldPath": "status.podIP"}}


DRIVER_RULE = {
    "to": [
        {
            "namespaceSelector": {
                "matchLabels": {"kubernetes.io/metadata.name": NAMESPACE}
            },
            "podSelector": {"matchLabels": {"srw.io/plane": "service"}},
        }
    ],
    "ports": [{"protocol": "TCP", "port": "srw-driver"}],
}


def _workspace_tiers(docs: list[dict]) -> list[dict]:
    return [
        doc
        for doc in docs
        if doc["kind"] == "NetworkPolicy"
        and "-workspace-policy-" in doc["metadata"]["name"]
    ]


def _agent_egress(docs: list[dict]) -> dict:
    return one(docs, "NetworkPolicy", "srw-superhuman-remote-worker-agent-egress")


def test_workspaces_and_agents_reach_only_the_driver_port_in_the_driver_namespace():
    docs = render(EXCHANGE, ON, "agent.networkPolicy.enabled=true")
    tiers = _workspace_tiers(docs)
    assert tiers
    for policy in tiers:
        rules = [r for r in policy["spec"]["egress"] if r.get("to")]
        assert DRIVER_RULE in rules
        # The only rule naming the driver namespace, and only the named port.
        into = [
            r
            for r in rules
            if any(
                peer.get("namespaceSelector", {})
                .get("matchLabels", {})
                .get("kubernetes.io/metadata.name")
                == NAMESPACE
                for peer in r["to"]
            )
        ]
        assert into == [DRIVER_RULE]
    assert DRIVER_RULE in _agent_egress(docs)["spec"]["egress"]


def test_no_rule_into_the_driver_namespace_without_hosting():
    docs = render("agent.networkPolicy.enabled=true")
    for policy in [*_workspace_tiers(docs), _agent_egress(docs)]:
        assert DRIVER_RULE not in policy["spec"]["egress"]
        assert NAMESPACE not in yaml.safe_dump(policy)


def test_the_driver_rule_follows_the_driver_namespace():
    docs = render(EXCHANGE, ON, "connectors.servicePods.namespace=hosted")
    (rule,) = [
        r
        for r in _workspace_tiers(docs)[0]["spec"]["egress"]
        if r.get("ports") == [{"protocol": "TCP", "port": "srw-driver"}]
    ]
    selector = rule["to"][0]["namespaceSelector"]["matchLabels"]
    assert selector == {"kubernetes.io/metadata.name": "hosted"}


def test_the_reconciler_settings_reach_the_orchestrator():
    import json

    env = orchestrator_env(render(EXCHANGE, ON))
    assert env["CONNECTOR_SERVICE_NAMESPACE"] == NAMESPACE
    assert env["CONNECTOR_SERVICE_RELEASE_NAMESPACE"] == "srw"
    assert env["CONNECTOR_SERVICE_MAX_INSTALLATION"] == "10"
    assert env["CONNECTOR_SERVICE_IDLE_SECONDS"] == "600"
    assert env["CONNECTOR_SERVICE_START_TIMEOUT_SECONDS"] == "180"
    assert env["CONNECTOR_SERVICE_RECONCILE_SECONDS"] == "15"
    assert env["CONNECTOR_SERVICE_EXCHANGE_HOST"] == (
        "srw-superhuman-remote-worker-orchestrator.srw.svc"
    )
    labels = json.loads(env["CONNECTOR_SERVICE_ORCHESTRATOR_LABELS"])
    assert labels == {
        "app.kubernetes.io/name": "superhuman-remote-worker",
        "app.kubernetes.io/instance": "srw",
        "app.kubernetes.io/component": "orchestrator",
    }
    # The exchange's Service and the orchestrator pods carry these names.
    docs = render(EXCHANGE, ON)
    service = one(docs, "Service", "srw-superhuman-remote-worker-orchestrator")
    assert service["spec"]["selector"] == labels
    resources = json.loads(env["CONNECTOR_SERVICE_RESOURCES"])
    assert resources["max"]["memory"] == "2Gi"
    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    assert example["connectors"]["servicePods"]["idleSeconds"] <= 120


def test_the_shim_image_reaches_the_orchestrator_pinned_when_a_digest_is_set():
    env = orchestrator_env(render())
    assert env["CONNECTOR_DRIVER_SHIM_IMAGE"] == (
        "ghcr.io/superhuman-remote-worker/srw-driver-shim:latest"
    )
    digest = "sha256:" + "a" * 64
    env = orchestrator_env(render(f"connectors.drivers.shim.image.digest={digest}"))
    assert env["CONNECTOR_DRIVER_SHIM_IMAGE"] == (
        f"ghcr.io/superhuman-remote-worker/srw-driver-shim@{digest}"
    )


def test_tilt_builds_the_shim_and_pins_it_by_digest():
    import ast

    tiltfile = (ROOT / "Tiltfile").read_text()
    tree = ast.parse(tiltfile)
    build = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "docker_build"
        and node.args
        and getattr(node.args[0], "value", None) == "srw-driver-shim"
    )
    keywords = {item.arg: item.value for item in build.keywords}
    assert ast.literal_eval(keywords["dockerfile"]) == "docker/Dockerfile.driver-shim"
    assert "drivers/shim/" in ast.literal_eval(keywords["only"])
    assert (
        "('srw-driver-shim', 'connectors.drivers.shim.image.repository', "
        "'connectors.drivers.shim.image.tag')" in tiltfile
    )
    assert "'srw-mcp', 'srw-vm-preparer', 'srw-driver-shim'" in tiltfile


ECHO = (
    "connectors.drivers.echo.enabled=true",
    "connectors.drivers.echo.image.repository=srw-registry:5000/srw-driver-echo",
    "connectors.drivers.echo.image.tag=tilt-1",
)


def test_the_echo_driver_is_off_by_default_and_follows_a_tag_or_pins_a_digest():
    assert "CONNECTOR_ECHO_DRIVER_IMAGE" not in orchestrator_env(render())
    env = orchestrator_env(render(EXCHANGE, ON, *ECHO))
    assert (
        env["CONNECTOR_ECHO_DRIVER_IMAGE"] == "srw-registry:5000/srw-driver-echo:tilt-1"
    )
    digest = "sha256:" + "b" * 64
    env = orchestrator_env(
        render(EXCHANGE, ON, *ECHO, f"connectors.drivers.echo.image.digest={digest}")
    )
    assert env["CONNECTOR_ECHO_DRIVER_IMAGE"] == (
        f"srw-registry:5000/srw-driver-echo:tilt-1@{digest}"
    )


@pytest.mark.parametrize(
    "settings",
    [
        ECHO,  # a service driver without service hosting
        (EXCHANGE, ON, "connectors.drivers.echo.enabled=true"),  # no image
    ],
)
def test_the_echo_driver_refuses_an_incomplete_setup(settings):
    with pytest.raises(subprocess.CalledProcessError):
        render(*settings)


def test_tilt_builds_the_echo_driver_for_the_dev_profile_only():
    import ast

    tiltfile = (ROOT / "Tiltfile").read_text()
    build = next(
        node
        for node in ast.walk(ast.parse(tiltfile))
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "docker_build"
        and node.args
        and getattr(node.args[0], "value", None) == "srw-driver-echo"
    )
    keywords = {item.arg: item.value for item in build.keywords}
    assert ast.literal_eval(keywords["dockerfile"]) == "docker/Dockerfile.driver-echo"
    assert "drivers/echo/" in ast.literal_eval(keywords["only"])
    assert (
        "('srw-driver-echo', 'connectors.drivers.echo.image.repository', "
        "'connectors.drivers.echo.image.tag')" in tiltfile
    )
    assert "'srw-driver-shim', 'srw-driver-echo']" in tiltfile
    # Never published: no CI workflow builds it.
    for workflow in (ROOT / ".github/workflows").glob("*.yml"):
        assert "driver-echo" not in workflow.read_text()
    example = yaml.safe_load(
        (ROOT / "deployment/values-local.yaml.example").read_text()
    )
    assert example["connectors"]["drivers"]["echo"]["enabled"] is True
    env = orchestrator_env(
        render(values=(ROOT / "deployment/values-local.yaml.example",))
    )
    assert env["CONNECTOR_ECHO_DRIVER_IMAGE"].startswith(
        "srw-registry:5000/srw-driver-echo:"
    )


def test_the_echo_image_declares_its_spec_label():
    import json

    from shared.connectors.builtin import ECHO_SERVICE_SPEC

    dockerfile = (ROOT / "docker/Dockerfile.driver-echo").read_text()
    raw = dockerfile.split("ARG SRW_DRIVER_SPEC='", 1)[1].split("'\n", 1)[0]
    label = json.loads(raw)
    assert "LABEL io.srw.driver.spec=$SRW_DRIVER_SPEC" in dockerfile
    assert label["name"] == ECHO_SERVICE_SPEC.name
    assert label["protocol_version"] == ECHO_SERVICE_SPEC.protocol_version
    assert label["config_schema"] == ECHO_SERVICE_SPEC.config_schema
    assert [slot["name"] for slot in label["credential_slots"]] == ["secret"]


def test_the_k3d_profile_resolves_from_the_k3d_registry_over_http():
    example = ROOT / "deployment/values-local.yaml.example"
    registry = yaml.safe_load(example.read_text())["connectors"]["drivers"]["registry"]
    assert registry["insecureHosts"] == ["srw-registry:5000"]
    # It sits on the k3d network: a private address, which only a listed
    # registry may resolve to.
    assert registry["privateHosts"] == ["srw-registry:5000"]
    assert registry["resolveCacheSeconds"] <= 10
