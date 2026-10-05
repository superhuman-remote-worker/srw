"""Task 11: renders the chart and asserts the ssh-gateway component's shape.

Every test here is a rendering test, so each one can only catch a mutation
that changes the *rendered manifest*. That is deliberate: the defects this
plan keeps producing are configuration defects -- an endpoint that reads an
environment variable nothing sets, a Secret mounted into the wrong pod, a
key list that exists in one place and not the other. `helm template` is the
only place those become visible before a cluster sees them.

What `helm template` does NOT prove is that Kubernetes accepts the result;
shape errors (bad `items`, a `defaultMode` of the wrong type, wrong nesting)
render happily and fail at apply. `kubectl apply --dry-run=server` covers
that and is run out-of-band -- see the task report.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"

# The mount points the chart owns. Asserted against rather than re-derived,
# so moving a mount without moving the environment variable that points at it
# fails here rather than at the gateway's first connection.
HOST_KEY_DIR = "/run/secrets/ssh-gateway/host"
CA_DIR = "/run/secrets/ssh-gateway/ca"

# The minimum an operator must say to turn the component on. Every one of
# these is required because `load_config` refuses to boot without the value
# it maps to; the chart's `fail` preamble surfaces that at render time
# instead of as a crash-loop. `helm template ... --set sshGateway.enabled=true`
# on its own is EXPECTED to fail -- see test_enabling_without_config_fails.
ENABLE = [
    "--set",
    "sshGateway.enabled=true",
    "--set",
    "sshGateway.hostname=ssh.example.com",
    "--set",
    "sshGateway.allowedOrigins={https://app.example.com}",
    "--set",
    "sshGateway.trustedProxies=10.42.0.0/16",
    "--set",
    "sshGateway.hostKeySecret=srw-ssh-gateway-hostkey",
    "--set",
    "sshGateway.userCaSecret=srw-ssh-gateway-ca",
    # SESSION_JWT_SECRET is the HMAC key the gateway verifies the attach
    # token with. Without a sessionRouter secret configured the chart renders
    # no Secret for the gateway's `optional: true` secretKeyRef to resolve,
    # and load_config refuses to boot -- so this belongs in the minimum,
    # exactly like allowedOrigins.
    "--set",
    "sessionRouter.jwtSecret=chart-test-not-a-real-key",
]

# The committed, test-enforced list of every route the ORCHESTRATOR serves
# (tests/test_endpoint_inventory.py keeps it honest against main.py). Used to
# prove the gateway Ingress does not steal one of them.
ENDPOINT_INVENTORY = ROOT / "policy" / "endpoint_inventory.txt"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)


# The chart refuses to render without these two regardless of this component
# (license-gate.yaml and services/opencloud.yaml). Nothing here is about the
# ssh-gateway; they are just the price of a render.
CHART_BASE = [
    "--set",
    "global.domain=example.com",
    "--set",
    "license.acceptTerms=true",
]


def _run(*extra: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["helm", "template", "srw", str(CHART), *CHART_BASE, *extra],
        capture_output=True,
        text=True,
    )


def _render(*extra: str) -> list[dict]:
    result = _run(*extra)
    assert result.returncode == 0, result.stderr
    return [d for d in yaml.safe_load_all(result.stdout) if d]


def _find(docs: list[dict], kind: str, name_contains: str) -> list[dict]:
    return [
        d
        for d in docs
        if d.get("kind") == kind and name_contains in d["metadata"]["name"]
    ]


def _one(docs: list[dict], kind: str, name_contains: str) -> dict:
    matches = _find(docs, kind, name_contains)
    assert len(matches) == 1, f"expected exactly one {kind}/{name_contains}"
    return matches[0]


def _container(deployment: dict, name: str) -> dict:
    containers = deployment["spec"]["template"]["spec"]["containers"]
    matches = [c for c in containers if c["name"] == name]
    assert len(matches) == 1, f"no container named {name}"
    return matches[0]


def _env(container: dict) -> dict[str, dict]:
    return {entry["name"]: entry for entry in container.get("env", [])}


def _volume(deployment: dict, name: str) -> dict:
    volumes = deployment["spec"]["template"]["spec"].get("volumes", [])
    matches = [v for v in volumes if v["name"] == name]
    assert len(matches) == 1, f"no volume named {name}"
    return matches[0]


def _orchestrator_routes() -> set[str]:
    """Every path the orchestrator serves, from the committed inventory."""
    routes = set()
    for line in ENDPOINT_INVENTORY.read_text().splitlines():
        if line.startswith("#") or not line.strip():
            continue
        parts = line.split()
        if len(parts) >= 2 and parts[0].isupper() and parts[1].startswith("/"):
            routes.add(parts[1])
    return routes


@pytest.fixture(scope="module")
def docs() -> list[dict]:
    return _render(*ENABLE)


@pytest.fixture(scope="module")
def default_docs() -> list[dict]:
    return _render()


@pytest.fixture(scope="module")
def gateway(docs: list[dict]) -> dict:
    return _one(docs, "Deployment", "ssh-gateway")


@pytest.fixture(scope="module")
def orchestrator(docs: list[dict]) -> dict:
    return _one(docs, "Deployment", "-orchestrator")


# ---------------------------------------------------------------------------
# Default-off (correction 3)
# ---------------------------------------------------------------------------


def test_component_is_off_by_default(default_docs: list[dict]) -> None:
    """Correction 3: the brief's own version of this test asserted the string
    "ssh-gateway" is absent from the whole default render, which cannot hold
    -- the workspace NetworkPolicy names the component in a comment, and the
    values file names it too. Assert on rendered Kinds and names instead.
    """
    named = [
        f"{d['kind']}/{d['metadata']['name']}"
        for d in default_docs
        if "ssh-gateway" in d["metadata"]["name"]
    ]
    assert named == []


def test_default_orchestrator_is_untouched(default_docs: list[dict]) -> None:
    """The correction-4 wiring lives in an EXISTING Deployment. Leaving it
    ungated would put a mount for a Secret that does not exist into every
    install, which fails the orchestrator pod, not the gateway.
    """
    orchestrator = _one(default_docs, "Deployment", "-orchestrator")
    container = _container(orchestrator, "orchestrator")
    assert "SSH_GATEWAY_PUBLIC_HOST_KEYS" not in _env(container)
    assert "SSH_GATEWAY_HOSTNAME" not in _env(container)
    assert "AGENT_SSH_GATEWAY_HOST_KEY_SECRET" not in _env(container)
    volumes = orchestrator["spec"]["template"]["spec"].get("volumes", [])
    assert not [v for v in volumes if "ssh-gateway" in v["name"]]


def test_workspace_policy_does_not_admit_a_gateway_that_is_off(
    default_docs: list[dict],
) -> None:
    policies = _find(default_docs, "NetworkPolicy", "workspace-policy")
    assert policies
    for policy in policies:
        for rule in policy["spec"]["ingress"]:
            for source in rule.get("from", []):
                labels = source.get("podSelector", {}).get("matchLabels", {})
                assert labels.get("app.kubernetes.io/component") != "ssh-gateway"


# ---------------------------------------------------------------------------
# The gateway pod
# ---------------------------------------------------------------------------


def test_reuses_the_orchestrator_image(gateway: dict, orchestrator: dict) -> None:
    container = _container(gateway, "ssh-gateway")
    relay = _container(orchestrator, "orchestrator")
    assert "orchestrator" in container["image"]
    assert container["image"] == relay["image"]
    assert container["imagePullPolicy"] == relay["imagePullPolicy"]
    assert container["command"][0] == "uvicorn"
    assert "orchestrator.ssh_gateway:create_app" in container["command"]
    assert "--factory" in container["command"]


def test_gateway_can_stay_on_its_old_digest_during_relay_upgrade() -> None:
    old_digest = "sha256:" + "a" * 64
    new_digest = "sha256:" + "b" * 64
    docs = _render(
        *ENABLE,
        "--set",
        f"image.orchestrator.digest={new_digest}",
        "--set",
        "sshGateway.image.repository=ghcr.io/superhuman-remote-worker/srw-orchestrator",
        "--set",
        f"sshGateway.image.digest={old_digest}",
        "--set",
        "sshGateway.image.pullPolicy=IfNotPresent",
    )
    gateway = _container(_one(docs, "Deployment", "ssh-gateway"), "ssh-gateway")
    relay = _container(_one(docs, "Deployment", "-orchestrator"), "orchestrator")
    assert gateway["image"] == (
        "ghcr.io/superhuman-remote-worker/srw-orchestrator@" + old_digest
    )
    assert relay["image"] == (
        "ghcr.io/superhuman-remote-worker/srw-orchestrator@" + new_digest
    )
    assert gateway["imagePullPolicy"] == "IfNotPresent"


@pytest.mark.parametrize(
    "override",
    [
        "sshGateway.image.tag=old",
        "sshGateway.image.repository=ghcr.io/superhuman-remote-worker/srw-orchestrator",
        "sshGateway.image=old",
    ],
)
def test_incomplete_gateway_image_override_is_rejected(override: str) -> None:
    result = _run(*ENABLE, "--set", override)
    assert result.returncode != 0
    assert "sshGateway.image" in result.stderr


def test_runs_non_root_with_readonly_rootfs(gateway: dict) -> None:
    pod = gateway["spec"]["template"]["spec"]
    container = _container(gateway, "ssh-gateway")
    assert pod["securityContext"]["runAsNonRoot"] is True
    assert pod["securityContext"]["runAsUser"] == 999
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    assert container["securityContext"]["allowPrivilegeEscalation"] is False
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]


def test_does_not_mount_the_fleet_wide_key(gateway: dict) -> None:
    """D7: the gateway holds a CA. Mounting VM_SSH_PRIVATE_KEY here would
    reinstate exactly the blast radius the CA exists to remove."""
    rendered = yaml.safe_dump(gateway)
    assert "vm-ssh-key" not in rendered
    assert "ssh-privatekey" not in rendered


def test_secret_volumes_are_readable_by_the_non_root_user(gateway: dict) -> None:
    """Correction 1: 0400 on a root:root projected Secret is unreadable by
    uid 999 and crash-loops the pod. The repo answers this the same way in
    three other places (agent, canvas-gateway, orchestrator): 0444.
    """
    for name in ("ssh-gateway-host-keys", "ssh-gateway-user-ca"):
        mode = _volume(gateway, name)["secret"]["defaultMode"]
        assert mode == 0o444, f"{name} defaultMode {oct(mode)} is not 0444"


def test_gateway_mounts_only_the_private_host_keys_and_the_ca(gateway: dict) -> None:
    container = _container(gateway, "ssh-gateway")
    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts["ssh-gateway-host-keys"]["mountPath"] == HOST_KEY_DIR
    assert mounts["ssh-gateway-host-keys"]["readOnly"] is True
    assert mounts["ssh-gateway-user-ca"]["mountPath"] == CA_DIR
    assert mounts["ssh-gateway-user-ca"]["readOnly"] is True

    host_keys = _volume(gateway, "ssh-gateway-host-keys")["secret"]
    assert host_keys["secretName"] == "srw-ssh-gateway-hostkey"
    # The private halves, and nothing else: the gateway has no use for the
    # .pub files and projecting them would invite the "which copy is
    # authoritative" drift correction 5 is about.
    assert [item["key"] for item in host_keys["items"]] == ["ssh_host_ed25519_key"]

    user_ca = _volume(gateway, "ssh-gateway-user-ca")["secret"]
    assert user_ca["secretName"] == "srw-ssh-gateway-ca"
    assert [item["key"] for item in user_ca["items"]] == ["user-ca"]


def test_gateway_receives_the_session_jwt_secret(
    gateway: dict, orchestrator: dict
) -> None:
    """Correction 2b: `load_config` fails closed without SESSION_JWT_SECRET
    now that the attach token is a stateless HMAC over it. It must resolve
    the SAME Secret and key the orchestrator mints with, or every token is
    refused with nothing in either log to say why -- so this asserts against
    the orchestrator's own reference rather than a literal name, which would
    pass while the two drifted apart.
    """
    env = _env(_container(gateway, "ssh-gateway"))
    minted_with = _env(_container(orchestrator, "orchestrator"))["SESSION_JWT_SECRET"]
    assert (
        env["SESSION_JWT_SECRET"]["valueFrom"]["secretKeyRef"]
        == minted_with["valueFrom"]["secretKeyRef"]
    )
    assert env["MCP_INTERNAL_KEY"]["valueFrom"]["secretKeyRef"]["key"] == (
        "MCP_INTERNAL_KEY"
    )


def test_gateway_config_points_at_what_is_actually_mounted(docs: list[dict]) -> None:
    config = _one(docs, "ConfigMap", "ssh-gateway")["data"]
    assert config["SSH_GATEWAY_HOST_KEYS"] == f"{HOST_KEY_DIR}/ssh_host_ed25519_key"
    assert config["SSH_GATEWAY_USER_CA"] == f"{CA_DIR}/user-ca"
    assert config["SSH_GATEWAY_ALLOWED_ORIGINS"] == "https://app.example.com"


def test_trusted_proxies_reaches_the_gateway(docs: list[dict]) -> None:
    """Correction 2b: unset, every WSS client presents the ingress's address,
    so all of them share one source's 16-slot bucket and the seventeenth
    concurrent user is refused -- an outage shaped exactly like this plan's
    code leaks, arriving through configuration instead.
    """
    config = _one(docs, "ConfigMap", "ssh-gateway")["data"]
    assert config["SSH_GATEWAY_TRUSTED_PROXIES"] == "10.42.0.0/16"


def test_configmap_carries_no_setting_the_gateway_never_reads(
    docs: list[dict],
) -> None:
    """The signature defect of this plan, generalized, in BOTH directions.
    `load_config` is the gateway's only environment reader, so:

    * nothing may be rendered that it never reads -- dead config an operator
      can tune forever with no effect; and
    * nothing it requires may be missing -- which is the defect class this
      plan actually keeps producing.

    The second half is not symmetric bookkeeping. `ORCHESTRATOR_URL` has a
    default, `"http://orchestrator:8085"`, and that default is WRONG for this
    chart, which renders `srw-<release>-orchestrator`. Delete the ConfigMap
    line and the gateway boots perfectly happily; every attach then dies at
    DNS resolution. Silent, not fail-closed -- the worst shape available.

    Only the three settings whose defaults are genuinely correct here are
    exempt: SSH_GATEWAY_REQUIRE_TOKEN (defaults true, and a chart value to
    turn the user credential off would be a footgun), SSH_GATEWAY_SSH_HOST and
    SSH_GATEWAY_SSH_PORT.

    (This is also why there is no `sshGateway.limits` block: `load_config`
    reads none of the five caps -- see the task report.)
    """
    source = (ROOT / "src/orchestrator/services/ssh_gateway_config.py").read_text()
    read_by_load_config = set(re.findall(r'src\.get\(\s*"([A-Z_0-9]+)"', source))
    assert "SSH_GATEWAY_HOST_KEYS" in read_by_load_config  # regex sanity
    assert "ORCHESTRATOR_URL" in read_by_load_config
    assert "SESSION_JWT_SECRET" in read_by_load_config

    # Every key, not just the SSH_GATEWAY_* ones: LOG_LEVEL used to ride here
    # reading nothing, because ssh_gateway.py never calls configure_logging.
    config_keys = set(_one(docs, "ConfigMap", "ssh-gateway")["data"])
    assert config_keys <= read_by_load_config, sorted(config_keys - read_by_load_config)

    # The inverse. A required variable may arrive either through the
    # ConfigMap (`envFrom`) or as an explicit `env:` entry on the container --
    # the two Secret-backed ones take the second route.
    supplied = config_keys | set(
        _env(_container(_one(docs, "Deployment", "ssh-gateway"), "ssh-gateway"))
    )
    required = read_by_load_config - {
        "SSH_GATEWAY_REQUIRE_TOKEN",
        "SSH_GATEWAY_SSH_HOST",
        "SSH_GATEWAY_SSH_PORT",
    }
    assert required <= supplied, sorted(required - supplied)


# ---------------------------------------------------------------------------
# Correction 4: the host-key publication endpoint runs in the ORCHESTRATOR
# ---------------------------------------------------------------------------


def test_orchestrator_can_actually_serve_the_host_key_document(
    orchestrator: dict,
) -> None:
    """Correction 4: as briefed, GET /api/ssh/host-keys returned
    `{"host_keys": [], "hostname": ""}` forever -- the endpoint runs in the
    orchestrator pod, which mounted nothing from the gateway's host-key
    Secret, and the Secret carried no .pub halves to point at. A pinning
    client silently degrades to trust-on-first-use.
    """
    container = _container(orchestrator, "orchestrator")
    env = _env(container)

    # (c) explicit `env:` entries -- the orchestrator does NOT use envFrom,
    # so a ConfigMap key would never arrive.
    assert env["SSH_GATEWAY_HOSTNAME"]["value"] == "ssh.example.com"
    published = env["SSH_GATEWAY_PUBLIC_HOST_KEYS"]["value"]
    assert published == f"{HOST_KEY_DIR}/ssh_host_ed25519_key.pub"

    # (b) the Secret is mounted read-only into THIS pod, at the path the
    # variable names.
    mounts = {m["name"]: m for m in container["volumeMounts"]}
    assert mounts["ssh-gateway-host-keys"]["mountPath"] == HOST_KEY_DIR
    assert mounts["ssh-gateway-host-keys"]["readOnly"] is True

    # (a) .pub entries exist to point at.
    volume = _volume(orchestrator, "ssh-gateway-host-keys")["secret"]
    assert volume["secretName"] == "srw-ssh-gateway-hostkey"
    assert [item["key"] for item in volume["items"]] == ["ssh_host_ed25519_key.pub"]

    for path in published.split(","):
        assert path.startswith(f"{HOST_KEY_DIR}/")


def test_orchestrator_never_receives_a_private_host_key(orchestrator: dict) -> None:
    """Correction 4 rejects pointing SSH_GATEWAY_PUBLIC_HOST_KEYS at the
    private key files: it would work (import_public_key emits only public
    material) but it puts the gateway's host private keys in a second pod
    for no benefit. `items:` is what keeps them out.
    """
    volume = _volume(orchestrator, "ssh-gateway-host-keys")["secret"]
    for item in volume["items"]:
        assert item["key"].endswith(".pub"), item["key"]
        assert item["path"].endswith(".pub"), item["path"]

    env = _env(_container(orchestrator, "orchestrator"))
    for path in env["SSH_GATEWAY_PUBLIC_HOST_KEYS"]["value"].split(","):
        assert path.endswith(".pub"), path


def test_host_key_env_is_appended_after_the_existing_orchestrator_env(
    orchestrator: dict,
) -> None:
    """Inserting entries into the MIDDLE of an existing Deployment's env list
    can trigger the Kubernetes strategic-merge bug that produces
    `env[N].valueFrom` patch errors and needs a delete+recreate of the
    resource (memory: helm_env_reorder_strategic_merge_bug). Appending at the
    end leaves every pre-existing index where it was.
    """
    names = [entry["name"] for entry in _container(orchestrator, "orchestrator")["env"]]
    assert names[-5:] == [
        "SSH_GATEWAY_PUBLIC_HOST_KEYS",
        "SSH_GATEWAY_HOSTNAME",
        "WORKSPACE_BUILTIN_TEMPLATES",
        "WORKSPACE_DEFAULTS",
        "AGENT_SSH_GATEWAY_HOST_KEY_SECRET",
    ]


# ---------------------------------------------------------------------------
# Correction 5: one value drives both sides
# ---------------------------------------------------------------------------


def test_published_and_served_host_keys_cannot_drift(docs: list[dict]) -> None:
    """Correction 5: the gateway reads SSH_GATEWAY_HOST_KEYS (private paths,
    gateway pod) and the endpoint reads SSH_GATEWAY_PUBLIC_HOST_KEYS (public
    paths, orchestrator pod). Different variable, pod, Deployment and file
    set. When they drift, clients see a host-key mismatch INDISTINGUISHABLE
    from an active MITM, so the two must be rendered from one chart value.
    """
    served = _one(docs, "ConfigMap", "ssh-gateway")["data"][
        "SSH_GATEWAY_HOST_KEYS"
    ].split(",")
    orchestrator = _one(docs, "Deployment", "-orchestrator")
    published = _env(_container(orchestrator, "orchestrator"))[
        "SSH_GATEWAY_PUBLIC_HOST_KEYS"
    ]["value"].split(",")

    assert len(served) == len(published)
    assert [os.path.basename(p) for p in served] == [
        os.path.basename(p).removesuffix(".pub") for p in published
    ]


def test_a_second_host_key_lands_on_both_sides() -> None:
    """The single-value claim above holds trivially for a one-element list.
    Add a second name and both variables must grow together -- this is what
    catches a re-derivation that hardcodes one side.
    """
    docs = _render(
        *ENABLE,
        "--set",
        "sshGateway.hostKeyNames={ssh_host_ed25519_key,ssh_host_ed25519_key_b}",
    )
    served = _one(docs, "ConfigMap", "ssh-gateway")["data"][
        "SSH_GATEWAY_HOST_KEYS"
    ].split(",")
    orchestrator = _one(docs, "Deployment", "-orchestrator")
    published = _env(_container(orchestrator, "orchestrator"))[
        "SSH_GATEWAY_PUBLIC_HOST_KEYS"
    ]["value"].split(",")

    assert len(served) == 2
    assert len(published) == 2
    assert [os.path.basename(p) for p in served] == [
        os.path.basename(p).removesuffix(".pub") for p in published
    ]
    # ...and the mounted item lists too, in both pods.
    gateway = _one(docs, "Deployment", "ssh-gateway")
    assert len(_volume(gateway, "ssh-gateway-host-keys")["secret"]["items"]) == 2
    assert len(_volume(orchestrator, "ssh-gateway-host-keys")["secret"]["items"]) == 2


# ---------------------------------------------------------------------------
# Correction 6: Ed25519 only
# ---------------------------------------------------------------------------


def test_default_host_key_name_is_ed25519() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["sshGateway"]["hostKeyNames"] == ["ssh_host_ed25519_key"]


def test_an_rsa_host_key_name_is_refused_at_render_time() -> None:
    """Correction 6: `_require_ed25519_host_key` raises on any algorithm that
    is not ssh-ed25519, so an `ssh_host_rsa_key` in the Secret means the
    gateway refuses to start -- a crash-loop whose cause is three files away
    from the values change that caused it. The chart names the mistake at
    render time instead. This is a naming-convention tripwire, not a
    cryptographic check; the real enforcement stays in ssh_gateway_config.
    """
    result = _run(*ENABLE, "--set", "sshGateway.hostKeyNames={ssh_host_rsa_key}")
    assert result.returncode != 0
    assert "Ed25519" in result.stderr
    assert "ssh_host_rsa_key" in result.stderr


def test_no_rsa_host_key_survives_anywhere_in_the_chart() -> None:
    for name in ("values.yaml", "values.example.yaml"):
        assert "ssh_host_rsa_key" not in (CHART / name).read_text()


# ---------------------------------------------------------------------------
# Correction 1b: user-ca.pub must actually reach workspaces
# ---------------------------------------------------------------------------


def _vm_ssh_key_external_secret(*extra: str) -> dict:
    docs = _render(
        *ENABLE,
        "--set",
        "externalSecrets.enabled=true",
        "--set",
        "externalSecrets.vaultPath=secret/data/srw",
        *extra,
    )
    return _one(docs, "ExternalSecret", "-vm-ssh-key")


def test_layout_b_supplies_the_user_ca_to_the_workspace_secret() -> None:
    """Correction 1b (CRITICAL): the provisioner projects `user-ca.pub` out of
    the `vm-ssh-key` Secret (container_provisioner.py:12604), NOT out of
    sshGateway.userCaSecret. As briefed nothing ever put that key there, so
    the pod started (`optional: True`), the entrypoint skipped the write, and
    every attach ended in PermissionDenied -- fail-closed, but the whole
    workspace-CA feature inert.
    """
    keys = [
        entry["secretKey"]
        for entry in _vm_ssh_key_external_secret().get("spec", {}).get("data", [])
    ]
    assert "user-ca.pub" in keys
    assert "ssh-privatekey" in keys
    assert "ssh-publickey" in keys


def test_layout_a_supplies_the_user_ca_alongside_its_bundle() -> None:
    """Layout A pulls a WHOLE bundle with `dataFrom: extract`, which has no
    item to add. ESO permits `data` alongside `dataFrom` (data wins on
    conflict), so the CA key arrives as an explicit entry.
    """
    external_secret = _vm_ssh_key_external_secret(
        "--set", "externalSecrets.vmSshKeyVaultPath=secret/data/srw-vm-ssh-key"
    )
    spec = external_secret["spec"]
    assert spec["dataFrom"], "layout A must keep pulling the operator's bundle"
    assert [entry["secretKey"] for entry in spec["data"]] == ["user-ca.pub"]


def test_the_user_ca_entry_is_absent_when_the_gateway_is_off() -> None:
    """An ESO `data` entry whose vault property does not exist fails the whole
    ExternalSecret sync, which would break `vm-ssh-key` -- the key the entire
    platform reaches every workspace with -- for every install that never
    asked for an ssh-gateway.
    """
    docs = _render(
        "--set",
        "externalSecrets.enabled=true",
        "--set",
        "externalSecrets.vaultPath=secret/data/srw",
    )
    spec = _one(docs, "ExternalSecret", "-vm-ssh-key")["spec"]
    keys = [entry["secretKey"] for entry in spec.get("data", [])]
    assert "user-ca.pub" not in keys


# ---------------------------------------------------------------------------
# Doors: ingress, service, network policy
# ---------------------------------------------------------------------------


def test_ingress_pins_an_explicit_router_priority(docs: list[dict]) -> None:
    ingress = _one(docs, "Ingress", "ssh-gateway")
    annotations = ingress["metadata"]["annotations"]
    assert annotations["traefik.ingress.kubernetes.io/router.priority"] == "130"


def test_ingress_routes_only_the_attach_socket_to_the_gateway(
    docs: list[dict],
) -> None:
    """`/api/ssh/attach` is served by the gateway on 8087; `/api/ssh/host-keys`
    is served by the ORCHESTRATOR on 8085. A `/api/ssh` prefix here would
    swallow the second and 404 every pinning client.
    """
    ingress = _one(docs, "Ingress", "ssh-gateway")
    paths = ingress["spec"]["rules"][0]["http"]["paths"]
    assert [p["path"] for p in paths] == ["/api/ssh/attach"]
    assert paths[0]["backend"]["service"]["port"]["number"] == 8087


def test_ingress_steals_no_orchestrator_route(docs: list[dict]) -> None:
    """C1: `pathType: Prefix` on `/api/ssh/attach` swallowed
    `POST /api/ssh/attach-token` -- the endpoint that MINTS the token this
    door verifies. Traefik renders Prefix as `PathPrefix()`, a RAW STRING
    prefix rather than a path-element match, and the explicit
    `router.priority: 130` beats the api ingress's length-derived default. So
    every token request reached a gateway that serves two routes and got a
    Starlette 404: no token, no attach, feature dead, and invisible to any
    test that only reads this one manifest.

    Modelled on Traefik's actual matchers -- Exact -> `Path()`,
    Prefix/ImplementationSpecific -> `PathPrefix()` -- against the committed
    orchestrator inventory. Catches both the regression (someone restores
    `Prefix`) and the next `/api/ssh/attach-*` endpoint anyone adds.
    """
    orchestrator_routes = _orchestrator_routes()
    # Regression detector, not a shape echo: if the inventory ever stops
    # carrying the route that caused this bug, the test must say so rather
    # than pass on an empty set.
    assert "/api/ssh/attach-token" in orchestrator_routes
    assert "/api/ssh/host-keys" in orchestrator_routes

    paths = _one(docs, "Ingress", "ssh-gateway")["spec"]["rules"][0]["http"]["paths"]
    stolen: dict[str, list[str]] = {}
    for entry in paths:
        claimed, kind = entry["path"], entry["pathType"]
        if kind == "Exact":
            captured = [r for r in orchestrator_routes if r == claimed]
        else:  # Prefix and ImplementationSpecific both become PathPrefix()
            captured = [r for r in orchestrator_routes if r.startswith(claimed)]
        if captured:
            stolen[f"{claimed} ({kind})"] = sorted(captured)

    assert not stolen, (
        "the ssh-gateway Ingress captures orchestrator routes, which reach a "
        f"gateway that serves only /api/ssh/attach and /healthz: {stolen}"
    )


def test_ingress_host_follows_cockpits_actual_apiurl(docs: list[dict]) -> None:
    """task-7 live gate, controller correction C1: cockpit's own generated
    `env.js` picks its `apiUrl` via a `sameOriginApi` ternary
    (`srw.cockpitFacingApiUrl` -- cockpit/deployment.yaml), but this Ingress
    used to be pinned to the bare `srw.apiUrl` host regardless of that flag.
    With `sameOriginApi: true` the two disagreed: cockpit dialled the
    cockpit host and this Ingress sat on the api host, so the generated
    `ProxyCommand`'s WSS attach landed on the orchestrator's own ASGI app
    (no route there) and came back a bare 403 with NOTHING in the
    ssh-gateway pod's logs -- indistinguishable from a token/auth failure
    without checking the gateway's logs and finding them empty. Both
    settings must render on the SAME host cockpit's own apiUrl would use, or
    this regresses silently (a rendering test can't see a 403 that never
    reaches this file).
    """
    same_origin_off = _render(*ENABLE, "--set", "auth.bff.sameOriginApi=false")
    same_origin_on = _render(*ENABLE, "--set", "auth.bff.sameOriginApi=true")

    host_off = _one(same_origin_off, "Ingress", "ssh-gateway")["spec"]["rules"][0][
        "host"
    ]
    host_on = _one(same_origin_on, "Ingress", "ssh-gateway")["spec"]["rules"][0]["host"]

    assert host_off == "api.example.com"
    assert host_on == "example.com"
    # And the TLS secret follows the same host, or cert-manager issues a
    # redundant cert for it under the wrong Ingress's name.
    assert _one(same_origin_off, "Ingress", "ssh-gateway")["spec"]["tls"][0][
        "secretName"
    ].endswith("-api-tls")
    assert _one(same_origin_on, "Ingress", "ssh-gateway")["spec"]["tls"][0][
        "secretName"
    ].endswith("-cockpit-tls")


def test_cockpit_env_js_apiurl_host_matches_the_gateway_ingress(
    docs: list[dict],
) -> None:
    """The other half of the C1 regression: prove the two templates that
    independently compute "cockpit's API host" (cockpit/deployment.yaml's
    `env.js` and ssh-gateway/ingress.yaml's Ingress host) can no longer
    drift apart, across both settings of the flag that caused them to.
    """
    import re

    for override in ("false", "true"):
        rendered = _render(*ENABLE, "--set", f"auth.bff.sameOriginApi={override}")
        ingress_host = _one(rendered, "Ingress", "ssh-gateway")["spec"]["rules"][0][
            "host"
        ]
        configmap = _one(rendered, "ConfigMap", "cockpit-env")
        env_js = configmap["data"]["env.js"]
        match = re.search(r"apiUrl'\] = '([^']+)'", env_js)
        assert match, f"could not find apiUrl assignment in env.js:\n{env_js}"
        api_url_host = match.group(1).split("://", 1)[1].split("/", 1)[0]
        assert ingress_host == api_url_host, (
            f"sameOriginApi={override}: ssh-gateway Ingress host "
            f"({ingress_host!r}) != cockpit's own env.js apiUrl host "
            f"({api_url_host!r}) -- a generated ProxyCommand would dial the "
            "wrong host and get a bare 403 with nothing in the gateway's logs"
        )


def test_cors_middleware_allows_the_service_worker_bypass_header(
    default_docs: list[dict],
) -> None:
    """Not ssh-gateway-specific, but found live while running this exact
    plan's task-7 gate: `auth.interceptor.ts` sets `ngsw-bypass: 1` on every
    non-safe (mutating) request to the orchestrator (bd31e072), and the
    Traefik CORS middleware's `accessControlAllowHeaders` never grew a
    matching entry. Any cross-origin cockpit/api deployment -- which is the
    chart's own DEFAULT (`auth.bff.sameOriginApi: false`), not a corner case
    -- therefore had every POST/PUT/PATCH/DELETE cockpit request (including
    `POST /api/ssh-keys/challenge`, the first step of registering an SSH
    key) silently rejected by the browser's CORS preflight before it ever
    reached the orchestrator. Nothing server-side logs a CORS rejection, so
    there is no signal of this anywhere but the browser console -- this
    rendering test is the only thing that catches a future removal of the
    header from either side without a live browser pass.
    """
    middleware = _one(default_docs, "Middleware", "cors")
    allow_headers = middleware["spec"]["headers"]["accessControlAllowHeaders"]
    assert "ngsw-bypass" in allow_headers


def test_tcp_listener_is_off_by_default(docs: list[dict]) -> None:
    services = _find(docs, "Service", "ssh-gateway")
    assert services
    assert all(s["spec"]["type"] == "ClusterIP" for s in services)


def test_cluster_service_exposes_ssh_for_port_forwarding(docs: list[dict]) -> None:
    """The raw SSH listener runs unconditionally -- /healthz reports 503 while
    it is down -- so the ClusterIP Service carries 2222 whether or not the
    LoadBalancer door is open. Task 12's gate is a port-forward at that name.
    """
    service = _one(docs, "Service", "ssh-gateway")
    ports = {p["name"]: p["port"] for p in service["spec"]["ports"]}
    assert ports == {"http": 8087, "ssh": 2222}


def test_tcp_listener_requires_cidr_scoping() -> None:
    result = _run(*ENABLE, "--set", "sshGateway.tcp.enabled=true")
    assert result.returncode != 0
    # Assert on the reason, not just the failure: an unrelated render error
    # would otherwise let this pass while proving nothing.
    assert "allowedClientCIDRs" in result.stderr


def test_privileged_tcp_port_is_refused() -> None:
    """The pod runs as uid 999 with every capability dropped, so a port below
    1024 never binds. The listener is not optional -- /healthz answers 503
    while its accept loop is down -- so the pod would simply never go Ready,
    with the cause a `bind: permission denied` deep in the log.
    """
    for port in ("22", "70000"):
        result = _run(*ENABLE, "--set", f"sshGateway.tcp.port={port}")
        assert result.returncode != 0, port
        assert "tcp.port" in result.stderr, port


def test_tcp_loadbalancer_is_scoped_when_enabled() -> None:
    docs = _render(
        *ENABLE,
        "--set",
        "sshGateway.tcp.enabled=true",
        "--set",
        "sshGateway.tcp.allowedClientCIDRs={192.168.1.0/24}",
    )
    balancers = [
        s
        for s in _find(docs, "Service", "ssh-gateway")
        if s["spec"]["type"] == "LoadBalancer"
    ]
    assert len(balancers) == 1
    assert balancers[0]["spec"]["loadBalancerSourceRanges"] == ["192.168.1.0/24"]
    assert (
        balancers[0]["metadata"]["annotations"]["svccontroller.k3s.cattle.io/enablelb"]
        == "false"
    )


def test_enabling_without_config_fails_closed() -> None:
    """`load_config` refuses to boot without an origin allow-list, a CA, host
    keys or a trusted-proxy declaration. The chart says so at render time
    rather than letting the operator discover it as a crash-loop.
    """
    for setting, expected in (
        ("sshGateway.allowedOrigins=null", "allowedOrigins"),
        ("sshGateway.hostKeySecret=", "hostKeySecret"),
        ("sshGateway.userCaSecret=", "userCaSecret"),
        ("sshGateway.trustedProxies=", "trustedProxies"),
        # C2: with neither sessionRouter value set the chart renders no Secret
        # at all, the gateway's `optional: true` secretKeyRef resolves to
        # nothing, and load_config raises on SESSION_JWT_SECRET -- a
        # CrashLoopBackOff three files away from the values file. Mirrors
        # collabora/network-policy.yaml's identical precondition.
        ("sessionRouter.jwtSecret=", "sessionRouter"),
    ):
        result = _run(*ENABLE, "--set", setting)
        assert result.returncode != 0, setting
        assert expected in result.stderr, setting


def test_the_session_jwt_secret_the_gateway_names_actually_exists(
    docs: list[dict],
) -> None:
    """C2, the supply side: the render-time `fail` proves the operator SAID
    something, not that a Secret arrives. Layout A (`sessionRouter.jwtSecret`)
    must actually render the Secret the gateway's secretKeyRef names -- the
    reference is `optional: true`, so a name that resolves to nothing is a
    silent unset variable, not a scheduling error.

    Asserted against the reference the Deployment renders rather than a
    literal, so a change to jwtSecretName's default cannot drift them apart.
    """
    ref = _env(_container(_one(docs, "Deployment", "ssh-gateway"), "ssh-gateway"))[
        "SESSION_JWT_SECRET"
    ]["valueFrom"]["secretKeyRef"]
    secret = _one(docs, "Secret", ref["name"])
    assert ref["key"] in (secret.get("stringData") or secret.get("data") or {})


def test_gateway_network_policy_scopes_both_directions(docs: list[dict]) -> None:
    policy = _one(docs, "NetworkPolicy", "ssh-gateway")
    ingress_ports = {
        port["port"] for rule in policy["spec"]["ingress"] for port in rule["ports"]
    }
    assert 8087 in ingress_ports

    egress_ports = {
        port["port"] for rule in policy["spec"]["egress"] for port in rule["ports"]
    }
    # Orchestrator (target resolution + audit), workspace sshd, DNS.
    assert {8085, 30022, 53} <= egress_ports
    # The gateway has no reason to reach a database, and D7 says it must not
    # be able to reach the fleet-wide key's blast radius either.
    assert 5432 not in egress_ports


def test_workspace_policy_admits_the_gateway(docs: list[dict]) -> None:
    policies = _find(docs, "NetworkPolicy", "workspace-policy")
    assert policies
    for policy in policies:
        admitted = [
            rule
            for rule in policy["spec"]["ingress"]
            for source in rule.get("from", [])
            if source.get("podSelector", {})
            .get("matchLabels", {})
            .get("app.kubernetes.io/component")
            == "ssh-gateway"
        ]
        assert admitted, f"{policy['metadata']['name']} does not admit the gateway"
        ports = {port["port"] for rule in admitted for port in rule["ports"]}
        # This fixture uses VM mode off. VM mode is covered separately by
        # the paired gateway-egress/workspace-ingress regression below.
        assert ports == {30022}


@pytest.mark.parametrize(
    ("setting", "expected_ports"),
    [
        ("vm.mode=off", {30022}),
        ("vm.mode=same-cluster", {22, 30022}),
        ("vmController.enabled=true", {22, 30022}),
    ],
)
def test_gateway_workspace_ssh_policy_is_bidirectional(
    setting: str,
    expected_ports: set[int],
) -> None:
    """A real VM inner hop needs gateway egress as well as workspace ingress."""
    documents = _render(
        *ENABLE,
        "--set",
        setting,
        "--set",
        "vm.lifecycleAuthSecretName=vm-lifecycle-test",
    )
    gateway = _one(documents, "NetworkPolicy", "ssh-gateway")
    workspace_rules = [
        rule
        for rule in gateway["spec"]["egress"]
        if rule.get("to")
        == [{"podSelector": {"matchLabels": {"srw.io/component": "agent-workspace"}}}]
    ]
    assert len(workspace_rules) == 1
    assert {p["port"] for p in workspace_rules[0]["ports"]} == expected_ports
    for policy in _find(documents, "NetworkPolicy", "workspace-policy"):
        admitted = [
            rule
            for rule in policy["spec"]["ingress"]
            if any(
                source.get("podSelector", {})
                .get("matchLabels", {})
                .get("app.kubernetes.io/component")
                == "ssh-gateway"
                for source in rule.get("from", [])
            )
        ]
        assert {p["port"] for rule in admitted for p in rule["ports"]} == expected_ports


# ---------------------------------------------------------------------------
# Chart CI
# ---------------------------------------------------------------------------


def test_chart_ci_actually_renders_the_component() -> None:
    """canvas.livePreview.viewer.enabled is never true in any of the nine
    helm/ci scenarios, so canvas-gateway's rendered output has never been
    validated by CI. Do not repeat that.
    """
    docs = _render("-f", str(CHART / "ci/eval-values.yaml"))
    assert _find(docs, "Deployment", "ssh-gateway")
    assert _find(docs, "Service", "ssh-gateway")
    assert _find(docs, "ConfigMap", "ssh-gateway")
    assert _find(docs, "NetworkPolicy", "ssh-gateway")
    orchestrator = _one(docs, "Deployment", "-orchestrator")
    assert "SSH_GATEWAY_PUBLIC_HOST_KEYS" in _env(
        _container(orchestrator, "orchestrator")
    )

    # C2: the scenario exists to VALIDATE the component, so it has to describe
    # an install that could boot, not merely one that renders. As written it
    # set no sessionRouter JWT secret, so SESSION_JWT_SECRET named a Secret
    # this render never produced and the gateway crash-looped -- in the very
    # CI case added to catch that.
    ref = _env(_container(_one(docs, "Deployment", "ssh-gateway"), "ssh-gateway"))[
        "SESSION_JWT_SECRET"
    ]["valueFrom"]["secretKeyRef"]
    assert _find(docs, "Secret", ref["name"]), (
        f"eval-values renders no Secret named {ref['name']}; the gateway's "
        "SESSION_JWT_SECRET is optional:true, so this is an unset variable "
        "and load_config refuses to boot"
    )
