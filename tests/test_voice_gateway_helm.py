"""The chart's local voice path: voiceGateway and asterisk; local_voice_gateway.md §4.4.

Renders the chart with both components off (nothing of them, the seed as
before) and on (a registered and a peer line, two profiles, both network
modes), and loads the two rendered configs with the gateway's own readers,
so a template and the code that reads it cannot drift apart.
"""

from __future__ import annotations

import base64
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from orchestrator.services.connector_egress import SERVICE_EGRESS_ANNOTATION
from orchestrator.services.connector_service_launch import label_value
from shared.connectors.builtin import GPT_LIVE_SIP_SPEC
from voice_gateway.asterisk.render import load_values, render
from voice_gateway.config import load_config

ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "helm"
RELEASE = "srw"
NAMESPACE = "srw"
FULL = "srw-superhuman-remote-worker"

pytestmark = pytest.mark.skipif(
    shutil.which("helm") is None, reason="Helm is not installed"
)

LINES = [
    {
        "name": "reception",
        "mode": "register",
        "pbx": "192.0.2.10",
        "extension": "2000",
        "passwordSecret": {"name": "srw-voice-lines", "key": "reception-sip"},
        "connectorId": "00000000-0000-4000-8000-000000000001",
        "webhookSecret": {"name": "srw-voice-lines", "key": "reception-webhook"},
    },
    {
        "name": "hotline",
        "mode": "peer",
        "pbx": "192.0.2.20:5070",
        "match": ["192.0.2.20/32"],
        "connectorId": "00000000-0000-4000-8000-000000000002",
        "webhookSecret": {"name": "srw-voice-lines", "key": "hotline-webhook"},
    },
]
PROFILES = {"echo": {"engine": "echo"}, "scripted": {"engine": "scripted"}}
VOICE = {
    "voiceGateway": {
        "enabled": True,
        "lines": LINES,
        "profiles": PROFILES,
        "catalog": {
            "models": {"echo": {"displayName": "Echo test", "ratePerMinuteUsd": 0.02}}
        },
    },
    "asterisk": {
        "enabled": True,
        "nodeSelector": {"kubernetes.io/hostname": "voice-node"},
    },
}
LOAD_BALANCER = {
    "network": {
        "mode": "loadBalancer",
        "externalAddress": "192.0.2.50",
        "allowedSources": ["192.0.2.10/32"],
        "loadBalancer": {"ip": "192.0.2.50"},
    },
    "rtp": {"start": 10000, "end": 10003},
}


def _merge(base: dict, extra: dict) -> dict:
    """``base`` with ``extra`` merged in; an empty mapping replaces."""
    out = dict(base)
    for key, value in extra.items():
        if isinstance(value, dict) and value and isinstance(out.get(key), dict):
            out[key] = _merge(out[key], value)
        else:
            out[key] = value
    return out


def _render(tmp_path: Path, values: dict) -> subprocess.CompletedProcess:
    path = tmp_path / "voice-values.yaml"
    path.write_text(yaml.safe_dump(values), encoding="utf-8")
    return subprocess.run(
        [
            "helm",
            "template",
            RELEASE,
            str(CHART),
            "--namespace",
            NAMESPACE,
            "-f",
            str(CHART / "ci/test-values.yaml"),
            "-f",
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=180,
    )


def _docs(tmp_path: Path, values: dict) -> dict[tuple[str, str], dict]:
    result = _render(tmp_path, values)
    assert result.returncode == 0, result.stderr
    return {
        (doc["kind"], doc["metadata"]["name"]): doc
        for doc in yaml.safe_load_all(result.stdout)
        if doc
    }


def _refused(tmp_path: Path, values: dict, fragment: str) -> None:
    result = _render(tmp_path, values)
    assert result.returncode != 0
    assert fragment in result.stderr, result.stderr


def _voice_objects(docs: dict) -> list[tuple[str, str]]:
    return sorted(
        key for key in docs if "voice-gateway" in key[1] or "asterisk" in key[1]
    )


# =============================================================================
# Off by default
# =============================================================================


def test_off_by_default_renders_nothing_of_the_voice_path(tmp_path):
    docs = _docs(tmp_path, {})
    assert _voice_objects(docs) == []
    assert ("ConfigMap", f"{FULL}-llm-seed") not in docs


def test_values_document_both_components_off():
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["voiceGateway"]["enabled"] is False
    assert values["asterisk"]["enabled"] is False
    assert values["asterisk"]["network"]["mode"] == "hostNetwork"
    assert values["voiceGateway"]["image"]["repository"].endswith("srw-voice-gateway")
    assert values["asterisk"]["image"]["repository"].endswith("srw-asterisk")


# =============================================================================
# On: the objects and what they hold
# =============================================================================


@pytest.fixture(scope="module")
def voice_docs(tmp_path_factory):
    return _docs(tmp_path_factory.mktemp("voice"), VOICE)


def test_on_renders_the_gateway_and_the_addon(voice_docs):
    assert _voice_objects(voice_docs) == sorted(
        [
            ("ConfigMap", f"{FULL}-asterisk"),
            ("ConfigMap", f"{FULL}-voice-gateway"),
            ("Deployment", f"{FULL}-asterisk"),
            ("Deployment", f"{FULL}-voice-gateway"),
            ("NetworkPolicy", f"{FULL}-voice-gateway"),
            ("Secret", f"{FULL}-voice-gateway"),
            ("Service", f"{FULL}-asterisk-ari"),
            ("Service", f"{FULL}-voice-gateway"),
            ("Service", f"{FULL}-voice-gateway-media"),
        ]
    )


def _files(tmp_path: Path, text: str, secrets: dict[str, str]) -> Path:
    """The config with /run/srw-voice moved under tmp_path, its secrets there."""
    run = tmp_path / "run"
    for name, value in secrets.items():
        (run / name).parent.mkdir(parents=True, exist_ok=True)
        (run / name).write_text(value)
    path = tmp_path / "config.yaml"
    path.write_text(text.replace("/run/srw-voice", str(run)))
    return path


def test_the_gateway_reads_its_rendered_config(voice_docs, tmp_path):
    text = voice_docs[("ConfigMap", f"{FULL}-voice-gateway")]["data"]["gateway.yaml"]
    webhook = "whsec_" + base64.b64encode(b"chart-test-webhook-key").decode()
    config = load_config(
        _files(
            tmp_path,
            text,
            {
                "api-key": "chart-test-api-key",
                "asterisk-password": "chart-test-password",
                "lines/reception/webhook-secret": webhook,
                "lines/hotline/webhook-secret": webhook,
            },
        )
    )
    assert (config.host, config.port) == ("0.0.0.0", 18440)
    assert config.orchestrator_url == f"http://{FULL}-orchestrator.srw.svc:8085"
    assert {name: line.connector_id for name, line in config.lines.items()} == {
        line["name"]: line["connectorId"] for line in LINES
    }
    assert config.profiles == PROFILES
    assert config.source["ari_url"] == f"http://{FULL}-asterisk-ari.srw.svc:8088"
    assert config.source["media_listen"] == "0.0.0.0:18441"
    assert config.test_control is False


def test_the_renderer_reads_the_addons_values(voice_docs, tmp_path):
    text = voice_docs[("ConfigMap", f"{FULL}-asterisk")]["data"]["asterisk.yaml"]
    values = load_values(
        _files(
            tmp_path,
            text,
            {
                "asterisk-password": "chart-test-password",
                "lines/reception/sip-password": "chart-test-sip",
            },
        ),
        ari_bind="10.42.0.7:8088",
    )
    lines = {line.name: line for line in values.lines}
    assert lines["reception"].mode == "register"
    assert lines["reception"].extension == "2000"
    assert lines["hotline"].mode == "peer"
    assert lines["hotline"].match == ("192.0.2.20/32",)
    assert values.media_uri == f"ws://{FULL}-voice-gateway-media.srw.svc:18441/media"
    gateway = yaml.safe_load(
        voice_docs[("ConfigMap", f"{FULL}-voice-gateway")]["data"]["gateway.yaml"]
    )
    # Both ends of ARI and the audio connection agree.
    assert values.app == gateway["source"]["app"]
    assert values.ari_user == gateway["source"]["ari_user"]
    assert values.media_connection == gateway["source"]["media_connection"]
    files = render(values)
    assert "Bridge(${SRW_HANDOVER_TO},x)" in files["extensions.conf"]


def test_secrets_stay_out_of_config_maps(voice_docs):
    secret = voice_docs[("Secret", f"{FULL}-voice-gateway")]
    generated = [base64.b64decode(v).decode() for v in secret["data"].values()]
    assert sorted(secret["data"]) == ["api-key", "asterisk-password"]
    assert secret["metadata"]["annotations"]["helm.sh/resource-policy"] == "keep"
    for kind, name in _voice_objects(voice_docs):
        if kind == "ConfigMap":
            text = yaml.safe_dump(voice_docs[(kind, name)]["data"])
            assert not any(value in text for value in generated)
            assert "whsec_" not in text


def _pod(docs: dict, name: str) -> dict:
    return docs[("Deployment", f"{FULL}-{name}")]["spec"]["template"]["spec"]


def _mounted(pod: dict) -> dict[str, tuple[str, str]]:
    (volume,) = [v for v in pod["volumes"] if v["name"] == "secrets"]
    return {
        item["path"]: (source["secret"]["name"], item["key"])
        for source in volume["projected"]["sources"]
        for item in source["secret"]["items"]
    }


def test_each_component_mounts_only_its_secret_files(voice_docs):
    gateway, asterisk = _pod(voice_docs, "voice-gateway"), _pod(voice_docs, "asterisk")
    own = f"{FULL}-voice-gateway"
    assert _mounted(gateway) == {
        "api-key": (own, "api-key"),
        "asterisk-password": (own, "asterisk-password"),
        "lines/reception/webhook-secret": ("srw-voice-lines", "reception-webhook"),
        "lines/hotline/webhook-secret": ("srw-voice-lines", "hotline-webhook"),
    }
    # The SIP passwords only for the init container that renders Asterisk's
    # config; Asterisk itself mounts no Secret.
    assert _mounted(asterisk) == {
        "asterisk-password": (own, "asterisk-password"),
        "lines/reception/sip-password": ("srw-voice-lines", "reception-sip"),
    }
    (render_container,) = asterisk["initContainers"]
    assert "secrets" in {m["name"] for m in render_container["volumeMounts"]}
    (container,) = asterisk["containers"]
    assert "secrets" not in {m["name"] for m in container["volumeMounts"]}


def test_both_run_unprivileged_on_a_read_only_root(voice_docs):
    for name in ("voice-gateway", "asterisk"):
        pod = _pod(voice_docs, name)
        assert pod["securityContext"]["fsGroup"] == 10001
        assert pod["securityContext"]["runAsNonRoot"] is True
        assert pod["automountServiceAccountToken"] is False
        for container in pod["containers"] + pod.get("initContainers", []):
            context = container["securityContext"]
            assert context["readOnlyRootFilesystem"] is True
            assert context["allowPrivilegeEscalation"] is False
            assert context["capabilities"]["drop"] == ["ALL"]
        # Writable places are memory, so no call leaves a trace on disk.
        for volume in pod["volumes"]:
            if "emptyDir" in volume:
                assert volume["emptyDir"]["medium"] == "Memory"


def test_hostnetwork_pins_asterisk_and_the_gateway_beside_it(voice_docs):
    asterisk = _pod(voice_docs, "asterisk")
    assert asterisk["hostNetwork"] is True
    assert asterisk["dnsPolicy"] == "ClusterFirstWithHostNet"
    assert asterisk["nodeSelector"] == {"kubernetes.io/hostname": "voice-node"}
    (render_container,) = asterisk["initContainers"]
    assert render_container["args"][-2:] == ["--ari-bind", "$(POD_IP):8088"]
    affinity = _pod(voice_docs, "voice-gateway")["affinity"]["podAffinity"]
    (term,) = affinity["requiredDuringSchedulingIgnoredDuringExecution"]
    assert term["labelSelector"]["matchLabels"]["app.kubernetes.io/component"] == (
        "asterisk"
    )
    # NetworkPolicy does not reach a hostNetwork pod: none is rendered for it.
    assert ("NetworkPolicy", f"{FULL}-asterisk") not in voice_docs
    assert ("Service", f"{FULL}-asterisk-sip") not in voice_docs


def test_both_are_recreated_never_doubled(voice_docs):
    for name in ("voice-gateway", "asterisk"):
        spec = voice_docs[("Deployment", f"{FULL}-{name}")]["spec"]
        assert spec["replicas"] == 1
        assert spec["strategy"] == {"type": "Recreate"}


# =============================================================================
# Reachability: the phone-line driver, the orchestrator, Asterisk
# =============================================================================


def test_the_gateways_api_service_admits_the_phone_line_driver(voice_docs):
    service = voice_docs[("Service", f"{FULL}-voice-gateway")]
    assert service["metadata"]["annotations"] == {
        SERVICE_EGRESS_ANNOTATION: GPT_LIVE_SIP_SPEC.name
    }
    assert [port["port"] for port in service["spec"]["ports"]] == [18440]
    media = voice_docs[("Service", f"{FULL}-voice-gateway-media")]
    assert "annotations" not in media["metadata"]
    assert [port["port"] for port in media["spec"]["ports"]] == [18441]


def test_the_gateways_policy_admits_the_driver_orchestrator_and_asterisk(
    voice_docs,
):
    policy = voice_docs[("NetworkPolicy", f"{FULL}-voice-gateway")]["spec"]
    assert policy["policyTypes"] == ["Ingress"]
    api, media = policy["ingress"]
    driver, orchestrator = api["from"]
    assert driver["namespaceSelector"]["matchLabels"] == {
        "kubernetes.io/metadata.name": f"{FULL}-connectors"
    }
    # The driver pods' own label (connector_service_launch.ServicePodIdentity).
    assert driver["podSelector"]["matchLabels"] == {
        "srw.io/driver": label_value(GPT_LIVE_SIP_SPEC.name)
    }
    assert orchestrator["podSelector"]["matchLabels"][
        "app.kubernetes.io/component"
    ] == ("orchestrator")
    assert api["ports"] == [{"protocol": "TCP", "port": 18440}]
    (asterisk,) = media["from"]
    assert asterisk["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == (
        "asterisk"
    )
    assert media["ports"] == [{"protocol": "TCP", "port": 18441}]


def test_load_balancer_mode_publishes_sip_and_rtp_and_guards_ari(tmp_path):
    docs = _docs(tmp_path, _merge(VOICE, {"asterisk": LOAD_BALANCER}))
    pod = _pod(docs, "asterisk")
    assert "hostNetwork" not in pod
    assert "affinity" not in _pod(docs, "voice-gateway")
    service = docs[("Service", f"{FULL}-asterisk-sip")]["spec"]
    assert service["type"] == "LoadBalancer"
    assert service["externalTrafficPolicy"] == "Local"
    assert service["loadBalancerSourceRanges"] == ["192.0.2.10/32"]
    ports = {(p["port"], p["protocol"]) for p in service["ports"]}
    assert ports == {(5060, "UDP"), (5060, "TCP")} | {
        (port, "UDP") for port in range(10000, 10004)
    }
    policy = docs[("NetworkPolicy", f"{FULL}-asterisk")]["spec"]
    ari, phone = policy["ingress"]
    (gateway,) = ari["from"]
    assert gateway["podSelector"]["matchLabels"]["app.kubernetes.io/component"] == (
        "voice-gateway"
    )
    assert ari["ports"] == [{"protocol": "TCP", "port": 8088}]
    assert phone["from"] == [{"ipBlock": {"cidr": "192.0.2.10/32"}}]
    assert {"protocol": "UDP", "port": 10000, "endPort": 10003} in phone["ports"]
    addon = yaml.safe_load(
        docs[("ConfigMap", f"{FULL}-asterisk")]["data"]["asterisk.yaml"]
    )
    assert addon["sip"]["external_address"] == "192.0.2.50"
    # The cluster's ranges reach Asterisk without the load balancer.
    assert addon["sip"]["local_net"] == ["10.42.0.0/16", "10.43.0.0/16"]


# =============================================================================
# Catalog seeding
# =============================================================================


def _payload(docs: dict) -> dict:
    return yaml.safe_load(docs[("ConfigMap", f"{FULL}-llm-seed")]["data"]["llm.yaml"])


def test_the_profiles_are_seeded_as_live_voice_rows(voice_docs):
    payload = _payload(voice_docs)
    (endpoint,) = payload["systemEndpoints"]
    assert endpoint["label"] == "SRW voice gateway"
    assert endpoint["baseUrl"] == f"http://{FULL}-voice-gateway.srw.svc:18440/v1"
    assert endpoint["apiKeyEnv"] == "SEED_VOICE_GATEWAY_API_KEY"
    rows = {row["id"]: row for row in endpoint["models"]}
    assert set(rows) == set(PROFILES)
    assert all(row["capabilities"] == ["live_voice"] for row in rows.values())
    assert rows["echo"]["displayName"] == "Echo test"
    assert rows["echo"]["params"] == {
        "adapter": "openai-live",
        "rate_usd_per_minute": 0.02,
    }
    # A local engine prices at zero unless the operator sets a rate.
    assert rows["scripted"]["params"]["rate_usd_per_minute"] == 0
    assert "reconcile" not in endpoint  # insert-only like the rest
    # Only the gateway: llm.seed itself is off in these values.
    assert not payload["systemApiKeys"] and not payload["systemModels"]
    job = voice_docs[("Job", f"{FULL}-llm-seed")]["spec"]["template"]["spec"]
    (seed,) = job["containers"]
    env = {item["name"]: item for item in seed["env"]}
    assert env["SEED_VOICE_GATEWAY_API_KEY"]["valueFrom"]["secretKeyRef"] == {
        "name": f"{FULL}-voice-gateway",
        "key": "api-key",
    }


def test_the_gateway_joins_an_existing_seed(tmp_path):
    seed = {
        "llm": {
            "seed": {
                "enabled": True,
                "systemEndpoints": [
                    {
                        "label": "Local Gemma",
                        "baseUrl": "http://vllm.ai.svc:8000/v1",
                        "models": [{"id": "gemma-4", "capabilities": ["chat"]}],
                    }
                ],
            }
        }
    }
    payload = _payload(_docs(tmp_path, _merge(VOICE, seed)))
    labels = [endpoint["label"] for endpoint in payload["systemEndpoints"]]
    assert labels == ["Local Gemma", "SRW voice gateway"]


def test_seeding_can_be_left_to_the_admin(tmp_path):
    docs = _docs(
        tmp_path, _merge(VOICE, {"voiceGateway": {"catalog": {"seed": False}}})
    )
    assert ("ConfigMap", f"{FULL}-llm-seed") not in docs
    assert ("Job", f"{FULL}-llm-seed") not in docs


def test_an_existing_secret_replaces_the_generated_one(tmp_path):
    docs = _docs(
        tmp_path, _merge(VOICE, {"voiceGateway": {"existingSecret": "vault-voice"}})
    )
    assert ("Secret", f"{FULL}-voice-gateway") not in docs
    assert _mounted(_pod(docs, "voice-gateway"))["api-key"] == (
        "vault-voice",
        "api-key",
    )
    job = docs[("Job", f"{FULL}-llm-seed")]["spec"]["template"]["spec"]
    env = {item["name"]: item for item in job["containers"][0]["env"]}
    assert env["SEED_VOICE_GATEWAY_API_KEY"]["valueFrom"]["secretKeyRef"]["name"] == (
        "vault-voice"
    )


# =============================================================================
# What the chart refuses
# =============================================================================


def _line(**changes) -> dict:
    return {"voiceGateway": {"lines": [{**LINES[0], **changes}, LINES[1]]}}


@pytest.mark.parametrize(
    "values, fragment",
    [
        (
            {"asterisk": {"enabled": True, "nodeSelector": {"a": "b"}}},
            "asterisk.enabled needs voiceGateway.enabled",
        ),
        (
            _merge(VOICE, {"asterisk": {"enabled": False}}),
            "needs asterisk.enabled",
        ),
        (_merge(VOICE, {"voiceGateway": {"lines": []}}), "voiceGateway.lines is empty"),
        (_merge(VOICE, {"voiceGateway": {"profiles": {}}}), "profiles is empty"),
        (_merge(VOICE, _line(name="Reception")), "lowercase letters"),
        (_merge(VOICE, _line(name="hotline")), "used twice"),
        (_merge(VOICE, _line(mode="trunk")), "register or peer"),
        (_merge(VOICE, _line(passwordSecret=None)), "registered line needs"),
        (_merge(VOICE, _line(connectorId="line-1")), "connectorId"),
        (_merge(VOICE, _line(webhookSecret={"name": "x"})), "webhookSecret"),
        (
            _merge(VOICE, {"asterisk": {"nodeSelector": {}}}),
            "needs asterisk.nodeSelector",
        ),
        (
            _merge(VOICE, {"asterisk": {"network": {"mode": "loadBalancer"}}}),
            "needs asterisk.network.externalAddress",
        ),
        (
            _merge(VOICE, {"asterisk": {"network": {"mode": "nodePort"}}}),
            "hostNetwork or loadBalancer",
        ),
        (
            _merge(VOICE, {"asterisk": {"rtp": {"start": 10000, "end": 10001}}}),
            "two ports per call",
        ),
        (
            _merge(
                VOICE, {"voiceGateway": {"catalog": {"models": {"gpt-live-1": {}}}}}
            ),
            "names no profile",
        ),
    ],
)
def test_unusable_values_fail_the_render_with_the_fix(tmp_path, values, fragment):
    _refused(tmp_path, values, fragment)
