"""Operator network policy stays separate from arbitrary image configuration."""

from copy import deepcopy
import json
from pathlib import Path
import re

import pytest
import yaml

from orchestrator.services.manifest_harness_egress import (
    HARNESS_EGRESS_SCHEMA,
    HarnessEgressConfigurationError,
    validate_harness_egress,
)
from orchestrator.services.manifest_workspaces import public_workspace_egress


@pytest.mark.parametrize(
    "rules",
    [
        [],
        [{}],  # Explicit Kubernetes allow-all is operator authority.
        [{"to": [], "ports": []}],
        [
            {
                "to": [
                    {"ipBlock": {"cidr": "192.0.2.0/24", "except": ["192.0.2.64/26"]}}
                ],
                "ports": [{"port": 443}],
            }
        ],
        [
            {
                "to": [{"ipBlock": {"cidr": "2001:db8::/64"}}],
                "ports": [{"port": 8000, "endPort": 8100, "protocol": "SCTP"}],
            }
        ],
        [
            {
                "to": [
                    {
                        "namespaceSelector": {},
                        "podSelector": {
                            "matchLabels": {"example.com/name": "model-v1"},
                            "matchExpressions": [
                                {
                                    "key": "team",
                                    "operator": "In",
                                    "values": ["inference"],
                                },
                                {"key": "disabled", "operator": "DoesNotExist"},
                            ],
                        },
                    }
                ],
                "ports": [{"port": "https"}],
            }
        ],
        [
            {
                "to": [
                    {
                        "podSelector": {
                            "matchExpressions": [
                                {
                                    "key": "team",
                                    "operator": "NotIn",
                                    "values": ["other"],
                                },
                                {"key": "enabled", "operator": "Exists", "values": []},
                            ]
                        }
                    }
                ],
                "ports": [{"protocol": "UDP"}],
            }
        ],
    ],
)
def test_valid_network_policy_rules_keep_kubernetes_semantics_and_own_their_copy(rules):
    original = deepcopy(rules)
    result = validate_harness_egress(json.dumps(rules))
    assert result == tuple(original)
    if result:
        result[0]["to"] = []
    assert rules == original


@pytest.mark.parametrize(
    "rules",
    [
        "",
        "{",
        '{"to":[]}',
        '[{"to":[],"to":[]}]',
        None,
        [{"unknown": []}],
        [{"to": None}],
        [{"to": [{}]}],
        [{"to": [{"ipblock": {"cidr": "192.0.2.1/32"}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.1/32"}, "podSelector": {}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.1"}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.1/24"}}]}],
        [{"to": [{"ipBlock": {"cidr": "private.invalid/24"}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.0/24", "except": ["192.0.3.0/24"]}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.0/24", "except": ["192.0.2.0/24"]}}]}],
        [{"to": [{"ipBlock": {"cidr": "192.0.2.0/24", "except": ["2001:db8::/64"]}}]}],
        [{"to": [{"namespaceSelector": {"label": {"name": "wrong"}}}]}],
        [{"to": [{"podSelector": {"matchLabels": {"bad..domain/key": "ok"}}}]}],
        [{"to": [{"podSelector": {"matchLabels": {"key": "not a label"}}}]}],
        [{"to": [{"podSelector": {"matchLabels": {"key": "gateway\n"}}}]}],
        [{"to": [{"podSelector": {"matchLabels": {"key\n": "gateway"}}}]}],
        [
            {
                "to": [
                    {
                        "podSelector": {
                            "matchExpressions": [
                                {
                                    "key": "team",
                                    "operator": "In",
                                    "values": ["gateway\n"],
                                }
                            ]
                        }
                    }
                ]
            }
        ],
        [
            {
                "to": [
                    {
                        "podSelector": {
                            "matchExpressions": [{"key": "x", "operator": "In"}]
                        }
                    }
                ]
            }
        ],
        [
            {
                "to": [
                    {
                        "podSelector": {
                            "matchExpressions": [
                                {"key": "x", "operator": "Exists", "values": ["bad"]}
                            ]
                        }
                    }
                ]
            }
        ],
        [{"ports": [{"port": True}]}],
        [{"ports": [{"port": 443.0}]}],
        [{"ports": [{"port": 8000, "endPort": 8001.0}]}],
        [{"ports": [{"port": 0}]}],
        [{"ports": [{"port": 65536}]}],
        [{"ports": [{"port": "443"}]}],
        [{"ports": [{"port": "https\n"}]}],
        [{"ports": [{"port": "http--alt"}]}],
        [{"ports": [{"protocol": "tcp"}]}],
        [{"ports": [{"endPort": 8080}]}],
        [{"ports": [{"port": "http", "endPort": 8080}]}],
        [{"ports": [{"port": 9000, "endPort": 8000}]}],
        [{"ports": [{"port": 443, "host": "private.invalid"}]}],
    ],
)
def test_bad_installed_policy_is_rejected_without_echoing_values(rules):
    with pytest.raises(HarnessEgressConfigurationError) as rejected:
        validate_harness_egress(rules)
    assert "MANIFEST_HARNESS_EGRESS" in str(rejected.value)
    assert "private.invalid" not in str(rejected.value)


def test_helm_policy_schema_matches_the_server_structure_contract():
    path = Path(__file__).resolve().parents[1] / "helm/values.schema.json"
    schema = json.loads(path.read_text())
    assert (
        schema["properties"]["manifestHosting"]["properties"]["harnessEgress"]
        == HARNESS_EGRESS_SCHEMA
    )


def test_setting_name_appears_in_the_error_prefix():
    with pytest.raises(HarnessEgressConfigurationError) as err:
        validate_harness_egress("not json", setting="MANIFEST_WORKSPACE_EGRESS")
    assert str(err.value).startswith("Invalid MANIFEST_WORKSPACE_EGRESS: ")


ROOT = Path(__file__).resolve().parents[1]
# Where the generic-hosting example for an image that calls public services
# lives. SRW-adapter harness images (the Claude Code image) run under the agent
# pod's network rules, so they document no harnessEgress.
DOCUMENTED = ("examples/manifests/README.md",)


def _documented_harness_egress(relative: str) -> list:
    """The generic-hosting harnessEgress example in a Markdown doc: a
    `manifestHosting.harnessEgress` block with an HTTPS rule."""
    text = (ROOT / relative).read_text()
    blocks = re.findall(r"```yaml\n(.*?)```", text, re.S)
    found = []
    for block in blocks:
        data = yaml.safe_load(block)
        rules = (data or {}).get("manifestHosting", {}).get("harnessEgress") or []
        if any(port.get("port") == 443 for r in rules for port in r.get("ports", [])):
            found.append(rules)
    assert len(found) == 1, relative
    return found[0]


def test_the_documented_public_https_harness_egress_is_accepted():
    # I3: DNS to kube-dns, HTTPS to public addresses, in the generic-hosting
    # example of the manifest guide.
    examples = {path: _documented_harness_egress(path) for path in DOCUMENTED}
    first = examples[DOCUMENTED[0]]
    for path, rules in examples.items():
        assert rules == first, path
        assert validate_harness_egress(json.dumps(rules)) == tuple(rules), path
    dns, https = first
    # The same DNS rule and public-address block as the workspace default.
    default_dns, default_public = public_workspace_egress()
    assert dns == default_dns
    assert https == {**default_public, "ports": [{"protocol": "TCP", "port": 443}]}
