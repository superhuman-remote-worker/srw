"""Validate installation-owned generic harness NetworkPolicy egress rules.

This is deployment policy, never an authored Expert/Connector setting. The Helm
schema copies HARNESS_EGRESS_SCHEMA; a conformance test keeps that copy aligned.
Kubernetes semantics apply, including explicitly broad empty rules/selectors.
"""

from copy import deepcopy
import ipaddress
import json
import re

from jsonschema import Draft202012Validator


def _object(properties, *, required=()):
    result = {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }
    if required:
        result["required"] = list(required)
    return result


_LABEL_VALUE = r"^$|^[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$"
_LABEL_KEY = {
    "type": "string",
    "minLength": 1,
    "maxLength": 317,
    "pattern": r"^([a-z0-9]([a-z0-9.-]*[a-z0-9])?/)?[A-Za-z0-9]([A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?$",
}
_VALUE = {"type": "string", "maxLength": 63, "pattern": _LABEL_VALUE}
_SELECTOR = _object(
    {
        "matchLabels": {
            "type": "object",
            "propertyNames": _LABEL_KEY,
            "additionalProperties": _VALUE,
        },
        "matchExpressions": {
            "type": "array",
            "items": _object(
                {
                    "key": _LABEL_KEY,
                    "operator": {"enum": ["In", "NotIn", "Exists", "DoesNotExist"]},
                    "values": {"type": "array", "items": _VALUE},
                },
                required=("key", "operator"),
            ),
        },
    }
)
HARNESS_EGRESS_SCHEMA = {
    "type": "array",
    "maxItems": 64,
    "items": _object(
        {
            "to": {
                "type": "array",
                "items": _object(
                    {
                        "ipBlock": _object(
                            {
                                "cidr": {"type": "string", "minLength": 3},
                                "except": {
                                    "type": "array",
                                    "items": {"type": "string", "minLength": 3},
                                },
                            },
                            required=("cidr",),
                        ),
                        "namespaceSelector": _SELECTOR,
                        "podSelector": _SELECTOR,
                    }
                ),
            },
            "ports": {
                "type": "array",
                "items": _object(
                    {
                        "protocol": {"enum": ["TCP", "UDP", "SCTP"]},
                        "port": {
                            "oneOf": [
                                {"type": "integer", "minimum": 1, "maximum": 65535},
                                {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 15,
                                    "pattern": r"^[a-z0-9]([a-z0-9-]*[a-z0-9])?$",
                                },
                            ]
                        },
                        "endPort": {"type": "integer", "minimum": 1, "maximum": 65535},
                    }
                ),
            },
        }
    ),
}
_VALIDATOR = Draft202012Validator(HARNESS_EGRESS_SCHEMA)
_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\Z")


class HarnessEgressConfigurationError(ValueError):
    """A sanitized operator configuration error; never include supplied values."""


def _fail(message):
    raise HarnessEgressConfigurationError("Invalid MANIFEST_HARNESS_EGRESS: " + message)


def _unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            _fail("duplicate JSON object fields are not allowed.")
        result[key] = value
    return result


def _cidr(value):
    if "/" not in value:
        _fail("IP blocks require explicit CIDR prefixes.")
    try:
        return ipaddress.ip_network(value, strict=True)
    except ValueError:
        _fail("IP blocks require canonical IPv4 or IPv6 CIDRs.")


def _selector(selector):
    keys = list(selector.get("matchLabels", {}))
    label_values = list(selector.get("matchLabels", {}).values())
    for expression in selector.get("matchExpressions", []):
        keys.append(expression["key"])
        values = expression.get("values", [])
        label_values.extend(values)
        if (expression["operator"] in {"In", "NotIn"}) != bool(values):
            _fail("selector values do not match their operator.")
    for key in keys:
        if not re.fullmatch(_LABEL_KEY["pattern"], key):
            _fail("selector keys must be complete Kubernetes label names.")
        if "/" in key:
            prefix = key.split("/", 1)[0]
            if len(prefix) > 253 or not all(
                _DNS_LABEL.fullmatch(part) for part in prefix.split(".")
            ):
                _fail("selector key prefixes must be valid DNS subdomains.")
    if any(re.fullmatch(_LABEL_VALUE, value) is None for value in label_values):
        _fail("selector values must be complete Kubernetes label values.")


def validate_harness_egress(value, *, setting: str = "MANIFEST_HARNESS_EGRESS"):
    """Return an owned copy or reject the whole policy before any workload effects."""
    try:
        return _validate_harness_egress(value)
    except HarnessEgressConfigurationError as exc:
        default = "Invalid MANIFEST_HARNESS_EGRESS: "
        if setting == "MANIFEST_HARNESS_EGRESS":
            raise
        detail = str(exc).removeprefix(default)
        raise HarnessEgressConfigurationError(f"Invalid {setting}: {detail}") from None


def _validate_harness_egress(value):
    if isinstance(value, str):
        if len(value) > 128 * 1024:
            _fail("JSON policy exceeds 128 KiB.")
        try:
            value = json.loads(value, object_pairs_hook=_unique_object)
        except (json.JSONDecodeError, RecursionError):
            _fail("expected a JSON array of NetworkPolicy egress rules.")
    if next(_VALIDATOR.iter_errors(value), None) is not None:
        _fail("unknown fields or invalid rule, peer, selector, or port types.")
    for rule in value:
        for peer in rule.get("to", []):
            if not peer:
                _fail("a peer must select IP blocks, namespaces, or pods.")
            if "ipBlock" in peer:
                if len(peer) != 1:
                    _fail("IP blocks cannot be combined with selectors.")
                block = _cidr(peer["ipBlock"]["cidr"])
                for excluded in peer["ipBlock"].get("except", []):
                    excluded = _cidr(excluded)
                    if (
                        excluded.version != block.version
                        or excluded.prefixlen <= block.prefixlen
                        or not excluded.subnet_of(block)
                    ):
                        _fail("CIDR exclusions must be strictly contained subnets.")
            for key in ("namespaceSelector", "podSelector"):
                if key in peer:
                    _selector(peer[key])
        for port in rule.get("ports", []):
            first, last = port.get("port"), port.get("endPort")
            if (
                first is not None
                and not isinstance(first, str)
                and type(first) is not int
            ) or (last is not None and type(last) is not int):
                _fail("numeric ports must be JSON integers.")
            if isinstance(first, str) and (
                re.fullmatch(r"[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", first) is None
                or not re.search("[a-z]", first)
                or "--" in first
            ):
                _fail(
                    "named ports must be complete names with a letter and no adjacent hyphens."
                )
            if last is not None and (not isinstance(first, int) or last < first):
                _fail("endPort requires a numeric port and an ascending range.")
    return tuple(deepcopy(value))
