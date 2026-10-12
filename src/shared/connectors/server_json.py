"""An MCP Registry ``server.json`` as a managed MCP driver spec (D6).

The MCP Registry format covers most of what a managed MCP driver needs: the
image (an ``oci`` package), the transport, the port and path, and the
environment the server reads. :func:`spec_from_server_json` maps one ``oci``
package to the spec JSON SRW registers (``shared.connectors.registration``):

* the driver name is the server's reverse-DNS name with its last segment
  appended (``io.github.acme/weather`` at ``1.4.2`` becomes
  ``io.github.acme.weather/v1``): the registry's ``/`` is not a driver
  name's, and the major version is the contract version;
* ``streamable-http`` is SRW's ``http`` transport, at the port and path of
  the package's transport URL; ``stdio`` is SRW's ``stdio`` (served once the
  stdio bridge is installed); ``sse`` is refused;
* a non-secret environment variable becomes a config property (its
  description, default and choices with it) that the server's environment
  reads through ``${config.<key>}``; a required one is required;
* a secret header of the transport becomes the connector's one credential,
  which SRW's front injects per request (a ``Bearer`` value keeps its
  scheme). A secret environment variable is refused: a managed server never
  holds a credential in its environment in this release;
* no tool is classed read: a ReadOnly binding sees none until someone writes
  a spec that classes them (fail closed).

npm, pypi, nuget and mcpb packages, and ``remotes``, are unsupported here:
:class:`ServerJsonError` names why, so the registration says so instead of
importing half a server.

Design: knowledge-base/knowledge/features/connector_drivers.md, "MCP"
(Import from ``server.json``); research lane 5 §7, lane 6.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from .images import ImageReference

#: The image label the MCP Registry checks for ownership of an oci package.
SERVER_NAME_LABEL = "io.modelcontextprotocol.server.name"
#: The port SRW's front listens on, unless the server needs it.
FRONT_PORT = 8080
_LABEL = re.compile(r"[^a-z0-9-]+")
_CONFIG_KEY = re.compile(r"[^a-z0-9_]+")
_SEMVER_MAJOR = re.compile(r"^v?(0|[1-9][0-9]*)(?:\.|$)")


class ServerJsonError(ValueError):
    """A ``server.json`` SRW cannot import, with the reason to show."""


def _label(text: str) -> str:
    label = _LABEL.sub("-", text.lower()).strip("-")
    label = label.lstrip("0123456789-")
    return label or "server"


def driver_name(server_name: str, version: str | None) -> str:
    """The driver name for a registry name and version."""
    namespace, slash, server = server_name.partition("/")
    if not slash or not namespace or not server:
        raise ServerJsonError(
            f"server.json name {server_name!r} is not '<reverse-DNS>/<server>'"
        )
    labels = [_label(part) for part in namespace.split(".") if part]
    labels.append(_label(server.replace("/", "-")))
    match = _SEMVER_MAJOR.match(str(version or ""))
    major = max(1, int(match.group(1))) if match else 1
    return ".".join(labels) + f"/v{major}"


def image_reference(package: Mapping[str, Any]) -> str:
    """An ``oci`` package's image reference, in either schema generation:
    ``identifier`` as a full reference, or ``registryBaseUrl`` plus
    ``identifier`` plus ``version``."""
    identifier = package.get("identifier")
    if not isinstance(identifier, str) or not identifier:
        raise ServerJsonError("the oci package has no identifier")
    base = package.get("registryBaseUrl")
    if isinstance(base, str) and base:
        host = urlsplit(base).netloc or base
        if not identifier.startswith(host + "/"):
            identifier = f"{host}/{identifier}"
    version = package.get("version")
    name_part = identifier.rsplit("/", 1)[-1]
    if (
        isinstance(version, str)
        and version
        and ":" not in name_part
        and "@" not in identifier
    ):
        identifier = f"{identifier}:{version}"
    try:
        return str(ImageReference.parse(identifier))
    except ValueError as exc:
        raise ServerJsonError(
            f"the oci package's image {identifier!r}: {exc}"
        ) from None


def _transport(package: Mapping[str, Any]) -> tuple[str, int, str]:
    transport = package.get("transport")
    kind = transport.get("type") if isinstance(transport, Mapping) else None
    if kind == "stdio":
        return "stdio", FRONT_PORT + 1, "/mcp"
    if kind != "streamable-http":
        raise ServerJsonError(
            f"transport {kind!r} is not one SRW manages (streamable-http or stdio)"
        )
    url = transport.get("url")
    if not isinstance(url, str) or "{" in url:
        raise ServerJsonError(
            "the streamable-http transport needs a literal URL (a templated one, "
            "such as {port}, cannot be mapped); register the image with a spec "
            "label instead"
        )
    parts = urlsplit(url)
    try:
        port = parts.port or (443 if parts.scheme == "https" else 80)
    except ValueError:
        raise ServerJsonError(f"the transport URL {url!r} has no valid port") from None
    return "http", port, parts.path or "/mcp"


def _config_key(name: str) -> str:
    key = _CONFIG_KEY.sub("_", name.lower()).strip("_")
    return key if key and key[0].isalpha() else f"v_{key}"


def _environment(
    package: Mapping[str, Any],
) -> tuple[dict[str, Any], list[str], dict[str, str]]:
    """Config properties, the required ones and the server's environment."""
    properties: dict[str, Any] = {}
    required: list[str] = []
    env: dict[str, str] = {}
    secrets = []
    for variable in package.get("environmentVariables") or []:
        if not isinstance(variable, Mapping) or not isinstance(
            variable.get("name"), str
        ):
            raise ServerJsonError("an environment variable has no name")
        name = variable["name"]
        if variable.get("isSecret"):
            secrets.append(name)
            continue
        key = _config_key(name)
        if key in properties:
            raise ServerJsonError(f"two environment variables map to config.{key}")
        schema: dict[str, Any] = {"type": "string", "maxLength": 4096}
        if isinstance(variable.get("description"), str):
            schema["description"] = variable["description"][:500]
        if isinstance(variable.get("default"), str):
            schema["default"] = variable["default"]
        choices = variable.get("choices")
        if isinstance(choices, list) and all(isinstance(c, str) for c in choices):
            schema["enum"] = list(choices)
        properties[key] = schema
        if variable.get("isRequired") and "default" not in schema:
            required.append(key)
        env[name] = "${config." + key + "}"
    if secrets:
        raise ServerJsonError(
            f"secret environment variables {sorted(secrets)} are unsupported: a "
            "managed MCP server receives its credential per request, in a header "
            "SRW's front injects, never in its environment"
        )
    return properties, required, env


def _credential(package: Mapping[str, Any]) -> dict[str, str] | None:
    transport = package.get("transport")
    headers = transport.get("headers") if isinstance(transport, Mapping) else None
    secret = [
        header
        for header in headers or []
        if isinstance(header, Mapping) and header.get("isSecret")
    ]
    if len(secret) > 1:
        raise ServerJsonError(
            "more than one secret header: a managed MCP connector holds one credential"
        )
    if not secret:
        return None
    header = secret[0]
    name = header.get("name")
    if not isinstance(name, str) or not name:
        raise ServerJsonError("a secret header has no name")
    value = header.get("value")
    scheme = ""
    if isinstance(value, str) and value.lower().startswith("bearer "):
        scheme = "Bearer"
    return {"header": name, "scheme": scheme}


def _arguments(package: Mapping[str, Any]) -> list[str]:
    args: list[str] = []
    for argument in package.get("packageArguments") or []:
        if not isinstance(argument, Mapping):
            raise ServerJsonError("a package argument is not an object")
        if argument.get("isSecret"):
            raise ServerJsonError("secret package arguments are unsupported")
        value = argument.get("value", argument.get("default"))
        if not isinstance(value, str) or "{" in value:
            raise ServerJsonError(
                "a package argument without a literal value is unsupported"
            )
        if argument.get("type") == "named":
            name = argument.get("name")
            if not isinstance(name, str) or not name:
                raise ServerJsonError("a named package argument has no name")
            args += [name, value]
        else:
            args.append(value)
    if package.get("runtimeArguments"):
        raise ServerJsonError(
            "runtime arguments (for the container runtime) are unsupported"
        )
    return args


def oci_packages(server: Mapping[str, Any]) -> list[int]:
    """The indexes of a server's ``oci`` packages."""
    packages = server.get("packages") or []
    return [
        index
        for index, package in enumerate(packages)
        if isinstance(package, Mapping) and package.get("registryType") == "oci"
    ]


def spec_from_server_json(
    server: Any, *, package: int | None = None
) -> tuple[dict[str, Any], str]:
    """``(spec JSON, image reference)`` for one ``oci`` package of a
    ``server.json``; :class:`ServerJsonError` when it cannot be mapped."""
    if not isinstance(server, Mapping):
        raise ServerJsonError("server.json must be an object")
    name = server.get("name")
    if not isinstance(name, str):
        raise ServerJsonError("server.json has no name")
    indexes = oci_packages(server)
    if not indexes:
        kinds = sorted(
            {
                str(item.get("registryType"))
                for item in server.get("packages") or []
                if isinstance(item, Mapping)
            }
        )
        raise ServerJsonError(
            "server.json has no oci package"
            + (f" ({', '.join(kinds)} packages are unsupported)" if kinds else "")
            + (
                "; remote servers are external MCP connectors"
                if server.get("remotes")
                else ""
            )
        )
    if package is None:
        if len(indexes) > 1:
            raise ServerJsonError(
                f"server.json has several oci packages {indexes}; name one"
            )
        package = indexes[0]
    elif package not in indexes:
        raise ServerJsonError(f"package {package} is not an oci package")
    chosen = server["packages"][package]
    reference = image_reference(chosen)
    transport, port, path = _transport(chosen)
    properties, required, env = _environment(chosen)
    credential = _credential(chosen)
    front_port = FRONT_PORT if port != FRONT_PORT else FRONT_PORT + 10
    config_schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": False,
        "properties": properties,
    }
    if required:
        config_schema["required"] = required
    mcp: dict[str, Any] = {
        "transport": transport,
        "port": port,
        "path": path,
        "protocol": "legacy",
        "tools": {"read": []},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": credential,
        "env": env,
        "args": _arguments(chosen),
        "tool_pinning": "warn",
    }
    title = server.get("title") or server.get("description") or name
    spec: dict[str, Any] = {
        "name": driver_name(name, server.get("version")),
        "title": str(title)[:200],
        "protocol_version": "1.0",
        "plane": "service",
        "delivery_forms": ["mcp_client"],
        "config_schema": config_schema,
        "credential_slots": (
            [
                {
                    "name": "token",
                    "kind": "secret_string",
                    "schema": {
                        "type": "object",
                        "properties": {"token": {"type": "string", "writeOnly": True}},
                    },
                    "required": True,
                    "update": "replace",
                }
            ]
            if credential
            else []
        ),
        "access_levels": [
            {
                "id": "ReadOnly",
                "rank": 0,
                "enforced_by": (
                    "SRW's front hides and refuses every tool not classed read; "
                    "an imported server classes none"
                ),
                "tools": "*",
            },
            {
                "id": "ReadWrite",
                "rank": 1,
                "enforced_by": "The server's own credential decides what is allowed.",
                "tools": "*",
            },
        ],
        "default_access": "ReadWrite",
        "supported_backends": ["none", "sandbox", "virtual", "vm"],
        "workspace_requirements": "None: the agent process is the MCP client.",
        "tool_category": "mcp",
        "holds_upstream_credentials": credential is not None,
        "credential_delivery": "lease",
        "service": {
            "port": front_port,
            "callers": ["harness"],
            "start_seconds": 30,
            "mcp": mcp,
        },
    }
    return spec, reference


__all__ = [
    "FRONT_PORT",
    "SERVER_NAME_LABEL",
    "ServerJsonError",
    "driver_name",
    "image_reference",
    "oci_packages",
    "spec_from_server_json",
]
