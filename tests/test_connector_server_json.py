"""An MCP Registry ``server.json`` as a managed MCP driver spec (D6)."""

from __future__ import annotations

import pytest

from shared.connectors.registration import custom_driver_problems, spec_from_json
from shared.connectors.server_json import (
    ServerJsonError,
    driver_name,
    image_reference,
    spec_from_server_json,
)


def _server(**package_over):
    package = {
        "registryType": "oci",
        "identifier": "ghcr.io/acme/weather:1.4.2",
        "transport": {"type": "streamable-http", "url": "http://localhost:9000/mcp"},
        "environmentVariables": [
            {
                "name": "WEATHER_UNITS",
                "description": "Units",
                "default": "metric",
                "choices": ["metric", "imperial"],
            },
            {"name": "REGION", "isRequired": True},
        ],
    }
    package.update(package_over)
    return {
        "name": "io.github.acme/weather",
        "version": "1.4.2",
        "title": "Weather",
        "packages": [package],
    }


class TestMapping:
    def test_an_oci_http_package_maps_to_a_valid_managed_mcp_spec(self):
        spec_json, reference = spec_from_server_json(_server())
        assert reference == "ghcr.io/acme/weather:1.4.2"
        spec = spec_from_json(spec_json)
        assert spec.name == "io.github.acme.weather/v1"
        assert spec.plane == "service"
        assert custom_driver_problems(spec, privileged=False) == []
        mcp = spec.service.mcp
        assert (mcp["transport"], mcp["port"], mcp["path"]) == ("http", 9000, "/mcp")
        # Non-secret variables are config, read through ${config.<key>}.
        assert mcp["env"] == {
            "WEATHER_UNITS": "${config.weather_units}",
            "REGION": "${config.region}",
        }
        assert spec.config_schema["required"] == ["region"]
        assert spec.config_schema["properties"]["weather_units"]["enum"] == [
            "metric",
            "imperial",
        ]
        # No tool is classed read: a ReadOnly binding sees none.
        assert mcp["tools"] == {"read": []}
        assert mcp["credential"] is None and spec.credential_slots == ()

    def test_a_secret_header_is_the_connector_s_one_credential(self):
        transport = {
            "type": "streamable-http",
            "url": "http://localhost:8080/mcp",
            "headers": [
                {"name": "Authorization", "value": "Bearer {token}", "isSecret": True}
            ],
        }
        spec_json, _ = spec_from_server_json(_server(transport=transport))
        spec = spec_from_json(spec_json)
        assert spec.service.mcp["credential"] == {
            "header": "Authorization",
            "scheme": "Bearer",
        }
        assert [slot.name for slot in spec.credential_slots] == ["token"]
        # The front's own port moves out of the server's way.
        assert spec.service.port != spec.service.mcp["port"]

    def test_the_older_identifier_form_is_read(self):
        package = {
            "registryType": "oci",
            "registryBaseUrl": "https://docker.io",
            "identifier": "acme/weather",
            "version": "2.0.0",
        }
        assert image_reference(package) == "docker.io/acme/weather:2.0.0"

    @pytest.mark.parametrize(
        ("name", "version", "expected"),
        [
            ("io.github.acme/weather", "1.4.2", "io.github.acme.weather/v1"),
            ("com.example/Tool_X", "3.0.0", "com.example.tool-x/v3"),
            ("io.github.acme/weather", "0.9.0", "io.github.acme.weather/v1"),
            ("io.github.9acme/w", None, "io.github.acme.w/v1"),
        ],
    )
    def test_driver_names(self, name, version, expected):
        assert driver_name(name, version) == expected


class TestRefusals:
    @pytest.mark.parametrize(
        ("server", "message"),
        [
            (
                {"name": "a.b/c", "packages": [{"registryType": "npm"}]},
                "npm packages are unsupported",
            ),
            (
                {"name": "a.b/c", "remotes": [{"type": "streamable-http"}]},
                "remote servers are external MCP connectors",
            ),
            ({"name": "abc", "packages": []}, "no oci package"),
        ],
    )
    def test_what_is_not_an_oci_image(self, server, message):
        with pytest.raises(ServerJsonError, match=message):
            spec_from_server_json(server)

    def test_a_secret_environment_variable(self):
        server = _server(environmentVariables=[{"name": "API_KEY", "isSecret": True}])
        with pytest.raises(ServerJsonError, match="API_KEY"):
            spec_from_server_json(server)

    def test_a_templated_url(self):
        transport = {"type": "streamable-http", "url": "http://localhost:{port}/mcp"}
        with pytest.raises(ServerJsonError, match="templated"):
            spec_from_server_json(_server(transport=transport))

    def test_sse(self):
        with pytest.raises(ServerJsonError, match="sse"):
            spec_from_server_json(_server(transport={"type": "sse", "url": "x"}))

    def test_several_oci_packages_need_a_choice(self):
        server = _server()
        server["packages"].append(dict(server["packages"][0]))
        with pytest.raises(ServerJsonError, match="name one"):
            spec_from_server_json(server)
        _spec, reference = spec_from_server_json(server, package=1)
        assert reference == "ghcr.io/acme/weather:1.4.2"
