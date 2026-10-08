"""Registered image drivers: the spec as JSON, the rules, trust and bind output
(connector drivers D6, ``shared.connectors.registration``)."""

from __future__ import annotations

import copy

import pytest

from shared.connectors.builtin import (
    BUILTIN_SPECS,
    ECHO_SERVICE_SPEC,
    GENERIC_SPEC,
    IMAGE_DRIVER_SPEC,
    MCP_TEST_SPEC,
    spec_for_type,
)
from shared.connectors.contract import validate_spec
from shared.connectors.registration import (
    MAX_BINDING_ENTRIES,
    custom_driver_problems,
    image_binding_problems,
    repository_trusted,
    reserved_name,
    spec_from_json,
    spec_to_json,
    wire_credentials,
)

EXAMPLE = {
    "name": "example.env/v1",
    "title": "Example environment driver",
    "protocol_version": "1.0",
    "plane": "bind_time",
    "delivery_forms": ["env_file", "credential_file"],
    "config_schema": {
        "type": "object",
        "additionalProperties": False,
        "required": ["variable"],
        "properties": {"variable": {"type": "string"}},
    },
    "credential_slots": [
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
    ],
    "access_levels": [
        {
            "id": "ReadOnly",
            "rank": 0,
            "enforced_by": "Told to the agent only.",
            "advisory": True,
        },
        {"id": "ReadWrite", "rank": 1, "enforced_by": "The upstream decides."},
    ],
    "default_access": "ReadWrite",
    "supported_backends": ["sandbox", "vm"],
    "workspace_requirements": "A shell workspace.",
    "operations": ["gc"],
}


def _spec(**over):
    value = copy.deepcopy(EXAMPLE)
    value.update(over)
    return spec_from_json(value)


class TestTheSpecAsJson:
    def test_the_example_reads_and_is_valid(self):
        spec = _spec()
        assert validate_spec(spec) == []
        assert custom_driver_problems(spec, privileged=False) == []
        assert spec.credential_slots[0].name == "token"
        assert spec.operations == ("gc",)

    @pytest.mark.parametrize(
        "spec", [*BUILTIN_SPECS, ECHO_SERVICE_SPEC, MCP_TEST_SPEC], ids=lambda s: s.name
    )
    def test_shipped_specs_round_trip(self, spec):
        """Every key a label may carry survives JSON both ways (behaviour
        flags a label cannot set keep their defaults)."""
        again = spec_from_json(spec_to_json(spec))
        assert spec_to_json(again) == spec_to_json(spec)
        assert again.config_schema == spec.config_schema
        assert again.service == spec.service

    def test_an_unknown_key_is_refused_never_ignored(self):
        with pytest.raises(ValueError, match="unknown keys"):
            _spec(publishable=False)
        broken = copy.deepcopy(EXAMPLE)
        broken["credential_slots"][0]["names_field"] = "x"
        with pytest.raises(ValueError, match=r"credential_slots\[0\] has unknown"):
            spec_from_json(broken)

    @pytest.mark.parametrize(
        ("key", "value", "message"),
        [
            ("name", 7, "name must be"),
            ("delivery_forms", "env_file", "list of strings"),
            ("access_levels", {}, "must be a list"),
            ("config_schema", [], "must be an object"),
        ],
    )
    def test_a_wrong_shape_is_a_value_error(self, key, value, message):
        with pytest.raises(ValueError, match=message):
            _spec(**{key: value})


class TestTheRulesForARegisteredDriver:
    def test_srw_names_are_srw_s_own(self):
        assert reserved_name("srw.git/v1")
        assert not reserved_name("srwx.git/v1")
        problems = custom_driver_problems(_spec(name="srw.example/v1"), privileged=True)
        assert any("SRW's own" in problem for problem in problems)

    def test_an_image_never_runs_in_srw_s_process(self):
        problems = custom_driver_problems(_spec(plane="harness"), privileged=True)
        assert any("its own pod" in problem for problem in problems)

    def test_the_in_pod_plane_needs_trust_or_the_operator_s_switch(self):
        untrusted = custom_driver_problems(_spec(plane="in_pod"), privileged=False)
        assert any("trustedRepositories" in p for p in untrusted)
        trusted = custom_driver_problems(_spec(plane="in_pod"), privileged=True)
        assert any("not available in this release" in p for p in trusted)

    def test_a_bind_time_image_returns_env_and_files_only(self):
        problems = custom_driver_problems(
            _spec(delivery_forms=["env_file", "checkout"]), privileged=False
        )
        assert any("checkout" in p for p in problems)

    def test_a_bind_time_image_delivers_into_a_shell(self):
        problems = custom_driver_problems(
            _spec(supported_backends=["virtual"]), privileged=False
        )
        assert any("shell workspace" in p for p in problems)

    def test_a_bind_time_image_receives_credentials_inline(self):
        problems = custom_driver_problems(
            _spec(credential_delivery="lease"), privileged=False
        )
        assert any("inline" in p for p in problems)

    def test_a_service_image_is_a_managed_mcp_server(self):
        echo = spec_to_json(ECHO_SERVICE_SPEC)
        echo["name"] = "acme.echo/v1"
        problems = custom_driver_problems(spec_from_json(echo), privileged=False)
        assert any("managed MCP" in p for p in problems)
        server = spec_to_json(MCP_TEST_SPEC)
        server["name"] = "acme.mcp/v1"
        assert custom_driver_problems(spec_from_json(server), privileged=False) == []


class TestTrust:
    @pytest.mark.parametrize(
        ("reference", "trusted"),
        [
            ("ghcr.io/acme/driver:1.0", True),
            ("ghcr.io/acme/team/driver@sha256:" + "a" * 64, True),
            ("ghcr.io/acme", True),
            ("ghcr.io/acme-evil/driver:1.0", False),
            ("ghcr.io/acm/driver:1.0", False),
            ("docker.io/acme/driver", False),
            ("not a reference", False),
        ],
    )
    def test_repositories_match_on_a_path_boundary(self, reference, trusted):
        assert repository_trusted(reference, ["ghcr.io/acme"]) is trusted

    def test_docker_hub_names_normalize(self):
        assert repository_trusted("busybox:1.36", ["docker.io/library/busybox"])
        assert repository_trusted("busybox:1.36", ["busybox"])
        assert repository_trusted("acme/driver", ["docker.io/acme"])
        assert repository_trusted("docker.io/acme/driver:2", ["acme"]) is False
        assert repository_trusted("docker.io/acme/driver:2", ["acme/driver"])
        assert not repository_trusted("acme/driver", [])

    def test_a_full_reference_in_the_list_trusts_its_repository(self):
        assert repository_trusted("ghcr.io/acme/driver:2", ["ghcr.io/acme/driver:1"])


def _descriptor(*entries, driver="example.env/v1"):
    return {
        "driver": driver,
        "name": "example",
        "access": "ReadWrite",
        "entries": list(entries),
    }


def _env(name="EXAMPLE_TOKEN", value="v", recipient="workspace"):
    return {
        "recipient": recipient,
        "form": "env_file",
        "value": {"name": name, "value": value},
        "collision": "error",
    }


def _file(path="~/.srw-files/example/token", **extra):
    return {
        "recipient": "workspace",
        "form": "credential_file",
        "value": {"path": path, "content": "c", **extra},
        "collision": "skip_existing",
    }


class TestWhatABindReturns:
    def test_env_and_files_to_the_workspace_are_delivered(self):
        assert (
            image_binding_problems(_descriptor(_env(), _file(mode=0o600)), _spec())
            == []
        )

    def test_the_descriptor_schema_applies(self):
        problems = image_binding_problems({"driver": "example.env/v1"}, _spec())
        assert problems

    def test_it_names_its_own_driver(self):
        problems = image_binding_problems(
            _descriptor(_env(), driver="other.env/v1"), _spec()
        )
        assert any("names driver" in p for p in problems)

    def test_only_the_workspace_receives_it(self):
        problems = image_binding_problems(
            _descriptor(_env(recipient="harness")), _spec()
        )
        assert any("workspace" in p for p in problems)

    def test_only_declared_forms(self):
        spec = _spec(delivery_forms=["env_file"])
        problems = image_binding_problems(_descriptor(_file()), spec)
        assert any("credential_file" in p for p in problems)

    def test_files_land_only_where_credential_files_may(self):
        problems = image_binding_problems(_descriptor(_file(path="~/.bashrc")), _spec())
        assert any("path is refused" in p for p in problems)
        problems = image_binding_problems(_descriptor(_file(mode=0o755)), _spec())
        assert any("executable" in p for p in problems)
        problems = image_binding_problems(
            _descriptor(_file(transform="kubeconfig_prefix")), _spec()
        )
        assert any("SRW's own" in p for p in problems)

    def test_entries_are_bounded(self):
        entries = [_env(name=f"V{index}") for index in range(MAX_BINDING_ENTRIES + 1)]
        problems = image_binding_problems(_descriptor(*entries), _spec())
        assert any("at most" in p for p in problems)

    def test_the_wire_shape_is_the_env_and_file_connectors(self):
        wire = wire_credentials(
            _descriptor(
                _env("A", "1"),
                _env("B", "2"),
                _file(path="~/.aws/credentials", mode=0o640, env_var="AWS_SHARED"),
            )
        )
        assert wire == {
            "env_vars": {"A": "1", "B": "2"},
            "files": [
                {
                    "name": "credentials",
                    "target_path": "/home/srw/.aws/credentials",
                    "contents": "c",
                    "mode": "0640",
                    "env_var": "AWS_SHARED",
                }
            ],
        }

    def test_a_name_set_twice_is_refused(self):
        with pytest.raises(ValueError, match="twice"):
            wire_credentials(_descriptor(_env("A"), _env("A")))


class TestTheStoredType:
    def test_the_agent_resolves_the_stored_type_to_env_and_files(self):
        """The agent routes a registered driver's entry by these forms, with
        today's environment and credential-file materializers."""
        spec = spec_for_type("image_driver")
        assert spec is IMAGE_DRIVER_SPEC
        assert spec.delivery_forms == ("env_file", "credential_file")
        assert spec.supported_backends == GENERIC_SPEC.supported_backends
        assert validate_spec(spec) == []

    def test_no_catalogue_lists_it(self):
        assert IMAGE_DRIVER_SPEC not in BUILTIN_SPECS
