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
from shared.connectors.env_names import driver_env_problem
from shared.connectors.registration import (
    MAX_BINDING_ENTRIES,
    MAX_SCHEMA_BYTES,
    MAX_SCHEMA_DEPTH,
    custom_driver_problems,
    declared_env_names,
    image_binding_problems,
    moved_spec_problems,
    repository_trusted,
    reserved_name,
    schema_problems,
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
    "env_names": ["EXAMPLE_TOKEN", "EXAMPLE_TOKEN_FILE"],
    "config_schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {"file": {"type": "boolean"}},
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


NAMES = tuple(EXAMPLE["env_names"])


def _json(**over):
    value = copy.deepcopy(EXAMPLE)
    value.update(over)
    return value


def _spec(**over):
    return spec_from_json(_json(**over))


def _problems(**over):
    value = _json(**over)
    return custom_driver_problems(
        spec_from_json(value),
        privileged=False,
        env_names=declared_env_names(value),
    )


class TestTheSpecAsJson:
    def test_the_example_reads_and_is_valid(self):
        spec = _spec()
        assert validate_spec(spec) == []
        assert _problems() == []
        assert declared_env_names(EXAMPLE) == NAMES
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

    @pytest.mark.parametrize(
        ("reference", "trusted", "expected"),
        [
            # The D6 review's trust probe (d6-review/probe_trust.py).
            ("ghcr.io/org-evil/x:1", ["ghcr.io/org"], False),
            ("ghcr.io/orgx/y", ["ghcr.io/org"], False),
            ("evil.io/ghcr.io/org/x", ["ghcr.io/org"], False),
            ("GHCR.IO/org/x:1", ["ghcr.io/org"], False),
            ("ghcr.io/org/x@sha256:" + "a" * 64, ["ghcr.io/org/"], True),
            ("ghcr.io/org/x", ["ghcr.io/org:latest"], True),
            ("localhost:5005/a/b", ["localhost:5005/a"], True),
            # Docker Hub by any of its names.
            ("acme/x", ["docker.io/acme"], True),
            ("acme/x", ["index.docker.io/acme"], True),
            ("index.docker.io/acme/x", ["docker.io/acme"], True),
            # A bare name is an official image, never an organisation.
            ("acme/x", ["acme"], False),
            # A registry alone, a wildcard or nothing trusts nothing.
            ("ghcr.io/org/x", ["ghcr.io"], False),
            ("localhost:5005/a/b", ["localhost:5005"], False),
            ("ghcr.io/org/x", ["*"], False),
            ("ghcr.io/org/x", [""], False),
        ],
    )
    def test_the_review_s_cases(self, reference, trusted, expected):
        assert repository_trusted(reference, trusted) is expected


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


def _check(*entries, spec=None, names=NAMES, driver="example.env/v1"):
    return image_binding_problems(
        _descriptor(*entries, driver=driver), spec or _spec(), env_names=names
    )


class TestWhatABindReturns:
    def test_env_and_files_to_the_workspace_are_delivered(self):
        assert _check(_env(), _file(mode=0o600, env_var="EXAMPLE_TOKEN_FILE")) == []

    def test_the_descriptor_schema_applies(self):
        problems = image_binding_problems({"driver": "example.env/v1"}, _spec())
        assert problems

    def test_it_names_its_own_driver(self):
        problems = _check(_env(), driver="other.env/v1")
        assert any("names driver" in p for p in problems)

    def test_only_the_workspace_receives_it(self):
        problems = _check(_env(recipient="harness"))
        assert any("workspace" in p for p in problems)

    def test_only_declared_forms(self):
        problems = _check(_file(), spec=_spec(delivery_forms=["env_file"]))
        assert any("credential_file" in p for p in problems)

    def test_files_are_never_executable_nor_transformed(self):
        problems = _check(_file(mode=0o755))
        assert any("executable" in p for p in problems)
        problems = _check(_file(transform="kubeconfig_prefix"))
        assert any("SRW's own" in p for p in problems)

    def test_entries_are_bounded(self):
        names = tuple(f"V{index}" for index in range(MAX_BINDING_ENTRIES + 1))
        entries = [_env(name=name) for name in names]
        problems = _check(*entries, names=names)
        assert any("at most" in p for p in problems)

    def test_a_name_set_twice_is_refused(self):
        assert any("twice" in p for p in _check(_env(), _env()))
        problems = _check(_env(), _file(env_var="EXAMPLE_TOKEN"))
        assert any("twice" in p for p in problems)
        with pytest.raises(ValueError, match="twice"):
            wire_credentials(_descriptor(_env("A"), _env("A")))

    def test_values_the_workspace_takes(self):
        problems = _check(_env(value="a\x00b"))
        assert any("NUL" in p for p in problems)
        problems = _check(_env(value="x" * 65537))
        assert any("64 KiB" in p for p in problems)
        assert not any("x" * 20 in p for p in problems)

    def test_the_wire_shape_is_the_env_and_file_connectors(self):
        wire = wire_credentials(
            _descriptor(
                _env("A", "1"),
                _env("B", "2"),
                _file(path="~/.srw-files/a/credentials", mode=0o640, env_var="C"),
            )
        )
        assert wire == {
            "env_vars": {"A": "1", "B": "2"},
            "files": [
                {
                    "name": "credentials",
                    "target_path": "/home/srw/.srw-files/a/credentials",
                    "contents": "c",
                    "mode": "0640",
                    "env_var": "C",
                }
            ],
        }


#: What the D6 review showed a driver could set before the strict list
#: (scratchpad d6-review/probe_env.py): each runs code, redirects traffic or
#: loosens TLS in the consumer's workspace.
DENIED = [
    "GIT_SSH_COMMAND",
    "GIT_CONFIG_COUNT",
    "GIT_CONFIG_KEY_0",
    "GIT_CONFIG_VALUE_0",
    "GIT_ASKPASS",
    "GIT_PAGER",
    "GIT_DIR",
    "GIT_WORK_TREE",
    "GIT_EXEC_PATH",
    "GIT_SSL_NO_VERIFY",
    "NODE_OPTIONS",
    "PERL5OPT",
    "RUBYOPT",
    "JAVA_TOOL_OPTIONS",
    "_JAVA_OPTIONS",
    "JDK_JAVA_OPTIONS",
    "NPM_CONFIG_USERCONFIG",
    "npm_config_registry",
    "PIP_INDEX_URL",
    "UV_INDEX_URL",
    "EDITOR",
    "VISUAL",
    "PAGER",
    "LESSOPEN",
    "LESSCLOSE",
    "BROWSER",
    "SSH_ASKPASS",
    "SUDO_ASKPASS",
    "SSH_AUTH_SOCK",
    "HTTPS_PROXY",
    "https_proxy",
    "ALL_PROXY",
    "no_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "KUBECONFIG",
    "DOCKER_CONFIG",
    "DOCKER_HOST",
    "XDG_CONFIG_HOME",
    "GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES",
    "TMPDIR",
    "HISTFILE",
    "DOTNET_STARTUP_HOOKS",
    "BUN_INSTALL",
    "OPENSSL_CONF",
    "GLIBC_TUNABLES",
    "AWS_CONFIG_FILE",
    "CURL_HOME",
    "PSQLRC",
    "MAKEFLAGS",
    "CC",
    "PS1",
    "GOPROXY",
    "CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_RUNNER",
    "KUBE_EDITOR",
    "GH_PAGER",
    # what an environment connector is refused too
    "LD_PRELOAD",
    "PYTHONPATH",
    "PATH",
    "HOME",
    "BASH_ENV",
    "PROMPT_COMMAND",
    "SRW_X",
    "srw_x",
]
ALLOWED = [
    "EXAMPLE_TOKEN",
    "GITHUB_TOKEN",
    "GITLAB_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "PGPASSWORD",
    "PGHOST",
    "ANTHROPIC_API_KEY",
    "TERM",
]


class TestEnvironmentNames:
    @pytest.mark.parametrize("name", DENIED)
    def test_a_driver_may_not_set_what_runs_code_or_redirects(self, name):
        assert driver_env_problem(name) is not None
        # Refused at bind even were it declared...
        problems = _check(_env(name=name), names=(*NAMES, name))
        assert any(name in p for p in problems)
        # ...as a file's variable too...
        assert _check(_file(env_var=name), names=(*NAMES, name))
        # ...and a spec that declares it is never registered.
        declared = _problems(env_names=[*NAMES, name])
        assert any(name in p for p in declared)

    @pytest.mark.parametrize("name", ALLOWED)
    def test_ordinary_credentials_are_fine(self, name):
        assert driver_env_problem(name) is None
        assert _check(_env(name=name), names=(name,)) == []

    def test_a_name_the_spec_does_not_declare_is_refused(self):
        problems = _check(_env(name="EXAMPLE_UNDECLARED"))
        assert any("does not declare" in p for p in problems)
        problems = _check(_file(env_var="EXAMPLE_UNDECLARED"))
        assert any("does not declare" in p for p in problems)

    def test_no_declaration_refuses_every_variable(self):
        """A caller that forgets the names refuses, never delivers."""
        problems = image_binding_problems(_descriptor(_env()), _spec())
        assert any("does not declare" in p for p in problems)

    def test_a_driver_that_sets_variables_declares_them(self):
        assert any("env_names" in p for p in _problems(env_names=[]))
        # A driver returning files without a variable needs none.
        assert _problems(delivery_forms=["credential_file"], env_names=[]) == []

    def test_only_a_bind_time_driver_declares_names(self):
        server = spec_to_json(MCP_TEST_SPEC, env_names=["EXAMPLE_TOKEN"])
        server["name"] = "acme.mcp/v1"
        problems = custom_driver_problems(
            spec_from_json(server),
            privileged=False,
            env_names=declared_env_names(server),
        )
        assert any("only a bind-time driver" in p for p in problems)

    @pytest.mark.parametrize(
        ("names", "message"),
        [
            ("EXAMPLE_TOKEN", "list of strings"),
            (["A", "A"], "twice"),
            (["1A"], "not a variable name"),
            ([f"V{index}" for index in range(101)], "at most"),
        ],
    )
    def test_the_declaration_reads_strictly(self, names, message):
        with pytest.raises(ValueError, match=message):
            _spec(env_names=names)


class TestFileTargets:
    @pytest.mark.parametrize(
        "path",
        ["~/.srw-files/example/token", "~/.srw-files/x", "~/.netrc", "~/.pgpass"],
    )
    def test_a_driver_s_file_lands_in_srw_s_directory_or_a_login_file(self, path):
        assert _check(_file(path=path)) == []

    @pytest.mark.parametrize(
        "path",
        [
            # Credential-file locations whose formats run a command, until the
            # owner rules on shared env and file connectors.
            "~/.kube/config",
            "~/.aws/config",
            "~/.aws/credentials",
            "~/.docker/config.json",
            "~/.config/helm/repositories.yaml",
            "~/.config/gcloud/credentials.db",
            # Never a credential-file location at all.
            "~/.gitconfig",
            "~/.bashrc",
            "~/.ssh/config",
            "~/.srw-credentials/leases/x",
            "~/.config/git/config",
            "/etc/passwd",
            "~/.srw-files/../.bashrc",
        ],
    )
    def test_everything_else_is_refused(self, path):
        problems = _check(_file(path=path))
        assert any("path is refused" in p for p in problems)


#: The D6 review's ReDoS probe (d6-review/probe_redos.py): this pattern
#: doubles its time per character on the event loop.
REDOS = {
    "type": "object",
    "properties": {"a": {"type": "string", "pattern": "^(a+)+$"}},
}


class TestRegisteredSchemas:
    def test_a_regular_expression_is_refused(self):
        assert any("pattern" in p for p in _problems(config_schema=REDOS))
        slot = copy.deepcopy(EXAMPLE["credential_slots"][0])
        slot["schema"]["properties"]["token"]["pattern"] = "^(a+)+$"
        assert any("pattern" in p for p in _problems(credential_slots=[slot]))

    @pytest.mark.parametrize(
        "schema",
        [
            {"type": "object", "patternProperties": {"^x": {"type": "string"}}},
            {"type": "object", "properties": {"a": {"format": "regex"}}},
            {"type": "object", "propertyNames": {"pattern": "^(a|a)+$"}},
            {"allOf": [{"properties": {"a": {"pattern": "x"}}}]},
            {"items": [{"pattern": "x"}]},
            {"$defs": {"x": {"pattern": "x"}}, "$ref": "#/$defs/x"},
        ],
    )
    def test_every_place_a_regex_can_hide_is_found(self, schema):
        assert schema_problems(schema, "s")

    def test_a_property_named_pattern_is_data(self):
        schema = {
            "type": "object",
            "properties": {"pattern": {"type": "string"}},
            "required": ["pattern"],
            "default": {"pattern": 1},
        }
        assert schema_problems(schema, "s") == []

    def test_an_annotation_s_value_is_data(self):
        """With the allowlist a ``$ref`` reaches only ``$defs``, every one of
        them checked: what ``default`` or ``examples`` holds is never a
        schema, whatever keys it has."""
        schema = {
            "type": "object",
            "default": {"pattern": "^(a+)+$"},
            "examples": [{"$ref": "#/elsewhere"}],
        }
        assert schema_problems(schema, "s") == []

    def test_a_reference_stays_inside_the_schema(self):
        assert schema_problems({"$ref": "#/$defs/x", "$defs": {"x": {}}}, "s") == []
        problems = schema_problems({"$ref": "https://evil.example/s.json"}, "s")
        assert any("inside the schema" in p for p in problems)

    def test_size_and_depth_are_bounded(self):
        deep: dict = {"type": "string"}
        for _ in range(MAX_SCHEMA_DEPTH + 1):
            deep = {"type": "object", "properties": {"a": deep}}
        assert any("deeper" in p for p in schema_problems(deep, "s"))
        big = {
            "type": "object",
            "properties": {
                f"p{index}": {"type": "string", "description": "x" * 100}
                for index in range(MAX_SCHEMA_BYTES // 100)
            },
        }
        assert any("KiB" in p for p in schema_problems(big, "s"))


def _moved(**over):
    return _json(**over)


class TestAMovedTag:
    def test_the_same_contract_moves(self):
        assert moved_spec_problems(EXAMPLE, _moved(title="Renamed")) == []
        relaxed = _moved(env_names=["EXAMPLE_TOKEN"])
        assert moved_spec_problems(EXAMPLE, relaxed) == []

    @pytest.mark.parametrize("label", [None])
    def test_no_label_is_refused(self, label):
        problems = moved_spec_problems(EXAMPLE, label)
        assert any("no io.srw.driver.spec label" in p for p in problems)

    @pytest.mark.parametrize(
        ("over", "message"),
        [
            ({"name": "other.env/v1"}, "declares driver"),
            ({"plane": "service"}, "plane changed"),
            ({"protocol_version": "2.0"}, "protocol"),
            ({"credential_slots": []}, "disappeared"),
            ({"env_names": [*NAMES, "EXAMPLE_MORE"]}, "new environment names"),
            (
                {"delivery_forms": ["env_file", "credential_file", "lease_token"]},
                "new forms",
            ),
            (
                {"egress": [{"host": "api.example.com", "ports": [443]}]},
                "new egress",
            ),
            ({"credential_delivery": 7}, "malformed"),
        ],
    )
    def test_a_changed_contract_is_refused(self, over, message):
        problems = moved_spec_problems(EXAMPLE, _moved(**over))
        assert any(message in p for p in problems), problems

    def test_a_new_required_slot_or_a_changed_slot_is_refused(self):
        slot = copy.deepcopy(EXAMPLE["credential_slots"][0])
        extra = {**copy.deepcopy(slot), "name": "second", "required": True}
        problems = moved_spec_problems(EXAMPLE, _moved(credential_slots=[slot, extra]))
        assert any("new credential slot is required: second" in p for p in problems)
        optional = {**extra, "required": False}
        assert (
            moved_spec_problems(EXAMPLE, _moved(credential_slots=[slot, optional]))
            == []
        )
        changed = copy.deepcopy(slot)
        changed["schema"]["required"] = ["token"]
        problems = moved_spec_problems(EXAMPLE, _moved(credential_slots=[changed]))
        assert any("changed its kind or schema" in p for p in problems)


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


# =============================================================================
# The D6 re-review's probes (scratchpad d6-rereview/)
# =============================================================================

#: probe_env2.py and the re-review's list: names a tool reads to run code
#: (an option, a flag, a command, a config file or a home) that the first
#: list let through. The list is a best-effort lint, never a sandbox.
REREVIEW_DENIED = [
    "git_ssh_command",
    "Git_Ssh_Command",
    "ERL_AFLAGS",
    "ERL_FLAGS",
    "ELIXIR_ERL_OPTIONS",
    "CFLAGS",
    "CXXFLAGS",
    "CPPFLAGS",
    "LDFLAGS",
    "CGO_CFLAGS",
    "CGO_LDFLAGS",
    "GNUMAKEFLAGS",
    "MFLAGS",
    "JAVA_OPTS",
    "MAVEN_OPTS",
    "MAVEN_ARGS",
    "GRADLE_OPTS",
    "GRADLE_USER_HOME",
    "SBT_OPTS",
    "ANT_OPTS",
    "JAVA_HOME",
    "FZF_DEFAULT_COMMAND",
    "NODE_REPL_EXTERNAL_MODULE",
    "CONFIG_SITE",
    "CONFIG_SHELL",
    "CMAKE_TOOLCHAIN_FILE",
    "IPYTHONDIR",
    "VIM",
    "HGRCPATH",
    "ANSIBLE_CONFIG",
    "ANSIBLE_LIBRARY",
    "YARN_YARN_PATH",
    "YARN_RC_FILENAME",
    "YARN_NPM_REGISTRY_SERVER",
    "COREPACK_NPM_REGISTRY",
    "COMPOSER",
    "COMPOSER_HOME",
    "RIPGREP_CONFIG_PATH",
    "BASH_COMPLETION_USER_FILE",
    "CLOUDSDK_PYTHON",
    "TF_CLI_CONFIG_FILE",
    "JUPYTER_CONFIG_DIR",
    "JULIA_DEPOT_PATH",
    "DENO_CERT",
    "DENO_DIR",
    "MAILCAPS",
    "LOCPATH",
    "NLSPATH",
    "TERMINFO",
    "GH_BROWSER",
    "GH_CONFIG_DIR",
    "BOTO_CONFIG",
    "CONDARC",
    "POETRY_REPOSITORIES_X_URL",
    "PIPENV_PYPI_MIRROR",
    "HELM_DATA_HOME",
    "PERL5DB",
    "LUA_INIT",
    "NETRC",
    "LESS",
    "MANOPT",
    "PS0",
    "GLOBIGNORE",
    "SHELLOPTS",
    "BASHOPTS",
    "ELECTRON_RUN_AS_NODE",
    "RUBYGEMS_GEMDEPS",
    # The suffixes, on names no list spells out.
    "ACME_OPTS",
    "ACME_OPTIONS",
    "ACME_COMMAND",
    "ACME_ARGS",
    "INPUTRC",
    "WGETRC",
    "ACME_RCPATH",
    "ACME_CONFIG",
    "ACME_CONFIG_FILE",
    "ACME_CONFIG_PATH",
    "ACME_CONFIG_DIR",
    "ACME_HOME",
]
#: Credential-shaped names stay a driver's to set.
CREDENTIAL_SHAPED = [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "PGPASSWORD",
    "PGUSER",
    "PGDATABASE",
    "DATABASE_URL",
    "GITHUB_TOKEN",
    "GH_TOKEN",
    "OPENAI_API_KEY",
    "AZURE_CLIENT_SECRET",
    "AZURE_CLIENT_ID",
    "AZURE_TENANT_ID",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "HF_TOKEN",
    "REDIS_URL",
    "MONGODB_URI",
    "EXAMPLE_TOKEN_FILE",
]


class TestTheOneEnvironmentList:
    @pytest.mark.parametrize("name", REREVIEW_DENIED)
    def test_known_tool_hooks_are_refused(self, name):
        assert driver_env_problem(name) is not None
        problems = _check(_env(name=name), names=(*NAMES, name))
        assert any(name in p for p in problems)
        declared = _problems(env_names=[*NAMES, name])
        assert any(name in p for p in declared)

    @pytest.mark.parametrize("name", CREDENTIAL_SHAPED)
    def test_credential_shaped_names_stay_allowed(self, name):
        assert driver_env_problem(name) is None
        assert _check(_env(name=name), names=(name,)) == []
        assert _problems(env_names=[name]) == []

    def test_a_name_is_at_most_128_characters(self):
        assert driver_env_problem("A" * 128) is None
        long = "A" * 129
        problems = _check(_env(name=long), names=(long,))
        assert any("at most 128" in p for p in problems)
        assert not any(long in p for p in problems)
        with pytest.raises(ValueError, match="at most 128"):
            _spec(env_names=["A" * 5000])

    def test_an_mcp_server_s_own_process_is_checked_against_the_bridge_s_list(
        self,
    ):
        """D6 re-review 2: D5b's templated and credential variables set the
        MCP server's own process, never a workspace: they are checked
        against CODE_ENV (the bridge's own list, kept in env_names), not the
        workspace's longer one."""
        from shared.connectors import env_names, mcp

        assert mcp.CODE_ENV is env_names.CODE_ENV
        assert mcp.CODE_ENV_PREFIXES is env_names.CODE_ENV_PREFIXES
        assert env_names.CODE_ENV <= env_names.DRIVER_DENIED_NAMES
        for name in ("NODE_OPTIONS", "PYTHONPATH", "GIT_SSH_COMMAND", "PIP_INDEX_URL"):
            assert mcp.code_env(name)
        for name in ("NODE_ENV", "JAVA_HOME", "DATA_HOME", "FEATURE_FLAGS"):
            assert not mcp.code_env(name)
            assert env_names.loads_code(name)
        for name in CREDENTIAL_SHAPED:
            assert not mcp.code_env(name)

    def test_the_list_says_it_is_a_lint_and_who_is_trusted(self):
        from shared.connectors import env_names, registration

        doc = " ".join(env_names.__doc__.split())
        assert "best-effort lint" in doc
        assert "trust boundary" in doc
        assert "cannot run code" not in doc
        assert "nothing it names may run code" not in doc
        assert "trust boundary" in " ".join(registration.__doc__.split())


#: probe_schema_ref.py: a ``$ref`` to a key no structural walk visits, an
#: anchor, a keyword the walk did not know. ``EVIL`` doubles its time per
#: character in the validator.
EVIL = {"type": "string", "pattern": "^(a+)+$"}
REF_ESCAPES = {
    "ref_to_unknown_key": {
        "type": "object",
        "properties": {"a": {"$ref": "#/stash"}},
        "stash": EVIL,
    },
    "ref_to_anchor_in_unknown": {
        "type": "object",
        "properties": {"a": {"$ref": "#x"}},
        "stash": {"$anchor": "x", **EVIL},
    },
    "ref_to_defs": {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/x"}},
        "$defs": {"x": EVIL},
    },
    "ref_to_definitions": {
        "type": "object",
        "properties": {"a": {"$ref": "#/definitions/x"}},
        "definitions": {"x": EVIL},
    },
    "ref_into_enum_value": {
        "type": "object",
        "properties": {"a": {"$ref": "#/properties/b/enum/0"}},
    },
    "dependentSchemas": {
        "type": "object",
        "dependentSchemas": {"a": {"properties": {"a": EVIL}}},
    },
    "contentSchema": {
        "type": "object",
        "properties": {
            "a": {"contentMediaType": "application/json", "contentSchema": EVIL}
        },
    },
    "items_obj": {
        "type": "object",
        "properties": {"a": {"type": "array", "items": EVIL}},
    },
    "anchor_alone": {
        "type": "object",
        "properties": {"a": {"$ref": "#x"}},
        "$defs": {"y": {"$anchor": "x", "type": "string"}},
    },
    "id": {"$id": "https://evil.example/s", "type": "object"},
    "dynamic_anchor": {"$dynamicAnchor": "x", "type": "object"},
    "dynamic_ref": {"$dynamicRef": "#x", "type": "object"},
    "recursive_ref": {"$recursiveRef": "#", "type": "object"},
    "ref_to_root": {"type": "object", "properties": {"a": {"$ref": "#"}}},
    "ref_to_a_property": {
        "type": "object",
        "properties": {"a": {"$ref": "#/properties/b"}, "b": {"type": "string"}},
    },
    "ref_to_a_missing_def": {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/missing"}},
        "$defs": {"x": {"type": "string"}},
    },
    "ref_with_pointer_escape": {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/a~1b"}},
        "$defs": {"a/b": {"type": "string"}},
    },
    "other_dialect": {
        "$schema": "https://evil.example/meta",
        "type": "object",
    },
    "format_regex_under_unknown_key": {
        "type": "object",
        "stash": {"type": "string", "format": "regex"},
    },
    "pattern_properties_under_unknown_key": {
        "type": "object",
        "stash": {"patternProperties": {"^(a+)+$": {}}},
    },
}


class TestAReferenceReachesOnlyWhatWasChecked:
    @pytest.mark.parametrize("case", sorted(REF_ESCAPES))
    def test_the_config_schema_is_refused(self, case):
        problems = _problems(config_schema=REF_ESCAPES[case])
        assert problems, case

    @pytest.mark.parametrize("case", sorted(REF_ESCAPES))
    def test_a_credential_slot_schema_is_refused(self, case):
        slot = copy.deepcopy(EXAMPLE["credential_slots"][0])
        slot["schema"] = copy.deepcopy(REF_ESCAPES[case])
        assert _problems(credential_slots=[slot]), case

    def test_a_reference_into_its_own_defs_is_fine(self):
        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "a": {"$ref": "#/$defs/host"},
                "b": {"$ref": "#/$defs/port"},
            },
            "$defs": {
                "host": {"type": "string", "maxLength": 253},
                "port": {"type": "integer"},
            },
        }
        assert schema_problems(schema, "s") == []
        assert _problems(config_schema=schema) == []
        legacy = {**schema, "definitions": {"port": {"type": "integer"}}}
        assert any("definitions" in p for p in schema_problems(legacy, "s"))

    @pytest.mark.asyncio
    async def test_the_registered_driver_never_runs_an_unsafe_stored_schema(self):
        """probe_schema_reg.py: a schema that slipped past registration
        (stored before this check) is refused before the validator runs."""
        import time

        from fastapi import HTTPException

        from orchestrator.services.connector_drivers.registered import (
            RegisteredImageDriver,
        )

        value = _json(config_schema=REF_ESCAPES["ref_to_unknown_key"])
        assert _problems(**value)

        class Registration:
            spec = spec_from_json(value)
            image_reference = "ghcr.io/acme/driver:1"

        driver = RegisteredImageDriver(Registration())
        started = time.monotonic()
        with pytest.raises(HTTPException) as caught:
            await driver._refuse_invalid(
                Registration.spec.config_schema, {"a": "a" * 40 + "!"}, "config"
            )
        assert caught.value.status_code == 400
        assert time.monotonic() - started < 1.0


class TestReReviewFiles:
    """probe_files.py."""

    def test_one_path_is_written_once(self):
        problems = _check(_file(), _file())
        assert any("another entry writes too" in p for p in problems)
        problems = _check(_file("~/.srw-files/x"), _file("~/.srw-files/./x"))
        assert any("another entry writes too" in p for p in problems)
        assert _check(_file("~/.srw-files/x"), _file("~/.srw-files/y")) == []

    def test_a_path_is_bounded(self):
        problems = _check(_file("~/.srw-files/" + "a" * 300))
        assert any("longer than 255" in p for p in problems)

    @pytest.mark.parametrize(
        "content",
        [
            "machine a login b password c\nmacdef init\n!id\n\n",
            "macdef init\n!id\n\n",
            "machine a login b password c macdef init\n",
            "machine a\tmacdef\tinit\n",
        ],
    )
    def test_a_netrc_defines_no_macro(self, content):
        entry = _file("~/.netrc")
        entry["value"]["content"] = content
        problems = _check(entry)
        assert any("macdef" in p for p in problems)
        assert not any("!id" in p for p in problems)

    def test_a_netrc_without_a_macro_and_a_pgpass_are_fine(self):
        entry = _file("~/.netrc")
        entry["value"]["content"] = "machine a login macdefault password c\n"
        assert _check(entry) == []
        pgpass = _file("~/.pgpass")
        pgpass["value"]["content"] = "host:5432:db:user:macdef\n"
        assert _check(pgpass) == []


class TestReReviewMovedTags:
    """probe_moved.py: what an execution or a connector may hold must not
    disappear under a moved tag, and the image reaches no further."""

    def test_a_dropped_access_level_is_refused(self):
        moved = _moved(access_levels=[EXAMPLE["access_levels"][1]])
        problems = moved_spec_problems(EXAMPLE, moved)
        assert any("drops access levels: ReadOnly" in p for p in problems)

    def test_a_narrowed_backend_list_is_refused(self):
        problems = moved_spec_problems(EXAMPLE, _moved(supported_backends=["sandbox"]))
        assert any("no longer supports workspace backends: vm" in p for p in problems)

    def test_a_widened_backend_list_moves(self):
        widened = _moved(supported_backends=["sandbox", "vm", "virtual"])
        assert not any("backends" in p for p in moved_spec_problems(EXAMPLE, widened))

    def test_newly_needing_dns_is_refused(self):
        problems = moved_spec_problems(EXAMPLE, _moved(needs_dns="resolves its API"))
        assert any("newly needs DNS" in p for p in problems)
        before = _json(needs_dns="resolves its API")
        assert moved_spec_problems(before, _moved(needs_dns="still")) == []


# =============================================================================
# The D6 re-review 2's probes (scratchpad d6-rereview2/)
# =============================================================================


def _chain(n: int, combinator: str = "anyOf", leaf=None) -> dict:
    """probe_schema2.py: each definition references the next twice, so the
    work doubles per level."""
    defs = {}
    for index in range(n):
        defs[f"a{index}"] = {
            combinator: [
                {"$ref": f"#/$defs/a{index + 1}"},
                {"$ref": f"#/$defs/a{index + 1}"},
            ]
        }
    defs[f"a{n}"] = leaf or {"type": "integer"}
    return {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/a0"}},
        "$defs": defs,
    }


EXPENSIVE = {
    "self_cycle": {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/a"}},
        "$defs": {"a": {"$ref": "#/$defs/a"}},
    },
    "mutual_cycle": {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/a"}},
        "$defs": {"a": {"$ref": "#/$defs/b"}, "b": {"$ref": "#/$defs/a"}},
    },
    "cycle_through_a_property": {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/tree"}},
        "$defs": {
            "tree": {
                "type": "object",
                "properties": {"child": {"$ref": "#/$defs/tree"}},
            }
        },
    },
    "anyOf_chain_18": _chain(18),
    "anyOf_chain_25": _chain(25),
    "oneOf_chain_20": _chain(20, "oneOf"),
    "allOf_chain_20": _chain(20, "allOf", {"type": "string"}),
    # probe_noref.py: no $ref, one wide allOf applied to every item.
    "wide_allOf_items": {
        "type": "object",
        "properties": {
            "x": {
                "type": "array",
                "items": {"allOf": [{"minimum": -index} for index in range(1400)]},
            }
        },
    },
    "nested_width_past_the_budget": {
        "type": "object",
        "properties": {
            "x": {
                "type": "array",
                "items": {
                    "anyOf": [
                        {"anyOf": [{"minLength": j} for j in range(10)]}
                        for _ in range(60)
                    ]
                },
            }
        },
    },
    "a_long_single_chain": {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/a0"}},
        "$defs": {
            **{
                f"a{index}": {"properties": {"y": {"$ref": f"#/$defs/a{index + 1}"}}}
                for index in range(40)
            },
            "a40": {"type": "string"},
        },
    },
    "uniqueItems": {
        "type": "object",
        "properties": {"x": {"type": "array", "uniqueItems": True}},
    },
    "unevaluatedProperties": {
        "type": "object",
        "unevaluatedProperties": False,
    },
    "unevaluatedItems": {
        "type": "object",
        "properties": {"x": {"type": "array", "unevaluatedItems": False}},
    },
    "ref_dot_dot": {
        "type": "object",
        "properties": {"x": {"$ref": "#/$defs/.."}},
        "$defs": {"..": {"type": "string"}},
    },
}


class TestAValidationsCostIsBounded:
    @pytest.mark.parametrize("case", sorted(EXPENSIVE))
    def test_the_config_schema_is_refused_fast(self, case):
        import time

        started = time.monotonic()
        assert _problems(config_schema=EXPENSIVE[case]), case
        assert time.monotonic() - started < 0.5

    @pytest.mark.parametrize("case", sorted(EXPENSIVE))
    def test_a_credential_slot_schema_is_refused(self, case):
        slot = copy.deepcopy(EXAMPLE["credential_slots"][0])
        slot["schema"] = copy.deepcopy(EXPENSIVE[case])
        assert _problems(credential_slots=[slot]), case

    def test_the_reasons_say_what_costs(self):
        assert any("loops" in p for p in schema_problems(EXPENSIVE["self_cycle"], "s"))
        assert any(
            "applies more than" in p
            for p in schema_problems(EXPENSIVE["anyOf_chain_25"], "s")
        )
        assert any(
            "holds more than" in p
            for p in schema_problems(EXPENSIVE["wide_allOf_items"], "s")
        )
        assert any(
            "deeper than" in p
            for p in schema_problems(EXPENSIVE["a_long_single_chain"], "s")
        )

    def test_shared_definitions_within_the_budget_are_fine(self):
        """A definition used twice costs twice, and fits the budget."""
        schema = {
            "type": "object",
            "properties": {
                "primary": {"$ref": "#/$defs/endpoint"},
                "fallback": {"$ref": "#/$defs/endpoint"},
            },
            "$defs": {
                "endpoint": {
                    "type": "object",
                    "properties": {
                        "host": {"$ref": "#/$defs/host"},
                        "port": {"type": "integer"},
                    },
                },
                "host": {"type": "string", "maxLength": 253},
            },
        }
        assert schema_problems(schema, "s") == []
        assert _problems(config_schema=schema) == []

    def test_what_is_validated_is_bounded_too(self):
        from shared.connectors.registration import (
            MAX_CONFIG_BYTES,
            MAX_INSTANCE_NODES,
            instance_problem,
        )

        assert instance_problem({"a": [1] * 10}, "the config") is None
        many = {"a": [1] * MAX_INSTANCE_NODES}
        assert "more than" in instance_problem(many, "the config")
        big = {"a": "x" * MAX_CONFIG_BYTES}
        assert instance_problem(big, "the config") is None
        assert "KiB" in instance_problem(big, "the config", max_bytes=MAX_CONFIG_BYTES)

    @pytest.mark.asyncio
    async def test_a_cycle_stored_before_this_check_is_a_400_not_a_500(self):
        """probe_schema_reg2.py: a looping $ref gave a RecursionError (500)."""
        from fastapi import HTTPException

        from orchestrator.services.connector_drivers.registered import (
            RegisteredImageDriver,
        )

        value = _json(config_schema=EXPENSIVE["self_cycle"])

        class Registration:
            spec = spec_from_json(value)
            image_reference = "ghcr.io/acme/driver:1"

        driver = RegisteredImageDriver(Registration())
        with pytest.raises(HTTPException) as caught:
            await driver._refuse_invalid(
                Registration.spec.config_schema, {"x": 1}, "config"
            )
        assert caught.value.status_code == 400

    @pytest.mark.asyncio
    async def test_a_validation_runs_off_the_loop_and_has_a_deadline(self, monkeypatch):
        import asyncio
        import threading
        import time

        from fastapi import HTTPException

        from orchestrator.services.connector_drivers import registered

        class Registration:
            spec = _spec()
            image_reference = "ghcr.io/acme/driver:1"

        driver = registered.RegisteredImageDriver(Registration())
        loop_thread = threading.get_ident()
        seen: list[int] = []

        def slow(schema, value):
            seen.append(threading.get_ident())
            time.sleep(0.5)
            return None

        monkeypatch.setattr(registered, "_first_error", slow)
        monkeypatch.setattr(registered, "VALIDATION_SECONDS", 0.1)
        ticks = 0

        async def ticker():
            nonlocal ticks
            while True:
                await asyncio.sleep(0.01)
                ticks += 1

        running = asyncio.create_task(ticker())
        try:
            with pytest.raises(HTTPException) as caught:
                await driver._refuse_invalid({"type": "object"}, {}, "config")
        finally:
            running.cancel()
        # Too slow is SRW's trouble, never the caller's mistake: retry.
        assert caught.value.status_code == 503
        assert "took longer than" in caught.value.detail
        assert seen and seen[0] != loop_thread
        # The loop kept running while the validation did.
        assert ticks >= 3

    @pytest.mark.asyncio
    async def test_a_stored_config_is_bounded_in_size(self):
        from fastapi import HTTPException

        from orchestrator.services.connector_drivers.registered import (
            RegisteredImageDriver,
        )
        from shared.connectors.registration import MAX_CONFIG_BYTES

        schema = {
            "type": "object",
            "properties": {"note": {"type": "string"}},
        }

        class Registration:
            spec = _spec(config_schema=schema)
            image_reference = "ghcr.io/acme/driver:1"

        driver = RegisteredImageDriver(Registration())
        with pytest.raises(HTTPException) as caught:
            await driver._refuse_invalid(
                schema,
                {"note": "x" * MAX_CONFIG_BYTES},
                "config",
                max_bytes=MAX_CONFIG_BYTES,
            )
        assert "KiB" in caught.value.detail


#: probe_serverjson.py: variables a server.json import sets for the MCP
#: server's own process, which the workspace list refused.
SERVER_PROCESS_NAMES = [
    "WEATHER_UNITS",
    "NODE_ENV",
    "LOG_LEVEL",
    "JAVA_HOME",
    "DATA_HOME",
    "FEATURE_FLAGS",
    "OPENAPI_MCP_HEADERS",
    "REDIS_CONFIG",
    "TF_WORKSPACE",
    "DATA_SRC",
    "SEARCH_OPTIONS",
    "DOTNET_ENVIRONMENT",
    "ANSIBLE_HOST",
    "NODE_AUTH_TOKEN",
    "DOCKER_HOST",
    "KUBECONFIG",
]


class TestAnMcpServersOwnVariables:
    @pytest.mark.parametrize("name", SERVER_PROCESS_NAMES)
    def test_a_server_json_import_sets_them_again(self, name):
        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).parent))
        from test_connector_server_json import _server

        from shared.connectors.server_json import spec_from_server_json

        server = _server(environmentVariables=[{"name": name, "description": "x"}])
        spec_json, _ = spec_from_server_json(server)
        assert custom_driver_problems(spec_from_json(spec_json), privileged=False) == []

    @pytest.mark.parametrize("name", ["NODE_OPTIONS", "PYTHONPATH", "GIT_SSH_COMMAND"])
    def test_the_bridge_s_own_list_still_holds(self, name):
        import sys
        from pathlib import Path

        sys.path.insert(0, str(Path(__file__).parent))
        from test_connector_server_json import _server

        from shared.connectors.server_json import spec_from_server_json

        server = _server(environmentVariables=[{"name": name, "description": "x"}])
        try:
            spec_json, _ = spec_from_server_json(server)
        except ValueError:
            return  # refused at import
        problems = custom_driver_problems(spec_from_json(spec_json), privileged=False)
        assert problems


#: D6 re-review 2 (b): credentials within a prefix family, and the names the
#: list now spells out.
CREDENTIALS_IN_A_FAMILY = [
    "NODE_AUTH_TOKEN",
    "CARGO_REGISTRY_TOKEN",
    "UV_PUBLISH_TOKEN",
    "GEM_HOST_API_KEY",
    "YARN_NPM_AUTH_TOKEN",
    "PIP_PASSWORD",
    "TF_TOKEN_APP_TERRAFORM_IO_TOKEN",
    "ANSIBLE_VAULT_PASSWORD",
    "CLOUDSDK_ACCESS_KEY",
    "DENO_AUTH_SECRET",
]
SPELLED_OUT = [
    "PGSSLMODE",
    "PGSSLROOTCERT",
    "GODEBUG",
    "RSYNC_RSH",
    "CVS_RSH",
    "SVN_SSH",
    "FCEDIT",
    "HGMERGE",
    "PIPX_DEFAULT_PYTHON",
    "INPUTRC",
    "CONDARC",
    "WGETRC",
    "NETRC",
    "PSQLRC",
]


class TestTheListsReach:
    @pytest.mark.parametrize("name", CREDENTIALS_IN_A_FAMILY)
    def test_a_credential_in_a_prefix_family_is_a_driver_s_to_set(self, name):
        assert driver_env_problem(name) is None
        assert _check(_env(name=name), names=(name,)) == []

    @pytest.mark.parametrize("name", SPELLED_OUT)
    def test_names_the_list_spells_out_are_refused(self, name):
        assert driver_env_problem(name) is not None

    @pytest.mark.parametrize(
        "name",
        [
            "PYTHON_TOKEN",
            "LD_PRELOAD_TOKEN",
            "DYLD_SECRET",
            "GIT_CONFIG_TOKEN",
            "SRW_TOKEN",
        ],
    )
    def test_the_workspace_s_own_families_spare_no_credential(self, name):
        assert driver_env_problem(name) is not None

    def test_an_exact_entry_is_refused_whatever_it_ends_with(self):
        from shared.connectors import env_names

        exact = [
            name
            for name in env_names.DRIVER_DENIED_NAMES
            if env_names.credential_shaped(name)
        ]
        for name in exact:
            assert driver_env_problem(name) is not None

    @pytest.mark.parametrize("name", ["DATA_SRC", "MY_SRC", "RESOURCE_SRC", "ORC"])
    def test_rc_is_named_never_matched_as_a_suffix(self, name):
        assert driver_env_problem(name) is None

    def test_the_exemption_is_documented(self):
        from shared.connectors import env_names

        doc = " ".join(env_names.__doc__.split())
        assert "credential-shaped" in doc
        assert "CODE_ENV" in doc


# =============================================================================
# The allowlist (D6 schema round)
# =============================================================================

#: Every keyword the allowlist refuses, with a value of its own shape.
REFUSED_KEYWORDS = {
    "pattern": "^a$",
    "patternProperties": {"^a": {"type": "string"}},
    "multipleOf": 3,
    "not": {"type": "string"},
    "if": {"type": "string"},
    "then": {"type": "string"},
    "else": {"type": "string"},
    "dependentSchemas": {"a": {"required": ["b"]}},
    "dependentRequired": {"a": ["b"]},
    "dependencies": {"a": ["b"]},
    "contains": {"type": "string"},
    "minContains": 1,
    "maxContains": 2,
    "propertyNames": {"maxLength": 3},
    "uniqueItems": True,
    "unevaluatedProperties": False,
    "unevaluatedItems": False,
    "prefixItems": [{"type": "string"}],
    "additionalItems": False,
    "contentSchema": {"type": "string"},
    "contentMediaType": "application/json",
    "contentEncoding": "base64",
    "definitions": {"a": {"type": "string"}},
    "$id": "https://example.com/s",
    "$anchor": "a",
    "$dynamicAnchor": "a",
    "$dynamicRef": "#a",
    "$recursiveAnchor": True,
    "$recursiveRef": "#",
    "$vocabulary": {"https://example.com": True},
    "x-anything": 1,
}


def _placed(where: str, keyword: str, value) -> dict:
    """A schema holding ``keyword`` at ``where``: the root, a property, an
    array's items, a combinator's branch, or a referenced definition."""
    inner = {"type": "string", keyword: value}
    if where == "root":
        return {"type": "object", keyword: value}
    if where == "property":
        return {"type": "object", "properties": {"a": inner}}
    if where == "items":
        return {
            "type": "object",
            "properties": {"a": {"type": "array", "items": inner}},
        }
    if where == "combinator":
        return {
            "type": "object",
            "properties": {"a": {"anyOf": [{"type": "integer"}, inner]}},
        }
    return {
        "type": "object",
        "properties": {"a": {"$ref": "#/$defs/d"}},
        "$defs": {"d": inner},
    }


PLACES = ("root", "property", "items", "combinator", "definition")


class TestTheKeywordAllowlist:
    @pytest.mark.parametrize("place", PLACES)
    @pytest.mark.parametrize("keyword", sorted(REFUSED_KEYWORDS))
    def test_every_refused_keyword_is_refused_anywhere(self, keyword, place):
        schema = _placed(place, keyword, REFUSED_KEYWORDS[keyword])
        problems = schema_problems(schema, "s")
        assert any(keyword in p for p in problems), problems
        # In a config schema and a credential slot alike.
        assert _problems(config_schema=schema)
        slot = copy.deepcopy(EXAMPLE["credential_slots"][0])
        slot["schema"] = copy.deepcopy(schema)
        assert _problems(credential_slots=[slot])

    def test_every_allowed_keyword_together_is_fine(self):
        from shared.connectors.registration import SCHEMA_KEYWORDS

        schema = {
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "$comment": "every allowed keyword",
            "title": "Config",
            "description": "d",
            "type": "object",
            "additionalProperties": False,
            "minProperties": 0,
            "maxProperties": 10,
            "required": ["host"],
            "default": {},
            "examples": [{"host": "h"}],
            "properties": {
                "host": {
                    "type": "string",
                    "format": "hostname",
                    "minLength": 1,
                    "maxLength": 253,
                    "x-srw-order": 1,
                    "x-srw-group": "Server",
                },
                "port": {
                    "type": "integer",
                    "minimum": 1,
                    "maximum": 65535,
                    "exclusiveMinimum": 0,
                    "exclusiveMaximum": 65536,
                },
                "mode": {"enum": ["a", "b", None, 1, True]},
                "fixed": {"const": "x", "readOnly": True, "deprecated": True},
                "token": {
                    "type": "string",
                    "writeOnly": True,
                    "x-srw-widget": "password",
                },
                "notes": {"type": "string", "x-srw-multiline": True},
                "tags": {
                    "type": "array",
                    "minItems": 0,
                    "maxItems": 5,
                    "items": {"$ref": "#/$defs/tag"},
                },
                "either": {"oneOf": [{"type": "string"}, {"type": "integer"}]},
                "both": {"allOf": [{"type": "string"}, {"maxLength": 9}]},
                "any": {"anyOf": [{"type": "null"}, {"type": "boolean"}]},
            },
            "$defs": {"tag": {"type": "string", "maxLength": 32}},
        }
        assert schema_problems(schema, "s") == []
        used = set()
        stack = [schema]
        while stack:
            node = stack.pop()
            if isinstance(node, dict):
                used.update(key for key in node if key in SCHEMA_KEYWORDS)
                for key in ("properties", "$defs"):
                    stack.extend((node.get(key) or {}).values())
                stack.extend(node.get(key) for key in ("items",) if key in node)
                for key in ("allOf", "anyOf", "oneOf"):
                    stack.extend(node.get(key) or [])
        assert used == set(SCHEMA_KEYWORDS)

    @pytest.mark.parametrize(
        ("schema", "needle"),
        [
            (
                {"type": "object", "additionalProperties": {"type": "string"}},
                "true or false",
            ),
            ({"type": "array", "items": [{"type": "string"}]}, "one schema"),
            ({"enum": [{"a": 1}]}, "strings, numbers"),
            ({"enum": list(range(257))}, "more than 256"),
            ({"const": {"a": 1}}, "a string, a number"),
            ({"format": "regex"}, "is one of"),
            ({"format": "idn-email"}, "is one of"),
            ({"type": "decimal"}, "JSON types"),
            ({"minLength": -1}, "non-negative"),
            ({"maxLength": True}, "non-negative"),
            ({"minimum": "1"}, "a number"),
            ({"required": "a"}, "property names"),
            ({"anyOf": []}, "lists schemas"),
            (
                {"properties": {"a": {"$defs": {"b": {}}, "type": "string"}}},
                "the root's only",
            ),
            (
                {
                    "properties": {
                        "a": {"$schema": "https://json-schema.org/draft/2020-12/schema"}
                    }
                },
                "the root's only",
            ),
        ],
    )
    def test_an_allowed_keyword_takes_only_its_cheap_shape(self, schema, needle):
        problems = schema_problems(schema, "s")
        assert any(needle in p for p in problems), problems

    def test_formats_are_annotations_srw_never_asserts(self):
        """SRW's validator runs without a format checker: an allowed format
        costs nothing, and refuses nothing."""
        from jsonschema import Draft202012Validator

        from shared.connectors.registration import SCHEMA_FORMATS

        for name in SCHEMA_FORMATS:
            validator = Draft202012Validator({"type": "string", "format": name})
            assert validator.format_checker is None
            assert list(validator.iter_errors("not a " + name)) == []


class TestValidationsRunInABoundedPool:
    @pytest.mark.asyncio
    async def test_concurrent_slow_validations_never_exceed_the_cap(self, monkeypatch):
        import asyncio
        import threading
        import time

        from orchestrator.services import connector_schema_validation as pool

        monkeypatch.setattr(pool, "SLOT_WAIT_SECONDS", 5.0)
        lock = threading.Lock()
        active = 0
        peak = 0

        def slow():
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.15)
            with lock:
                active -= 1
            return "ok"

        results = await asyncio.gather(
            *(pool.run_validation(slow, timeout=5.0) for _ in range(7))
        )
        assert results == ["ok"] * 7
        assert peak <= pool.SLOTS

    @pytest.mark.asyncio
    async def test_no_free_slot_in_time_is_busy(self, monkeypatch):
        import asyncio
        import time

        from orchestrator.services import connector_schema_validation as pool

        monkeypatch.setattr(pool, "SLOT_WAIT_SECONDS", 0.05)
        outcomes = await asyncio.gather(
            *(
                pool.run_validation(lambda: time.sleep(0.4), timeout=5.0)
                for _ in range(pool.SLOTS + 1)
            ),
            return_exceptions=True,
        )
        busy = [o for o in outcomes if isinstance(o, pool.ValidationBusy)]
        assert len(busy) == 1

    @pytest.mark.asyncio
    async def test_a_timed_out_validation_keeps_its_slot_until_its_thread_ends(
        self, monkeypatch
    ):
        import asyncio
        import threading
        import time

        from orchestrator.services import connector_schema_validation as pool

        monkeypatch.setattr(pool, "SLOT_WAIT_SECONDS", 0.05)
        release = threading.Event()
        stuck = [
            asyncio.create_task(pool.run_validation(release.wait, timeout=0.05))
            for _ in range(pool.SLOTS)
        ]
        for task in stuck:
            with pytest.raises(pool.ValidationTimeout):
                await task
        # The callers gave up; their threads still run, so every slot is
        # still taken: no third validation starts beside them.
        with pytest.raises(pool.ValidationBusy):
            await pool.run_validation(lambda: "never", timeout=1.0)
        release.set()
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            try:
                assert await pool.run_validation(lambda: "ran", timeout=1.0) == "ran"
                break
            except pool.ValidationBusy:
                continue
        else:
            raise AssertionError("the slots were never released")

    @pytest.mark.asyncio
    async def test_a_busy_registered_validation_is_a_503(self, monkeypatch):
        from fastapi import HTTPException

        from orchestrator.services import connector_schema_validation as pool
        from orchestrator.services.connector_drivers.registered import (
            RegisteredImageDriver,
        )

        async def busy(*_args, **_kwargs):
            raise pool.ValidationBusy("SRW is busy validating; retry shortly")

        monkeypatch.setattr(pool, "run_validation", busy)

        class Registration:
            spec = _spec()
            image_reference = "ghcr.io/acme/driver:1"

        driver = RegisteredImageDriver(Registration())
        with pytest.raises(HTTPException) as caught:
            await driver._refuse_invalid({"type": "object"}, {}, "config")
        assert caught.value.status_code == 503
        assert "retry" in caught.value.detail
