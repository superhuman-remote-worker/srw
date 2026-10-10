"""The one environment rule every connector follows (``shared.connectors.env_names``).

Connector drivers decisions 24 to 26 and 36: SRW's reserved names and the
known variables that make a tool run code are refused for SRW's own
connectors and registered image drivers alike; proxies, CA bundles and
credential-shaped names inside a family are not.
"""

from __future__ import annotations

import pytest

from orchestrator.security.credential_files import (
    CredentialFileValidationError,
    normalize_credential_files,
)
from shared.connectors.env_names import connector_env_problem
from shared.connectors.registration import (
    custom_driver_problems,
    declared_env_names,
    image_binding_problems,
    spec_from_json,
)
from shared.credential_connectors import normalize_credential_env

RESERVED = [
    "PATH",
    "HOME",
    "SHELL",
    "IFS",
    "BASH_ENV",
    "PROMPT_COMMAND",
    "TMUX",
    "SRW_TOKEN",
    "srw_anything",
    "path",
    "LD_PRELOAD",
    "LD_LIBRARY_PATH",
    "DYLD_INSERT_LIBRARIES",
    "PYTHONPATH",
    "PYTHONSTARTUP",
    "PYTHON_TOKEN",
]

#: Code hooks, by the family the module docstring explains.
CODE_HOOKS = {
    "git": [
        "GIT_SSH_COMMAND",
        "GIT_SSH",
        "GIT_ASKPASS",
        "GIT_EDITOR",
        "GIT_PAGER",
        "GIT_EXTERNAL_DIFF",
        "GIT_PROXY_COMMAND",
        "GIT_EXEC_PATH",
        "GIT_TEMPLATE_DIR",
        "GIT_DIR",
        "GIT_COMMON_DIR",
        "GIT_CONFIG",
        "GIT_CONFIG_GLOBAL",
        "GIT_CONFIG_SYSTEM",
        "GIT_CONFIG_NOSYSTEM",
        "GIT_CONFIG_PARAMETERS",
        "GIT_CONFIG_COUNT",
        "GIT_CONFIG_KEY_0",
        "GIT_CONFIG_VALUE_0",
        "GIT_CONFIG_TOKEN",
        "GIT_ALLOW_PROTOCOL",
        "GIT_PROTOCOL_FROM_USER",
        "GIT_TERMINAL_PROMPT",
        "GIT_WORK_TREE",
        "GIT_TRACE",
        "git_ssh_command",
    ],
    "ssh": [
        "SSH_ASKPASS",
        "SSH_ASKPASS_REQUIRE",
        "SSH_SK_PROVIDER",
        "SSH_AUTH_SOCK",
        "SSH_AGENT_PID",
        "SSH_ORIGINAL_COMMAND",
        "SUDO_ASKPASS",
    ],
    "editors and pagers": [
        "EDITOR",
        "VISUAL",
        "PAGER",
        "MANPAGER",
        "LESSOPEN",
        "BROWSER",
        "GH_PAGER",
        "KUBE_EDITOR",
    ],
    "runtime options and flags": [
        "NODE_OPTIONS",
        "JAVA_TOOL_OPTIONS",
        "_JAVA_OPTIONS",
        "JDK_JAVA_OPTIONS",
        "RUBYOPT",
        "PERL5OPT",
        "MAVEN_OPTS",
        "CFLAGS",
        "GOFLAGS",
        "ERL_FLAGS",
    ],
    "code and search paths": [
        "NODE_PATH",
        "PERL5LIB",
        "RUBYLIB",
        "CLASSPATH",
        "DOTNET_STARTUP_HOOKS",
        "GCONV_PATH",
        "OPENSSL_CONF",
    ],
    "commands": ["CC", "RUSTC_WRAPPER", "FZF_DEFAULT_COMMAND", "RSYNC_RSH"],
    "rc files": ["PSQLRC", "INPUTRC", "WGETRC", "CONDARC", "NPMRC", "HGRCPATH"],
    "config files and homes": [
        "XDG_CONFIG_HOME",
        "DOCKER_CONFIG",
        "CURL_HOME",
        "CARGO_HOME",
        "JAVA_HOME",
        "GNUPGHOME",
        "TF_CLI_CONFIG_FILE",
        "RCLONE_CONFIG",
        "GH_CONFIG_DIR",
    ],
    "package sources and toolchains": [
        "PIP_INDEX_URL",
        "npm_config_registry",
        "UV_INDEX_URL",
        "GOPROXY",
        "GOTOOLCHAIN",
    ],
    "shells": ["PS1", "PS4", "ZDOTDIR", "HISTFILE", "ENV"],
}

PROXIES_AND_CA_BUNDLES = [
    "HTTPS_PROXY",
    "https_proxy",
    "HTTP_PROXY",
    "ALL_PROXY",
    "FTP_PROXY",
    "NO_PROXY",
    "no_proxy",
    "npm_config_https_proxy",
    "SSL_CERT_FILE",
    "SSL_CERT_DIR",
    "REQUESTS_CA_BUNDLE",
    "CURL_CA_BUNDLE",
    "AWS_CA_BUNDLE",
    "NODE_EXTRA_CA_CERTS",
    "GIT_SSL_CAINFO",
    "GIT_SSL_CAPATH",
    "PGSSLROOTCERT",
    "PIP_CERT",
    "DOCKER_CERT_PATH",
    "NODE_TLS_REJECT_UNAUTHORIZED",
    "GIT_SSL_NO_VERIFY",
    "PGSSLMODE",
]

CREDENTIAL_SHAPED = [
    "NODE_AUTH_TOKEN",
    "CARGO_REGISTRY_TOKEN",
    "UV_PUBLISH_TOKEN",
    "GEM_HOST_API_KEY",
    "PIP_PASSWORD",
    "YARN_NPM_AUTH_TOKEN",
    "GIT_TOKEN",
    # Not in any family at all.
    "GITHUB_TOKEN",
    "AWS_ACCESS_KEY_ID",
    "DATABASE_URL",
    "PGPASSWORD",
]


class TestTheRule:
    @pytest.mark.parametrize("name", RESERVED)
    def test_srw_s_reserved_names_are_refused(self, name):
        assert "reserved by the workspace" in connector_env_problem(name)

    def test_kubeconfig_is_the_kubeconfig_merge_s(self):
        assert connector_env_problem("KUBECONFIG") == (
            "KUBECONFIG is reserved: it names the merged kubeconfig"
        )

    @pytest.mark.parametrize(
        "name",
        [name for names in CODE_HOOKS.values() for name in names],
    )
    def test_a_code_hook_is_refused(self, name):
        problem = connector_env_problem(name)
        assert problem is not None
        assert "reserved" in problem or "connector may set" in problem

    @pytest.mark.parametrize("name", PROXIES_AND_CA_BUNDLES)
    def test_proxies_ca_bundles_and_tls_checks_are_allowed(self, name):
        assert connector_env_problem(name) is None

    @pytest.mark.parametrize("name", CREDENTIAL_SHAPED)
    def test_credential_shaped_names_are_allowed(self, name):
        assert connector_env_problem(name) is None

    @pytest.mark.parametrize(
        "name", ["GIT_AUTHOR_NAME", "GIT_AUTHOR_EMAIL", "GIT_COMMITTER_DATE"]
    )
    def test_git_s_commit_identity_is_allowed(self, name):
        assert connector_env_problem(name) is None

    @pytest.mark.parametrize("name", ["1BAD", "bad-name", "", "A" * 129, None, 3])
    def test_a_name_that_is_no_variable_is_refused(self, name):
        assert connector_env_problem(name) is not None


#: The minimal registered bind-time spec a driver check needs.
SPEC = {
    "name": "acme.env/v1",
    "title": "Acme",
    "protocol_version": "1.0",
    "plane": "bind_time",
    "delivery_forms": ["env_file", "credential_file"],
    "env_names": ["ACME_TOKEN"],
    "config_schema": {"type": "object"},
    "credential_slots": [],
    "access_levels": [],
    "supported_backends": ["sandbox", "vm"],
    "operations": [],
}


def _env_connector(name: str) -> str | None:
    try:
        normalize_credential_env({"ACME_TOKEN": "t", name: "v"})
    except ValueError as exc:
        return str(exc)
    return None


def _credential_file(name: str) -> str | None:
    files = [{"contents": "x", "target_path": "~/.srw-files/x", "env_var": name}]
    try:
        normalize_credential_files("generic_file", "x", {"files": files})
    except CredentialFileValidationError as exc:
        return str(exc)
    return None


def _driver_declaration(name: str) -> str | None:
    value = {**SPEC, "env_names": ["ACME_TOKEN", name]}
    problems = custom_driver_problems(
        spec_from_json(value), privileged=False, env_names=declared_env_names(value)
    )
    return "; ".join(problems) or None


def _driver_binding(name: str) -> str | None:
    value = {**SPEC, "env_names": ["ACME_TOKEN", name]}
    entry = {
        "recipient": "workspace",
        "form": "env_file",
        "value": {"name": name, "value": "v"},
        "collision": "error",
    }
    descriptor = {
        "driver": SPEC["name"],
        "name": "acme",
        "access": "ReadWrite",
        "entries": [entry],
    }
    problems = image_binding_problems(
        descriptor, spec_from_json(value), env_names=declared_env_names(value)
    )
    return "; ".join(problems) or None


CONSUMERS = {
    "environment connector": _env_connector,
    "credential file env_var": _credential_file,
    "driver env_names": _driver_declaration,
    "driver binding": _driver_binding,
}


class TestEveryConnectorFollowsIt:
    """SRW's own connectors and registered drivers apply the same rule
    (decision 26: the difference between them is trust, not rules)."""

    @pytest.mark.parametrize("consumer", CONSUMERS)
    @pytest.mark.parametrize(
        "name", ["NODE_OPTIONS", "GIT_SSH_COMMAND", "EDITOR", "PATH", "KUBECONFIG"]
    )
    def test_each_refuses_what_the_rule_refuses(self, consumer, name):
        problem = CONSUMERS[consumer](name)
        assert problem is not None
        assert connector_env_problem(name) in problem

    @pytest.mark.parametrize("consumer", CONSUMERS)
    @pytest.mark.parametrize(
        "name", ["HTTPS_PROXY", "SSL_CERT_FILE", "NODE_AUTH_TOKEN", "AWS_CONFIG_FILE"]
    )
    def test_each_allows_what_the_rule_allows(self, consumer, name):
        assert CONSUMERS[consumer](name) is None
