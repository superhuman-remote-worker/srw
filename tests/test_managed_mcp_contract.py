"""The managed MCP block of a service driver's spec (connector drivers D5a, D5b).

How a driver spec declares tool classes, which access level sees which
class, how the server container is configured from the connector's config
(and why no config value can inject a shell or an argument), how a stdio
server's credential reaches its binding's process, and what the spec rules
require of a managed MCP driver. The front applies the same tool-class rule
in Go; both read drivers/mcp-front/testdata.
"""

from __future__ import annotations

import dataclasses
import json
import re
from pathlib import Path

import pytest

from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    DEVELOPMENT_SPECS,
    GITEA_MCP_READ_TOOLS,
    GITEA_MCP_SPEC,
    LEGACY_TYPE_IDS,
    MANAGED_MCP_SPECS,
    MCP_STDIO_TEST_SPEC,
    MCP_TEST_SPEC,
    MEMORY_MCP_READ_TOOLS,
    spec_for_type,
)
from shared.connectors.contract import (
    ServiceSpec,
    effective_access,
    managed_mcp_driver,
    validate_spec,
)
from shared.connectors.mcp import (
    BRIDGE_PATH,
    CODE_ENV,
    FRONT_PATH,
    RESERVED_HEADERS,
    ManagedMcp,
    TemplateError,
    managed_mcp,
    mcp_problems,
    pattern_matches,
    render,
)
from shared.datasource_policy import datasource_tool_categories

ROOT = Path(__file__).resolve().parents[1]
LEVELS = ("ReadOnly", "ReadWrite")


def _block(**over):
    block = {
        "transport": "http",
        "port": 8091,
        "path": "/mcp",
        "tools": {"read": ["get_*", "list_repos"]},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": {"header": "Authorization", "scheme": "Bearer"},
        "env": {"HOST": "${config.url}"},
        "args": ["--port", "8091"],
    }
    block.update(over)
    return block


def test_tool_classes_follow_the_vectors_the_front_shares():
    vectors = json.loads(
        (ROOT / "drivers/mcp-front/testdata/pattern_vectors.json").read_text()
    )
    assert vectors["cases"]
    for case in vectors["cases"]:
        assert pattern_matches(case["pattern"], case["name"]) is case["match"], case


def test_an_unclassed_tool_is_a_write_tool_and_an_unknown_level_sees_nothing():
    mcp = ManagedMcp.parse(_block(), access_levels=LEVELS, front_port=8080)
    assert mcp.tool_class("get_me") == "read"
    assert mcp.tool_class("list_repos") == "read"
    assert mcp.tool_class("delete_repo") == "write"
    assert mcp.tool_class("brand_new_tool") == "write"
    assert mcp.allowed("get_me", "ReadOnly") and not mcp.allowed(
        "delete_repo", "ReadOnly"
    )
    assert mcp.allowed("delete_repo", "ReadWrite")
    for level in ("Admin", "", None):
        assert not mcp.allowed("get_me", level)


def test_the_server_is_configured_from_the_connector_config_and_never_a_secret():
    mcp = ManagedMcp.parse(_block(), access_levels=LEVELS, front_port=8080)
    assert mcp.server_env({"url": "https://g.example"}) == {"HOST": "https://g.example"}
    assert mcp.server_args({}) == ["--port", "8091"]
    assert render("a-${config.n}-${config.flag}", {"n": 3, "flag": True}) == (
        "a-3-true"
    )
    with pytest.raises(TemplateError):
        render("${config.missing}", {})
    with pytest.raises(TemplateError):
        render("${config.nested}", {"nested": {"x": 1}})
    assert mcp.upstream == "http://127.0.0.1:8091/mcp"
    assert FRONT_PATH == "/mcp"


def test_the_front_reads_its_own_block():
    mcp = ManagedMcp.parse(_block(), access_levels=LEVELS, front_port=8080)
    assert mcp.front_config() == {
        "transport": "http",
        "upstream": "http://127.0.0.1:8091/mcp",
        "protocol": "legacy",
        "tools": {"read": ["get_*", "list_repos"]},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": {"header": "Authorization", "scheme": "Bearer"},
        "max_in_flight_per_binding": 4,
        "tool_pinning": "warn",
    }
    unauthenticated = ManagedMcp.parse(
        _block(credential=None), access_levels=LEVELS, front_port=8080
    )
    assert unauthenticated.front_config()["credential"] is None


@pytest.mark.parametrize(
    ("over", "fragment"),
    [
        ({"transport": "sse"}, "transport"),
        ({"port": 8080}, "the front's port"),
        ({"port": True}, "port"),
        ({"path": "mcp"}, "path"),
        ({"protocol": "v3"}, "protocol"),
        # SRW's client and the front's probe speak initialize only.
        ({"protocol": "modern"}, "protocol"),
        ({"credential": {"header": "Mcp-Session-Id"}}, "front forwards"),
        ({"credential": {"header": "host"}}, "front forwards"),
        ({"credential": {"header": "Transfer-Encoding"}}, "front forwards"),
        # Access is the front's, per lease: never the server's configuration.
        ({"env": {"MODE": "${config.access}"}}, "per lease"),
        ({"args": ["--mode=${config.access}"]}, "per lease"),
        ({"tools": {"admin": []}}, "tools"),
        ({"tools": {"read": ["get_[a]"]}}, "tools.read"),
        ({"access": {"ReadOnly": ["read"]}}, "does not name the access levels"),
        ({"access": {**_block()["access"], "Admin": ["write"]}}, "unknown access"),
        ({"access": {"ReadOnly": ["admin"], "ReadWrite": []}}, "tool classes"),
        ({"credential": {"header": "Bad Header"}}, "header"),
        # The headers the front sets for the stdio bridge are its own.
        ({"credential": {"header": "Srw-Bridge-Credential"}}, "front forwards"),
        ({"credential": {"header": "srw-bridge-binding"}}, "front forwards"),
        # An HTTP server's credential is a header, never an environment
        # variable, and the stdio keys are the bridge's.
        ({"credential": {"env": "TOKEN"}}, "header"),
        ({"stdio_mode": "process-per-binding"}, "stdio servers only"),
        ({"max_bindings_per_pod": 4}, "stdio servers only"),
        ({"idle_seconds": 600}, "stdio servers only"),
        # A template can inject neither a shell nor an argument.
        ({"command": ["${config.program}"]}, "the program is the spec's"),
        ({"args": ["--root", "/data/${config.root}"]}, "whole argument"),
        ({"args": ["--root ${config.root}"]}, "whole argument"),
        ({"args": ["-c", "${config.code}"]}, "never code"),
        ({"args": ["--eval=${config.code}"]}, "never code"),
        ({"args": ["-e=${config.code}"]}, "never code"),
        ({"command": ["node", "-e"], "args": ["${config.code}"]}, "never code"),
        ({"command": ["/bin/sh"], "args": ["--x=${config.y}"]}, "is a shell"),
        ({"command": ["env", "node"], "args": ["${config.y}"]}, "is a shell"),
        ({"env": {"NODE_OPTIONS": "${config.flags}"}}, "loads code"),
        ({"env": {"PATH": "/bin:${config.dir}"}}, "loads code"),
        ({"env": {"SRW_TOKEN": "x"}}, "reserved"),
        ({"env": {"LD_PRELOAD": "/x.so"}}, "reserved"),
        ({"env": {"OK": "${secret.token}"}}, "placeholder"),
        ({"args": ["${env.HOME}"]}, "placeholder"),
        ({"max_in_flight_per_binding": 0}, "max_in_flight"),
        ({"tool_pinning": "ignore"}, "tool_pinning"),
        ({"surprise": 1}, "unknown key"),
    ],
)
def test_a_malformed_block_is_refused(over, fragment):
    problems = mcp_problems(_block(**over), access_levels=LEVELS, front_port=8080)
    assert any(fragment in problem for problem in problems), problems


def test_the_shipped_managed_servers_are_valid_and_resolve_for_the_agent():
    for spec in (GITEA_MCP_SPEC, MCP_TEST_SPEC, MCP_STDIO_TEST_SPEC):
        assert validate_spec(spec) == [], spec.name
        assert managed_mcp_driver(spec)
        assert managed_mcp(spec) is not None
        # The agent reads what it is sent: the stored type resolves, but no
        # catalogue lists a server the installation has not turned on.
        assert spec_for_type(spec.legacy_type) is spec
        assert spec.legacy_type not in LEGACY_TYPE_IDS
        assert spec.service.callers == ("harness",)
        assert spec.credential_delivery == "lease"
        assert spec.holds_upstream_credentials
    assert GITEA_MCP_SPEC in MANAGED_MCP_SPECS and MCP_TEST_SPEC in DEVELOPMENT_SPECS
    assert MCP_STDIO_TEST_SPEC in DEVELOPMENT_SPECS
    assert not any(managed_mcp_driver(spec) for spec in DATASOURCE_SPECS)


def test_gitea_classes_only_its_read_tools_as_read():
    mcp = managed_mcp(GITEA_MCP_SPEC)
    for name in GITEA_MCP_READ_TOOLS:
        assert mcp.allowed(name, "ReadOnly"), name
    # gitea-mcp 1.8's write tools: none is visible to a read-only binding.
    # attachment_read and actions_run_read read Gitea, but their download
    # methods write files at a caller-chosen output_path in the shared pod.
    for name in (
        "attachment_read",
        "actions_run_read",
        "create_repo",
        "fork_repo",
        "create_or_update_file",
        "delete_file",
        "create_branch",
        "delete_branch",
        "issue_write",
        "pull_request_write",
        "pull_request_review_write",
        "label_write",
        "wiki_write",
        "create_release",
        "delete_tag",
        "actions_run_write",
    ):
        assert not mcp.allowed(name, "ReadOnly"), name
        assert mcp.allowed(name, "ReadWrite"), name
    # Exact names: a tool Gitea adds stays hidden until it is classed.
    assert all("*" not in name for name in GITEA_MCP_READ_TOOLS)
    env = mcp.server_env({"url": "https://gitea.example.com"})
    assert env == {"MCP_MODE": "http", "GITEA_HOST": "https://gitea.example.com"}
    assert mcp.server_args({}) == ["-b", "127.0.0.1", "-p", "8091"]


def test_the_test_server_is_told_its_connectors_message():
    """The k3d gate proves injection by the message whoami reports: it
    reaches the server through the connector's configuration only."""
    mcp = managed_mcp(MCP_TEST_SPEC)
    assert mcp.server_env({"message": "d5a-0123456789"}) == {
        "MCP_TEST_MESSAGE": "d5a-0123456789"
    }
    assert MCP_TEST_SPEC.config_schema["required"] == ["message"]
    source = (ROOT / "drivers/mcp-test/main.go").read_text()
    assert 'os.Getenv("MCP_TEST_MESSAGE")' in source


def test_a_managed_server_is_called_by_the_agent_alone_and_delivers_by_lease():
    workspace = dataclasses.replace(
        MCP_TEST_SPEC,
        service=dataclasses.replace(MCP_TEST_SPEC.service, callers=("workspace",)),
    )
    assert any("agent process only" in p for p in validate_spec(workspace))
    inline = dataclasses.replace(MCP_TEST_SPEC, credential_delivery="inline")
    assert any("delivers by lease" in p for p in validate_spec(inline))
    file_too = dataclasses.replace(
        MCP_TEST_SPEC, delivery_forms=("mcp_client", "lease_token")
    )
    assert any("mcp_client form only" in p for p in validate_spec(file_too))
    # A lease driver still needs somewhere to deliver its token.
    no_form = dataclasses.replace(
        MCP_TEST_SPEC,
        delivery_forms=("env_file",),
        service=ServiceSpec(callers=("harness",)),
    )
    assert any("lease_token form" in p for p in validate_spec(no_form))


def test_the_access_level_comes_from_the_config_or_a_read_only_link():
    entry = {"type": "gitea_mcp", "config": {"access": "ReadOnly"}}
    assert effective_access(entry, GITEA_MCP_SPEC) == "ReadOnly"
    assert effective_access({"config": {}}, GITEA_MCP_SPEC) == "ReadWrite"
    assert (
        effective_access(
            {"config": {"access": "ReadWrite"}, "project_read_only": True},
            GITEA_MCP_SPEC,
        )
        == "ReadOnly"
    )


def test_an_attached_managed_server_binds_the_mcp_category():
    """Managed servers are outside the tool map: their tools are discovered
    at runtime like any MCP server's, so their category's sentinel is bound;
    the front lists only the tools the binding may call."""
    categories = datasource_tool_categories([{"type": "gitea_mcp"}])
    assert categories["mcp"] == ["*"]
    assert datasource_tool_categories([{"type": "postgresql"}])["mcp"] == []
    both = datasource_tool_categories([{"type": "mcp"}, {"type": "mcp_test"}])
    assert both["mcp"] == ["*"]
    stdio = datasource_tool_categories([{"type": "mcp_stdio_test"}])
    assert stdio["mcp"] == ["*"]


# =============================================================================
# stdio servers behind the bridge (D5b)
# =============================================================================


def _stdio(**over):
    block = {
        "transport": "stdio",
        "port": 8091,
        "tools": {"read": ["read_graph", "search_*"]},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": {"env": "SERVICE_TOKEN"},
        "env": {"DATA": "/tmp/${config.store}.json"},
        "args": ["--root=${config.root}", "${config.mode}"],
    }
    block.update(over)
    return block


def test_a_stdio_server_gets_its_credential_in_its_process_environment():
    mcp = ManagedMcp.parse(_stdio(), access_levels=LEVELS, front_port=8080)
    assert mcp.stdio and mcp.credential_env == "SERVICE_TOKEN"
    assert mcp.credential_header is None
    assert (mcp.stdio_mode, mcp.max_bindings_per_pod, mcp.idle_seconds) == (
        "process-per-binding",
        8,
        600,
    )
    assert mcp.front_config() == {
        "transport": "stdio",
        "upstream": "http://127.0.0.1:8091/mcp",
        "protocol": "legacy",
        "tools": {"read": ["read_graph", "search_*"]},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": {"env": "SERVICE_TOKEN"},
        "max_in_flight_per_binding": 4,
        "tool_pinning": "warn",
    }
    # A stdio server that takes no credential (the default: none).
    bare = _stdio()
    del bare["credential"]
    unauthenticated = ManagedMcp.parse(bare, access_levels=LEVELS, front_port=8080)
    assert unauthenticated.credential_env is None
    assert unauthenticated.front_config()["credential"] is None
    assert "--credential-env" not in unauthenticated.bridge_command(["srv"])


def test_the_bridge_is_the_command_and_the_servers_program_follows_it():
    mcp = ManagedMcp.parse(
        _stdio(max_bindings_per_pod=3, idle_seconds=120, path="/rpc"),
        access_levels=LEVELS,
        front_port=8080,
    )
    assert mcp.bridge_command(["node", "dist/index.js"]) == [
        BRIDGE_PATH,
        "serve",
        "--listen",
        "127.0.0.1:8091",
        "--path",
        "/rpc",
        "--max-processes",
        "3",
        "--idle",
        "120s",
        "--credential-env",
        "SERVICE_TOKEN",
        "--",
        "node",
        "dist/index.js",
    ]
    assert BRIDGE_PATH == "/srw/bin/srw-mcp-bridge"
    assert mcp.upstream == "http://127.0.0.1:8091/rpc"
    config = {"store": "graph", "root": "/data", "mode": "strict"}
    assert mcp.server_env(config) == {"DATA": "/tmp/graph.json"}
    assert mcp.server_args(config) == ["--root=/data", "strict"]


@pytest.mark.parametrize(
    ("config", "fragment"),
    [
        # A whole templated argument that would read as an option.
        ({"store": "g", "root": "/d", "mode": "--allow-write"}, "as an option"),
        ({"store": "g", "root": "/d", "mode": "-w"}, "as an option"),
        # No value holds a NUL or a line break (an argument, a variable).
        ({"store": "g", "root": "/d\nx", "mode": "m"}, "line break"),
        ({"store": "g\x00", "root": "/d", "mode": "m"}, "NUL"),
        ({"store": "g", "root": "/d", "mode": "a\rb"}, "line break"),
    ],
)
def test_a_config_value_cannot_inject_an_argument(config, fragment):
    mcp = ManagedMcp.parse(_stdio(), access_levels=LEVELS, front_port=8080)
    with pytest.raises(TemplateError, match=fragment):
        mcp.server_args(config)
        mcp.server_env(config)
    # After '=' a leading dash is the option's value, never an option.
    assert mcp.server_args({"root": "-x", "mode": "m", "store": "g"})[0] == (
        "--root=-x"
    )


def test_a_shell_program_takes_no_templated_argument():
    mcp = ManagedMcp.parse(_stdio(), access_levels=LEVELS, front_port=8080)
    # The image's own program is known only at launch.
    for program in (["/bin/sh", "-c"], ["bash"], ["/usr/bin/env", "node"]):
        assert "is a shell" in mcp.program_problem(program), program
    assert mcp.program_problem(["node", "dist/index.js"]) is None
    untemplated = ManagedMcp.parse(
        _stdio(args=["--fixed"]), access_levels=LEVELS, front_port=8080
    )
    assert untemplated.program_problem(["/bin/sh"]) is None


@pytest.mark.parametrize(
    ("over", "fragment"),
    [
        ({"credential": {"header": "Authorization"}}, '{"env": "NAME"}'),
        ({"credential": {"env": "T", "scheme": "Bearer"}}, '{"env": "NAME"}'),
        ({"credential": {"env": "1BAD"}}, "environment name"),
        ({"credential": {"env": "SRW_TOKEN"}}, "reserved or loads code"),
        ({"credential": {"env": "LD_PRELOAD"}}, "reserved or loads code"),
        ({"credential": {"env": "NODE_OPTIONS"}}, "reserved or loads code"),
        ({"credential": {"env": "pythonpath"}}, "reserved or loads code"),
        ({"credential": {"env": "PATH"}}, "reserved or loads code"),
        ({"credential": {"env": "DATA"}}, "also set in mcp env"),
        ({"stdio_mode": "shared-process"}, "not built"),
        ({"stdio_mode": "pooled"}, "stdio_mode"),
        ({"max_bindings_per_pod": 0}, "max_bindings_per_pod"),
        ({"max_bindings_per_pod": 65}, "max_bindings_per_pod"),
        ({"max_bindings_per_pod": True}, "max_bindings_per_pod"),
        ({"idle_seconds": 59}, "idle_seconds"),
        ({"idle_seconds": 86401}, "idle_seconds"),
        ({"path": "/srw/status"}, "bridge's own"),
        ({"port": 8080}, "the front's port"),
        ({"args": ["--x", "${config.access}"]}, "per lease"),
    ],
)
def test_a_malformed_stdio_block_is_refused(over, fragment):
    problems = mcp_problems(_stdio(**over), access_levels=LEVELS, front_port=8080)
    assert any(fragment in problem for problem in problems), problems


def test_the_stdio_test_server_is_the_stock_memory_image_behind_the_bridge():
    mcp = managed_mcp(MCP_STDIO_TEST_SPEC)
    assert mcp.stdio and mcp.credential_env == "MCP_STDIO_TEST_TOKEN"
    assert mcp.max_bindings_per_pod == 4
    # The memory server's read tools, exactly; every other tool writes.
    for name in MEMORY_MCP_READ_TOOLS:
        assert mcp.allowed(name, "ReadOnly"), name
    for name in (
        "create_entities",
        "create_relations",
        "add_observations",
        "delete_entities",
        "delete_observations",
        "delete_relations",
        "a_tool_added_later",
    ):
        assert not mcp.allowed(name, "ReadOnly") and mcp.allowed(name, "ReadWrite")
    # The image's root filesystem is read-only: its graph lives in /tmp.
    assert mcp.server_env({}) == {"MEMORY_FILE_PATH": "/tmp/memory.json"}
    assert mcp.server_args({}) == []
    assert not MCP_STDIO_TEST_SPEC.publishable


def test_the_bridge_and_the_contract_refuse_the_same_credential_names():
    """drivers/mcp-bridge refuses the same names on its own (defence in
    depth): its codeEnv list is this module's CODE_ENV."""
    source = (ROOT / "drivers/mcp-bridge/env.go").read_text()
    block = source[
        source.index("var codeEnv") : source.index("}\n", source.index("var codeEnv"))
    ]
    assert set(re.findall(r'"([A-Z_0-9]+)": true', block)) == set(CODE_ENV)
    front = (ROOT / "drivers/mcp-front/bridge.go").read_text()
    for header in ("Srw-Bridge-Binding", "Srw-Bridge-Credential"):
        assert f'"{header}"' in front and header.lower() in RESERVED_HEADERS
