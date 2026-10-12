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
    MCP_STDIO_PROBE_SPEC,
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
    BINDING_HOME,
    BRIDGE_PATH,
    BRIDGE_SOCKET,
    CODE_ENV,
    CODE_ENV_PREFIXES,
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
        # A stdio process's private directory: an http server has none.
        ({"env": {"DATA": "${binding.home}/x"}}, "an http server has none"),
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
    for spec in (
        GITEA_MCP_SPEC,
        MCP_TEST_SPEC,
        MCP_STDIO_TEST_SPEC,
        MCP_STDIO_PROBE_SPEC,
    ):
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
    assert MCP_STDIO_PROBE_SPEC in DEVELOPMENT_SPECS
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
    # The front reaches the bridge on its socket; the upstream's host is a
    # name only.
    assert mcp.front_config() == {
        "transport": "stdio",
        "upstream": "http://srw-mcp-bridge/mcp",
        "socket": BRIDGE_SOCKET,
        "protocol": "legacy",
        "tools": {"read": ["read_graph", "search_*"]},
        "access": {"ReadOnly": ["read"], "ReadWrite": ["read", "write"]},
        "credential": {"env": "SERVICE_TOKEN"},
        "max_in_flight_per_binding": 4,
        "tool_pinning": "warn",
    }
    assert BRIDGE_SOCKET == "/srw/bridge/bridge.sock"
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
    # The bridge serves the front's group on its socket and runs each
    # binding's process as a user of its own, from 20000, with a private
    # directory under /srw/home.
    assert mcp.bridge_command(["node", "dist/index.js"]) == [
        BRIDGE_PATH,
        "serve",
        "--socket",
        "/srw/bridge/bridge.sock",
        "--socket-group",
        "65532",
        "--uid-base",
        "20000",
        "--home-root",
        "/srw/home",
        "--path",
        "/rpc",
        "--max-processes",
        "3",
        "--idle",
        "120s",
        "--process-limit",
        "256",
        "--credential-env",
        "SERVICE_TOKEN",
        "--",
        "node",
        "dist/index.js",
    ]
    assert BRIDGE_PATH == "/srw/bin/srw-mcp-bridge"
    assert mcp.upstream == "http://srw-mcp-bridge/rpc"
    # Outside a pod (a test): no group, no users.
    assert mcp.bridge_command(
        ["srv"], socket="/t/b.sock", socket_group=None, uid_base=0, home_root="/t/h"
    )[2:8] == ["--socket", "/t/b.sock", "--uid-base", "0", "--home-root", "/t/h"]
    config = {"store": "graph", "root": "/data", "mode": "strict"}
    assert mcp.server_env(config) == {"DATA": "/tmp/graph.json"}
    assert mcp.server_args(config) == ["--root=/data", "strict"]


def test_each_bindings_processes_are_capped_and_its_address_space_is_opt_in():
    """process_limit is RLIMIT_NPROC of each binding's user (default 256);
    address_space_mb is RLIMIT_AS, opt-in (it breaks Node)."""
    default = ManagedMcp.parse(_stdio(), access_levels=LEVELS)
    assert (default.process_limit, default.address_space_mb) == (256, None)
    command = default.bridge_command(["srv"])
    assert command[command.index("--process-limit") + 1] == "256"
    assert "--address-space-mb" not in command
    capped = ManagedMcp.parse(
        _stdio(process_limit=64, address_space_mb=512), access_levels=LEVELS
    )
    command = capped.bridge_command(["srv"])
    assert command[command.index("--process-limit") + 1] == "64"
    assert command[command.index("--address-space-mb") + 1] == "512"
    assert command[-2:] == ["--", "srv"]
    # Each bound the bridge refuses is refused here first.
    for over in (
        {"process_limit": 8},
        {"process_limit": 5000},
        {"process_limit": True},
        {"address_space_mb": 10},
        {"address_space_mb": "512"},
    ):
        assert mcp_problems(_stdio(**over), access_levels=LEVELS), over
    # stdio servers only.
    for key in ("process_limit", "address_space_mb"):
        problems = mcp_problems(_block(**{key: 64}), access_levels=LEVELS)
        assert any("stdio servers only" in p for p in problems), key
    # The bridge's own bounds are the same.
    source = (ROOT / "drivers/mcp-bridge/main.go").read_text()
    assert "limits.processes < 16 || limits.processes > 4096" in source
    assert "limits.addressSpace < 64<<20 || limits.addressSpace > 1<<40" in source


def test_a_stdio_server_may_name_its_private_directory():
    """${binding.home} is the process's private directory, which the bridge
    fills in for each process; it passes the orchestrator untouched."""
    block = _stdio(
        env={"MEMORY_FILE_PATH": BINDING_HOME + "/memory.json"},
        args=["--data=" + BINDING_HOME + "/data"],
    )
    assert mcp_problems(block, access_levels=LEVELS) == []
    mcp = ManagedMcp.parse(block, access_levels=LEVELS)
    assert mcp.server_env({}) == {"MEMORY_FILE_PATH": "${binding.home}/memory.json"}
    assert mcp.server_args({}) == ["--data=${binding.home}/data"]
    assert mcp.program_problem(["node", "index.js"]) is None


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


def _launch_problem(args, program, *, command=None, config=None):
    """Why a stdio block with these args is refused, at registration, at
    launch over the whole argv, or when the config is rendered; None when
    it runs."""
    block = _stdio(args=list(args), env={})
    if command is not None:
        block["command"] = list(command)
    problems = mcp_problems(block, access_levels=LEVELS)
    if problems:
        return problems[0]
    mcp = ManagedMcp.parse(block, access_levels=LEVELS)
    problem = mcp.program_problem(
        list(command) if command is not None else list(program),
        from_image=command is None,
    )
    if problem:
        return problem
    try:
        mcp.server_args(config or {"x": "value"})
    except TemplateError as exc:
        return str(exc)
    return None


# The D5b review's template probes (scratchpad py/templates.py): each one is
# refused at registration, at launch or at render.
@pytest.mark.parametrize(
    ("args", "program", "command", "config"),
    [
        (["${config.x}"], ("node", "idx.js"), None, {"x": "-e"}),
        (["--experimental-loader=${config.x}"], ("node", "idx.js"), None, None),
        (["--env-file=${config.x}"], ("node", "idx.js"), None, None),
        (["--inspect=${config.x}"], ("node", "idx.js"), None, None),
        (["-r", "${config.x}"], ("node", "idx.js"), None, None),
        (["--require", "${config.x}"], ("node", "idx.js"), None, None),
        (["--import", "${config.x}"], ("node", "idx.js"), None, None),
        (["--experimental-loader", "${config.x}"], ("node", "idx.js"), None, None),
        (["-W", "${config.x}"], ("python", "-m", "srv"), None, None),
        (["-e${config.x}"], ("node", "idx.js"), None, None),
        (["--eval=${config.x}"], ("node", "idx.js"), None, None),
        (["-c=${config.x}"], ("node", "idx.js"), None, None),
        (["${config.x}"], ("uvx",), None, None),
        (["-y", "${config.x}"], ("npx",), None, None),
        (["${config.x}"], (), ("/bin/sh", "-c", "exec node idx $0"), None),
        (["${config.x}"], (), ("/usr/bin/dash", "-c", "x"), None),
        (["${config.x}"], ("/bin/busybox", "sh", "-c", "x"), None, None),
        (["${config.x}"], ("/docker-entrypoint.sh",), None, None),
        (["${config.x}"], ("/sbin/tini", "--", "/bin/sh", "-c", "x"), None, None),
        (["${config.x}"], ("python", "-c"), None, None),
        (["${config.x}"], ("node", "-e"), None, None),
        (["${config.x}"], ("perl", "-e"), None, None),
        (["${config.x}"], ("xargs",), None, None),
        (["${config.x}"], ("gosu", "nobody"), None, None),
        (["${config.x}"], ("deno", "run", "-A"), None, None),
        (["${config.x}"], ("/bin/bash5", "-c", "x"), None, None),
        (["${config.x}"], ("python3", "-c"), None, None),
        (["--from", "${config.x}", "pkg"], ("uvx",), None, None),
        # An image entrypoint SRW cannot read an argv for: name the program.
        (["--root=${config.x}"], ("/server/github-mcp-server", "stdio"), None, None),
        (["${config.x}"], ("/app/run",), None, None),
    ],
)
def test_the_review_template_probes_are_refused(args, program, command, config):
    assert _launch_problem(args, program, command=command, config=config)


#: The long forms of the short code options, and each runner family's own
#: code options (the D5b re-review's npx --call=, swept): fused with "=" and
#: as the next argument, at launch (the image's program) and with mcp command.
_LONG_AND_RUNNER_CODE_OPTIONS = [
    ("--call", ("npx", "-y", "pkg")),
    ("--call", ("npm", "exec")),
    ("--eval", ("node", "idx.js")),
    ("--print", ("node", "idx.js")),
    ("--require", ("node", "idx.js")),
    ("--import", ("node", "idx.js")),
    ("--loader", ("node", "idx.js")),
    ("--experimental-loader", ("nodejs", "idx.js")),
    ("--node-options", ("npx", "-y", "pkg")),
    ("--script-shell", ("npm", "exec", "pkg")),
    ("--userconfig", ("npx", "pkg")),
    ("--preload", ("bun", "run", "idx.ts")),
    ("--config", ("bun", "idx.ts")),
    ("--config", ("deno", "run", "idx.ts")),
    ("--import-map", ("deno", "run", "idx.ts")),
    ("--location", ("deno", "run", "idx.ts")),
    ("-E", ("perl", "srv.pl")),
    ("-M", ("perl", "srv.pl")),
    ("-I", ("perl", "srv.pl")),
    ("-I", ("ruby3.3", "srv.rb")),
    ("-d", ("php", "srv.php")),
    ("-B", ("php8.3", "srv.php")),
    ("-cp", ("java", "-jar", "srv.jar")),
    ("-jar", ("java",)),
    ("--class-path", ("java", "Main")),
    ("--index", ("uvx", "pkg")),
    ("--find-links", ("uvx", "pkg")),
    ("--with-requirements", ("uv", "tool", "run", "pkg")),
    ("--spec", ("pipx", "run", "pkg")),
    ("-f", ("uvx", "pkg")),
]


@pytest.mark.parametrize(("option", "program"), _LONG_AND_RUNNER_CODE_OPTIONS)
def test_a_long_or_runner_code_option_never_takes_a_template(option, program):
    for args in ([f"{option}=${{config.x}}"], [option, "${config.x}"]):
        # The image's program, at launch.
        assert _launch_problem(args, program), (option, program, args)
        # The spec's own program: refused at registration already.
        problems = mcp_problems(
            _stdio(args=args, env={}, command=list(program)), access_levels=LEVELS
        )
        assert any("never code" in p for p in problems), (option, program, problems)


@pytest.mark.parametrize(
    ("args", "program", "command"),
    [
        (["${config.root}"], ("npx", "-y", "@modelcontextprotocol/server-fs"), None),
        (["--root=${config.root}"], ("node", "dist/index.js"), None),
        (["--config", "${config.root}"], ("node", "dist/index.js"), None),
        (["${config.root}"], ("python", "-m", "srv"), None),
        (["${config.root}"], ("tini", "--", "node", "idx.js"), None),
        (["--a=${config.root}"], ("/usr/local/bin/python3.12", "app.py"), None),
        # The spec names the program: its author vouches for its argv.
        (["--root=${config.root}"], (), ("/server/github-mcp-server", "stdio")),
        # A runner's own option is ordinary for another program.
        (["--config=${config.root}"], (), ("/server/mcp-server",)),
        (["--index", "${config.root}"], (), ("/server/search-mcp",)),
        (["--spec=${config.root}"], ("node", "openapi.js"), None),
        (["-I", "${config.root}"], ("node", "idx.js"), None),
    ],
)
def test_ordinary_templated_arguments_still_run(args, program, command):
    assert (
        _launch_problem(args, program, command=command, config={"root": "/d"}) is None
    )


@pytest.mark.parametrize(
    "name",
    [
        "GIT_SSH_COMMAND",
        "BUN_OPTIONS",
        "DOTNET_STARTUP_HOOKS",
        "OPENSSL_CONF",
        "npm_config_registry",
        "NPM_CONFIG_NODE_OPTIONS",
        "PIP_INDEX_URL",
        "UV_INDEX_URL",
        "PYTHONUSERBASE",
        "PAGER",
        "EDITOR",
        "BROWSER",
        "GLIBC_TUNABLES",
        "PYTHONWARNINGS",
        "Node_Options",
    ],
)
def test_no_config_value_lands_in_a_variable_that_runs_code(name):
    templated = mcp_problems(_stdio(env={name: "${config.x}"}), access_levels=LEVELS)
    assert any("no config value is templated" in p for p in templated), templated
    credential = mcp_problems(
        _stdio(env={}, credential={"env": name}), access_levels=LEVELS
    )
    assert any("reserved or loads code" in p for p in credential), credential
    # A literal value is the spec author's.
    assert mcp_problems(_stdio(env={name: "fixed"}), access_levels=LEVELS) == []


def test_the_deny_lists_are_a_lint_the_spec_author_is_the_boundary():
    from shared.connectors import mcp as module

    assert "lint" in module.__doc__ and "trust" in module.__doc__
    assert CODE_ENV_PREFIXES == ("GIT_", "NPM_CONFIG_", "PIP_", "UV_")


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
        # The bridge serves a socket: a stdio block names no port.
        ({"port": 8091}, "an http server's"),
        ({"args": ["--x", "${config.access}"]}, "per lease"),
        ({"credential": {"env": "TMPDIR"}}, "reserved or loads code"),
        ({"credential": {"env": "GIT_ASKPASS"}}, "reserved or loads code"),
        ({"env": {"X": "${binding.other}"}}, "placeholder"),
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
    # The image's root filesystem is read-only: each binding's graph lives
    # in its process's private directory, and a process's heap is capped
    # below the container's limit (whose OOM kill stops every process).
    assert mcp.server_env({}) == {
        "MEMORY_FILE_PATH": "${binding.home}/memory.json",
        "NODE_OPTIONS": "--max-old-space-size=64",
    }
    assert mcp.server_args({}) == []
    assert MCP_STDIO_TEST_SPEC.service.resources["limits"]["memory"] == "640Mi"
    assert not MCP_STDIO_TEST_SPEC.publishable


def test_the_stdio_probe_server_is_the_test_image_in_stdio_mode():
    """The gate's isolation probes: SRW's test server with -stdio, whose
    probe tools are read tools (they report only whether an attempt was
    refused)."""
    mcp = managed_mcp(MCP_STDIO_PROBE_SPEC)
    assert mcp.stdio and mcp.credential_env == "MCP_TEST_TOKEN"
    assert list(mcp.command) == [
        "/srw-mcp-test",
        "-stdio",
        "-credential-env",
        "MCP_TEST_TOKEN",
    ]
    for name in ("self_status", "probe_path", "probe_socket", "probe_signal", "whoami"):
        assert mcp.allowed(name, "ReadOnly"), name
    assert not mcp.allowed("notes_write", "ReadOnly")
    source = (ROOT / "drivers/mcp-test/stdio.go").read_text()
    for name in ("self_status", "probe_path", "probe_socket", "probe_signal"):
        assert f'"{name}"' in source
    assert not MCP_STDIO_PROBE_SPEC.publishable


def test_the_bridge_and_the_contract_refuse_the_same_credential_names():
    """drivers/mcp-bridge refuses the same names on its own (defence in
    depth): its codeEnv list is this module's CODE_ENV."""
    source = (ROOT / "drivers/mcp-bridge/env.go").read_text()
    block = source[
        source.index("codeEnv = map") : source.index(
            "}\n", source.index("codeEnv = map")
        )
    ]
    assert set(re.findall(r'"([A-Z_0-9]+)": true', block)) == set(CODE_ENV)
    prefixes = re.search(r"codeEnvPrefixes = \[\]string\{([^}]*)\}", source)
    assert prefixes and tuple(re.findall(r'"([A-Z_]+)"', prefixes.group(1))) == (
        CODE_ENV_PREFIXES
    )
    front = (ROOT / "drivers/mcp-front/bridge.go").read_text()
    for header in ("Srw-Bridge-Binding", "Srw-Bridge-Credential"):
        assert f'"{header}"' in front and header.lower() in RESERVED_HEADERS
