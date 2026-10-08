"""The managed MCP block of a service driver's spec (connector drivers D5a).

How a driver spec declares tool classes, which access level sees which
class, how the server container is configured from the connector's config,
and what the spec rules require of a managed MCP driver. The front applies
the same tool-class rule in Go; both read drivers/mcp-front/testdata.
"""

from __future__ import annotations

import dataclasses
import json
from pathlib import Path

import pytest

from shared.connectors.builtin import (
    DATASOURCE_SPECS,
    DEVELOPMENT_SPECS,
    GITEA_MCP_READ_TOOLS,
    GITEA_MCP_SPEC,
    LEGACY_TYPE_IDS,
    MANAGED_MCP_SPECS,
    MCP_TEST_SPEC,
    spec_for_type,
)
from shared.connectors.contract import (
    ServiceSpec,
    effective_access,
    managed_mcp_driver,
    validate_spec,
)
from shared.connectors.mcp import (
    FRONT_PATH,
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
        ({"transport": "stdio"}, "stdio images come with D5b"),
        ({"port": 8080}, "the front's port"),
        ({"port": True}, "port"),
        ({"path": "mcp"}, "path"),
        ({"protocol": "v3"}, "protocol"),
        ({"tools": {"admin": []}}, "tools"),
        ({"tools": {"read": ["get_[a]"]}}, "tools.read"),
        ({"access": {"ReadOnly": ["read"]}}, "does not name the access levels"),
        ({"access": {**_block()["access"], "Admin": ["write"]}}, "unknown access"),
        ({"access": {"ReadOnly": ["admin"], "ReadWrite": []}}, "tool classes"),
        ({"credential": {"header": "Bad Header"}}, "header"),
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
    for spec in (GITEA_MCP_SPEC, MCP_TEST_SPEC):
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
    assert not any(managed_mcp_driver(spec) for spec in DATASOURCE_SPECS)


def test_gitea_classes_only_its_read_tools_as_read():
    mcp = managed_mcp(GITEA_MCP_SPEC)
    for name in GITEA_MCP_READ_TOOLS:
        assert mcp.allowed(name, "ReadOnly"), name
    # gitea-mcp 1.8's write tools: none is visible to a read-only binding.
    for name in (
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
