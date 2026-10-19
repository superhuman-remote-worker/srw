"""Every tool declares its permission effect, and the declarations agree with
the other places that classify the same tools
(knowledge-base/knowledge/features/tool_permission_tiers.md, D4/D8/D11)."""

from types import SimpleNamespace

import pytest

from agent.subagents.child import WRITE_TOOLS
from agent.tools.mcp.manager import mcp_tool_effect
from mcp_server.capabilities import TOOL_CAPABILITIES
from shared.connectors.builtin import (
    BUILTIN_SPECS,
    GITEA_MCP_SPEC,
    MCP_STDIO_TEST_SPEC,
    MEMORY_MCP_READ_TOOLS,
    spec_for_type,
)
from shared.connectors.mcp import managed_mcp
from shared.orch_surface.jobs.descriptors import descriptor, get_descriptors
from shared.permission_modes import TOOL_EFFECTS, UNDECLARED_EFFECT
from shared.tool_catalog import TOOL_REGISTRY, tool_effect

_STATIC = {
    name: meta for name, meta in TOOL_REGISTRY.items() if meta.get("category") != "mcp"
}


def test_every_catalog_entry_declares_a_known_effect():
    undeclared = sorted(n for n, m in _STATIC.items() if "effect" not in m)
    unknown = sorted(
        n for n, m in _STATIC.items() if m.get("effect") not in TOOL_EFFECTS
    )

    assert undeclared == []
    assert unknown == []


@pytest.mark.parametrize(
    ("effect", "names"),
    [
        (
            "read",
            [
                "read_file",
                "git_status",
                "kb_search",
                "web_search",
                "extract_webpage",
                "browser_navigate",
                "shell_read",
                "sql_query",
                "mongo_aggregate",
                "email_read",
                "list_jobs",
                "propose_automation",
            ],
        ),
        ("bookkeeping", ["task_add", "set_canvas", "browser_close", "stop_agent"]),
        (
            "write",
            [
                "write_file",
                "edit_file",
                "cancel_command",
                "repo_commit",
                "repo_pull",
                "webdav_read",
                "browser_click",
                "pause_job",
            ],
        ),
        (
            "external",
            [
                "delete_file",
                "delete_directory",
                "email_draft",
                "email_send",
                "repo_push",
                "sql_execute",
                "mongo_insert",
                "webdav_delete",
                "delegate_agent",
                "message_agent",
                "create_job",
                "cancel_job",
            ],
        ),
        ("shell", ["run_command", "shell_execute"]),
    ],
)
def test_classification_of_the_tools_the_design_names(effect, names):
    assert {n: tool_effect(n) for n in names} == {n: effect for n in names}


def test_an_unknown_tool_counts_as_the_undeclared_effect():
    assert tool_effect("no_such_tool") == UNDECLARED_EFFECT == "external"


def test_job_descriptors_agree_with_the_mcp_servers_own_capability_table():
    for item in get_descriptors():
        cap = TOOL_CAPABILITIES[item.name]
        if cap.read_only:
            assert item.effect == "read", item.name
        elif cap.destructive:
            assert item.effect == "external", item.name
        else:
            assert item.effect in ("write", "external"), item.name


def test_a_descriptor_must_declare_a_known_effect():
    async def fake_tool(client, caller):
        """Docstring."""

    with pytest.raises(TypeError, match="effect"):
        descriptor(
            group="job_inspection",
            plane="job_observability",
            caller_defaults=frozenset(),
            effect="sideways",
        )(fake_tool)


_READ_ONLY_LEVELS = {"ReadOnly", "read_only", "read"}


def test_a_read_only_connector_level_binds_no_external_or_shell_tool():
    # A connector's read-only level is about its upstream; a tool that writes
    # only into the workspace (repo_pull, webdav_read) may still sit in it.
    for spec in BUILTIN_SPECS:
        for level in spec.access_levels:
            if level.id not in _READ_ONLY_LEVELS or level.tools == "*":
                continue
            offending = [
                t for t in level.tools if tool_effect(t) in ("external", "shell")
            ]
            assert offending == [], (spec.legacy_types, level.id)


def test_subagent_write_policy_tools_are_declared_writes():
    assert {n: tool_effect(n) for n in WRITE_TOOLS if tool_effect(n) == "read"} == {}


class TestMcpToolEffect:
    @pytest.mark.parametrize(
        ("hints", "effect"),
        [
            ({"readOnlyHint": True}, "read"),
            ({"readOnlyHint": True, "destructiveHint": True}, "read"),
            ({"destructiveHint": False, "openWorldHint": False}, "write"),
            ({"destructiveHint": False}, "external"),  # openWorld defaults to true
            ({"openWorldHint": False}, "external"),  # destructive defaults to true
            ({"readOnlyHint": False}, "external"),
            ({}, "external"),
            (None, "external"),
            ("junk", "external"),
        ],
    )
    def test_a_user_added_server_is_mapped_from_its_hints(self, hints, effect):
        assert mcp_tool_effect(hints) == effect

    def test_a_managed_server_is_classed_by_its_driver_spec_not_its_hints(self):
        block = managed_mcp(spec_for_type(GITEA_MCP_SPEC.legacy_type))

        assert mcp_tool_effect({}, block, "get_me") == "read"
        assert (
            mcp_tool_effect({"readOnlyHint": True}, block, "not_a_read") == "external"
        )

    def test_the_memory_servers_reads_are_reads(self):
        block = managed_mcp(MCP_STDIO_TEST_SPEC)

        assert {mcp_tool_effect(None, block, n) for n in MEMORY_MCP_READ_TOOLS} == {
            "read"
        }

    def test_registration_carries_the_effect_and_falls_back(self):
        from agent.tools.registry import register_mcp_tools

        tools = [
            SimpleNamespace(
                name="mcp__a__look",
                description="",
                metadata={"mcp_tool_name": "look", "effect": "read"},
            ),
            SimpleNamespace(name="mcp__a__poke", description="", metadata={}),
        ]
        manager = SimpleNamespace(get_langchain_tools=lambda: tools, statuses={})
        try:
            register_mcp_tools(manager)

            assert tool_effect("mcp__a__look") == "read"
            assert tool_effect("mcp__a__poke") == "external"
        finally:
            register_mcp_tools(None)
        assert "mcp__a__look" not in TOOL_REGISTRY
