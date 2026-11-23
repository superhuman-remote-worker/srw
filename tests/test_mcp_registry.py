"""Dynamic MCP tool registration and wildcard expansion."""

from types import SimpleNamespace

import pytest

from agent.tools.context import ToolContext
from agent.tools.registry import (
    TOOL_REGISTRY,
    expand_tool_wildcards,
    filter_tools_by_phase,
    load_tools,
    register_mcp_tools,
)


class FakeTool(SimpleNamespace):
    pass


class FakeManager:
    def __init__(self, names):
        self._tools = [
            FakeTool(
                name=name,
                description=f"desc {name}",
                args_schema=None,
                coroutine=None,
                metadata={"mcp_server": "fake"},
            )
            for name in names
        ]
        self.statuses = {"fake": "connected"}
        self.image_checks = []

    def get_langchain_tools(self, sees_images=None, save_file=None):
        self.image_checks.append(sees_images)
        return self._tools


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    for name in [
        name
        for name, metadata in TOOL_REGISTRY.items()
        if metadata.get("category") == "mcp"
    ]:
        del TOOL_REGISTRY[name]


def test_register_and_purge_idempotent():
    register_mcp_tools(FakeManager(["mcp__a__one", "mcp__a__two"]))
    assert TOOL_REGISTRY["mcp__a__one"]["category"] == "mcp"

    register_mcp_tools(FakeManager(["mcp__a__three"]))

    assert "mcp__a__one" not in TOOL_REGISTRY
    assert "mcp__a__three" in TOOL_REGISTRY


def test_register_none_clears_stale_tools():
    register_mcp_tools(FakeManager(["mcp__a__one"]))
    register_mcp_tools(None)
    assert "mcp__a__one" not in TOOL_REGISTRY


def test_expand_wildcard():
    register_mcp_tools(FakeManager(["mcp__a__one"]))
    assert expand_tool_wildcards(["read_file", "*"]) == [
        "read_file",
        "mcp__a__one",
    ]
    assert expand_tool_wildcards(["read_file"]) == ["read_file"]


def test_mcp_tools_pass_phase_filter():
    register_mcp_tools(FakeManager(["mcp__a__one"]))
    assert filter_tools_by_phase(["mcp__a__one"], "strategic") == ["mcp__a__one"]
    assert filter_tools_by_phase(["mcp__a__one"], "tactical") == ["mcp__a__one"]


def test_load_tools_pulls_from_manager():
    manager = FakeManager(["mcp__a__one", "mcp__a__two"])
    register_mcp_tools(manager)
    context = ToolContext(datasources={"mcp": manager})

    tools = load_tools(["mcp__a__one"], context)

    assert [tool.name for tool in tools] == ["mcp__a__one"]
    # The tools are bound to this context's own "does the model see
    # images" check, so a subagent's model never decides for its parent.
    assert manager.image_checks[-1] == context.get_phase_multimodal


def test_load_tools_without_manager_warns_not_raises():
    register_mcp_tools(FakeManager(["mcp__a__one"]))
    tools = load_tools(["mcp__a__one"], ToolContext())
    assert tools == []
