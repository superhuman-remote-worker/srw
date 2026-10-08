"""MCP routing through datasource setup and README.md facts rendering."""

from agent.core.datasource_setup import inject_workspace_facts, process_datasources
from agent.tools.mcp.manager import MCPManager


def _mcp_ds(name="GitHub", status=None, tools=None):
    ds = {
        "type": "mcp",
        "name": name,
        "connection_url": "https://example.com/mcp",
        "credentials": {"transport": "http"},
    }
    if status is not None:
        ds["_mcp_status"] = status
        ds["_mcp_tools"] = tools or []
    return ds


class FakeWorkspace:
    def __init__(self):
        self.files = {}

    def read_file(self, path):
        return self.files.get(path, "")

    def write_file(self, path, content):
        self.files[path] = content


def test_process_datasources_creates_manager_without_io():
    connections, clients = process_datasources([_mcp_ds()])
    assert isinstance(connections["mcp"], MCPManager)
    assert "mcp" not in clients


def test_process_datasources_groups_all_mcp_into_one_manager():
    connections, _ = process_datasources([_mcp_ds("A"), _mcp_ds("B")])
    assert len(connections["mcp"]._handles) == 2


def test_index_renders_connected_server():
    workspace = FakeWorkspace()
    inject_workspace_facts(
        [
            _mcp_ds(
                status="connected",
                tools=[
                    "mcp__github__create_issue",
                    "mcp__github__get_issue",
                ],
            )
        ],
        workspace,
    )

    text = workspace.files["README.md"]
    assert "<!-- srw:workspace-facts:start -->" in text
    assert "## Connectors" in text
    assert "Available Datasources" not in text
    assert "### MCP Servers" in text
    assert "**GitHub** (mcp, 2 tools)" in text
    assert "http" in text
    assert "`mcp__github__create_issue`" in text


def test_index_marks_unavailable_server():
    workspace = FakeWorkspace()
    inject_workspace_facts(
        [_mcp_ds(status="unavailable: connect timed out")],
        workspace,
    )

    text = workspace.files["README.md"]
    assert "unavailable: connect timed out" in text
    assert "tools)" not in text


def test_index_caps_long_tool_lists_at_40():
    tools = [f"mcp__big__tool_{index}" for index in range(50)]
    workspace = FakeWorkspace()
    inject_workspace_facts(
        [_mcp_ds(status="connected", tools=tools)],
        workspace,
    )

    text = workspace.files["README.md"]
    assert "mcp__big__tool_39" in text
    assert "mcp__big__tool_40" not in text
    assert "+10 more" in text
