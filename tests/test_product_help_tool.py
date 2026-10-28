"""Managed, workspace-independent SRW product-guide reader."""

from types import SimpleNamespace

from shared.runtime.core.skill_resolution import (
    APP_GUIDE_BREAK_GLASS_ENV,
    APP_GUIDE_LOADER_TOOL,
    APP_GUIDE_SKILL,
    add_persistent_system_skills,
    skill_bundle_digest,
)
from agent.tools.context import ToolContext
from agent.tools.product_help import create_product_help_tools
from agent.tools.registry import filter_tools_by_backend, load_tools


def _context(files: dict[str, str] | None = None) -> ToolContext:
    files = files or {
        "SKILL.md": (
            "---\nname: app-guide\ndescription: product help\n---\n"
            "ROUTE USING THE INDEX."
        ),
        "references/overview.md": "# Overview\nCURRENT OVERVIEW",
        "references/sessions.md": "# Sessions\nCURRENT SESSION HELP",
    }
    digest = skill_bundle_digest(files)
    return ToolContext(
        config={
            "_resolved_skills": {
                "menu": [
                    {
                        "name": APP_GUIDE_SKILL,
                        "system_managed": True,
                        "loader_tool": APP_GUIDE_LOADER_TOOL,
                        "bundle_digest": digest,
                    }
                ],
                "files": {APP_GUIDE_SKILL: files},
            }
        }
    )


def _reader(context: ToolContext):
    return {tool.name: tool for tool in create_product_help_tools(context)}[
        APP_GUIDE_LOADER_TOOL
    ]


def test_reader_works_without_a_workspace_and_lists_current_topics():
    context = _context()
    assert context.workspace_manager is None

    out = _reader(context).invoke({"topic_id": "index"})

    assert "ROUTE USING THE INDEX" in out
    assert "[available topic IDs]" in out
    assert "overview" in out and "sessions" in out
    assert "sha256:" in out


def test_reader_returns_procedure_and_one_focused_reference():
    out = _reader(_context()).invoke({"topic_id": "sessions"})

    assert "[guide procedure]" in out
    assert "ROUTE USING THE INDEX" in out
    assert "[product guide topic: sessions]" in out
    assert "CURRENT SESSION HELP" in out
    assert "CURRENT OVERVIEW" not in out


def test_reader_serves_current_email_and_okf_topics_independently():
    catalog = add_persistent_system_skills({})
    reader = _reader(ToolContext(config={"_resolved_skills": catalog}))

    email = reader.invoke({"topic_id": "datasources-email"})
    okf = reader.invoke({"topic_id": "datasources-okf"})

    assert "Folder allowlist" in email
    assert "OKF Root Path" not in email
    assert "OKF Root Path" in okf
    assert "Folder allowlist" not in okf


def test_reader_serves_current_automation_and_fleet_topics_independently():
    catalog = add_persistent_system_skills({})
    reader = _reader(ToolContext(config={"_resolved_skills": catalog}))

    automations = reader.invoke({"topic_id": "automations"})
    fleet = reader.invoke({"topic_id": "fleet-and-delegation"})

    assert "Scheduled delivery is at-least-once" in automations
    assert "Fleet Management does not delete jobs" not in automations
    assert "Fleet Management does not delete jobs" in fleet
    assert "Scheduled delivery is at-least-once" not in fleet


def test_reader_serves_canvas_and_permissions_topics_independently():
    catalog = add_persistent_system_skills({})
    reader = _reader(ToolContext(config={"_resolved_skills": catalog}))

    canvas = reader.invoke({"topic_id": "canvas-and-browser"})
    permissions = reader.invoke({"topic_id": "permissions-and-availability"})

    assert "Take control" in canvas
    assert "global, project, and user scope" not in canvas
    assert "global, project, and user scope" in permissions
    assert "Take control" not in permissions


def test_reader_serves_project_loop_and_protected_cloud_topics_independently():
    catalog = add_persistent_system_skills({})
    reader = _reader(ToolContext(config={"_resolved_skills": catalog}))

    loop = reader.invoke({"topic_id": "project-loops"})
    protected = reader.invoke({"topic_id": "protected-cloud"})

    assert "close the campaign, not the overall project loop" in loop
    assert "Reject is permanent" not in loop
    assert "Only one folder can be protected" in protected
    assert "Reject is permanent" in protected
    assert "close the campaign, not the overall project loop" not in protected


def test_reader_serves_jobs_experts_and_memory_topics_independently():
    catalog = add_persistent_system_skills({})
    reader = _reader(ToolContext(config={"_resolved_skills": catalog}))

    jobs = reader.invoke({"topic_id": "jobs"})
    experts = reader.invoke({"topic_id": "experts"})
    memory = reader.invoke({"topic_id": "memory-and-knowledge"})

    assert "Some pause reasons are retried or redispatched" in jobs
    assert "Which default wins" not in jobs
    assert "Which default wins" in experts
    assert "Context compaction is not long-term memory" not in experts
    assert "Context compaction is not long-term memory" in memory
    assert "Some pause reasons are retried or redispatched" not in memory


def test_reader_rejects_paths_and_unknown_topics_without_reading_them():
    reader = _reader(_context())

    traversal = reader.invoke({"topic_id": "../sessions"})
    unknown = reader.invoke({"topic_id": "not-a-topic"})

    assert "file paths are not accepted" in traversal
    assert "Unknown product-guide topic" in unknown
    assert "CURRENT SESSION HELP" not in traversal
    assert "CURRENT SESSION HELP" not in unknown


def test_reader_fails_closed_when_bundle_digest_changes():
    context = _context()
    context.config["_resolved_skills"]["files"][APP_GUIDE_SKILL][
        "references/sessions.md"
    ] = "MUTATED AFTER RESOLUTION"

    out = _reader(context).invoke({"topic_id": "sessions"})

    assert "unavailable" in out.lower()
    assert "MUTATED AFTER RESOLUTION" not in out


def test_break_glass_withholds_reader_even_from_a_direct_loader_request(monkeypatch):
    monkeypatch.setenv(APP_GUIDE_BREAK_GLASS_ENV, "true")
    context = _context()

    assert create_product_help_tools(context) == []
    assert load_tools([APP_GUIDE_LOADER_TOOL], context) == []


def test_product_help_tool_survives_none_backend_filter_and_loads():
    none_backend = SimpleNamespace(
        supports_shell=False,
        supports_file_tools=False,
        supports_canvas_presentation=False,
    )

    assert filter_tools_by_backend([APP_GUIDE_LOADER_TOOL], none_backend) == [
        APP_GUIDE_LOADER_TOOL
    ]
    loaded = load_tools([APP_GUIDE_LOADER_TOOL], _context())
    assert [tool.name for tool in loaded] == [APP_GUIDE_LOADER_TOOL]
