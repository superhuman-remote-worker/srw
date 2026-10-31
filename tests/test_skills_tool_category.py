"""``use_skill`` as its own tool category.

knowledge-base/knowledge/features/skills_tool_category.md: the tool moved out of
``workspace`` (D1), the skill list follows it (D2, tested in
test_skill_resolution.py / test_persistent_session.py), the assembler places the
list when a template has no slot for it (D3), a ``before_tool`` skill binding
brings the tool (D4), stored layers are converted (D5), and the file-tool gates
apply to it (D6).
"""

from types import SimpleNamespace

from agent.tools.registry import (
    TOOL_REGISTRY,
    apply_officer_tool_ceiling,
    filter_tools_by_backend,
)
from orchestrator.services.config_resolver import resolve_config
from shared.runtime.core.loader import (
    get_phase_system_prompt,
    get_subagent_system_prompt,
    get_system_prompt,
    load_agent_config_from_dict,
)
from shared.runtime.core.tool_policy import (
    normalize_tool_policy,
    validate_tool_override_fragment,
)
from shared.runtime.core.tool_report import backend_blocked_categories

MENU = [{"name": "hello-skill", "description": "Use when testing."}]


# ── D1: the category ──────────────────────────────────────────────────────────
def test_use_skill_is_the_skills_category():
    assert TOOL_REGISTRY["use_skill"]["category"] == "skills"


def test_an_expert_rewriting_its_workspace_list_keeps_use_skill():
    """The designer's own workspace list used to replace the base's, dropping
    use_skill; the skills group is inherited untouched now."""
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row={
            "name": "narrow",
            "config": {"tools": {"workspace": ["read_file", "write_file"]}},
        },
        expert_type="worker",
    )
    tools = blob["agent"]["tools"]
    assert tools["workspace"] == ["read_file", "write_file"]
    assert tools["skills"] == ["use_skill"]


# ── D5: stored layers and requests are converted ──────────────────────────────
def test_stored_workspace_list_naming_use_skill_moves_to_skills():
    layer = {"tools": {"workspace": ["read_file", "use_skill"]}}
    out = normalize_tool_policy(layer, source="stored-expert")
    assert out["tools"]["workspace"] == ["read_file"]
    assert out["tools"]["skills"] == ["use_skill"]
    assert layer["tools"]["workspace"] == ["read_file", "use_skill"]  # not mutated


def test_only_form_moves_too():
    out = normalize_tool_policy({"tools": {"workspace": {"only": ["use_skill"]}}})
    assert out["tools"]["workspace"] == []
    assert out["tools"]["skills"] == ["use_skill"]


def test_except_naming_use_skill_turns_skills_off():
    out = normalize_tool_policy(
        {"tools": {"workspace": {"except": ["use_skill", "delete_file"]}}}
    )
    assert "delete_file" not in out["tools"]["workspace"]
    assert "read_file" in out["tools"]["workspace"]
    assert out["tools"]["skills"] == []

    only_skill = normalize_tool_policy(
        {"tools": {"workspace": {"except": ["use_skill"]}}}
    )
    assert "read_file" in only_skill["tools"]["workspace"]
    assert only_skill["tools"]["skills"] == []


def test_a_layer_that_sets_skills_itself_wins():
    out = normalize_tool_policy(
        {"tools": {"workspace": ["read_file", "use_skill"], "skills": []}}
    )
    assert out["tools"]["workspace"] == ["read_file"]
    assert out["tools"]["skills"] == []


def test_request_override_naming_use_skill_under_workspace_is_accepted():
    accepted = validate_tool_override_fragment(
        {"tools": {"workspace": ["read_file", "use_skill"]}}
    )
    assert accepted == {"workspace": ["read_file"], "skills": ["use_skill"]}


# ── D4: a before_tool skill binding brings the tool ───────────────────────────
def _config(tools, bindings):
    return load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "tools": tools,
            "instruction_files": bindings,
        }
    )


def test_before_tool_binding_adds_use_skill_even_when_skills_are_off():
    cfg = _config(
        {"skills": [], "core": ["todo_complete"]},
        [{"skill": "verify-before-done", "trigger": "before_tool:todo_complete"}],
    )
    assert cfg.tools.skills == ["use_skill"]


def test_phase_start_binding_does_not_add_use_skill():
    cfg = _config(
        {"skills": []},
        [
            {
                "skill": "strategic-phase",
                "trigger": "phase_start:strategic",
                "enforce": False,
            }
        ],
    )
    assert cfg.tools.skills == []


def test_a_frozen_blob_listing_use_skill_under_workspace_is_not_duplicated():
    cfg = _config(
        {"workspace": ["read_file", "use_skill"]},
        [{"skill": "todo-guide", "trigger": "before_tool:next_phase_todos"}],
    )
    assert cfg.tools.skills == []
    assert cfg.tools.workspace == ["read_file", "use_skill"]


def test_the_binding_derived_tool_is_in_the_frozen_blob():
    blob = resolve_config(
        base_config_name="worker_base",
        expert_row={"name": "no-skills", "config": {"tools": {"skills": []}}},
        expert_type="worker",
    )
    # worker_base binds todo-guide / verify-before-done as before_tool gates.
    assert blob["agent"]["tools"]["skills"] == ["use_skill"]


# ── D3: the assembler places the list ─────────────────────────────────────────
def _worker(template):
    return load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "_resolved_prompts": {"systemprompt": template},
            "_resolved_skills": {"menu": MENU},
        }
    )


def test_worker_template_without_a_slot_gets_the_menu_appended():
    out = get_system_prompt(_worker("BASE {agent_display_name}"), tool_names=[])
    assert out.count("<available_skills") == 1
    assert out.index("BASE T") < out.index("<available_skills")


def test_worker_template_with_a_slot_keeps_its_spot_and_gets_it_once():
    out = get_system_prompt(
        _worker("A {available_skills} Z {agent_display_name}"), tool_names=[]
    )
    assert out.count("<available_skills") == 1
    assert out.index("<available_skills") < out.index("Z T")


def test_no_menu_no_block():
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "_resolved_prompts": {"systemprompt": "BASE"},
        }
    )
    assert "<available_skills" not in get_system_prompt(cfg, tool_names=[])


def test_session_template_without_a_slot_gets_the_menu_before_the_guide_floor():
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "_resolved_prompts": {"systemprompt_interactive": "SESSION BODY"},
            "_resolved_skills": {"menu": MENU},
        }
    )
    out = get_phase_system_prompt(
        cfg, is_strategic=False, prompt_type="interactive", tool_names=["use_skill"]
    )
    assert out.count("<available_skills") == 1
    assert out.index("SESSION BODY") < out.index("<available_skills")


def test_subagent_template_without_a_slot_gets_the_menu():
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "c",
            "display_name": "Child",
            "_resolved_prompts": {
                "systemprompt_subagent": "CHILD {subagent_environment}"
            },
            "_resolved_skills": {"menu": MENU},
        }
    )
    out = get_subagent_system_prompt(cfg, tool_names=[], environment="ENV")
    assert out.count("<available_skills") == 1


def test_bundled_family_templates_without_a_slot_now_carry_the_menu():
    """GPT-5's session template has no {available_skills}; its agents used to
    get no skill list at all."""
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "_resolved_skills": {"menu": MENU},
        }
    )
    out = get_phase_system_prompt(
        cfg,
        is_strategic=False,
        prompt_type="interactive",
        model="gpt-5",
        tool_names=["use_skill"],
    )
    assert out.count("- hello-skill: Use when testing.") == 1


# ── D6: the file-tool gates ───────────────────────────────────────────────────
def test_backend_without_file_tools_drops_skills():
    backend = SimpleNamespace(
        supports_shell=False,
        supports_file_tools=False,
        supports_canvas_presentation=False,
    )
    assert filter_tools_by_backend(["use_skill", "web_search"], backend) == [
        "web_search"
    ]
    assert "skills" in backend_blocked_categories({"supports_file_tools": False})


def test_officer_ceiling_denies_skills():
    kept = apply_officer_tool_ceiling(["use_skill", "web_search"], {"enabled": True})
    assert kept == ["web_search"]
