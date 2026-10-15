"""Agent Skills — Slice 3 (expert bindings + migration).

Covers the skill->binding mechanism: InstructionFileEntry's skill: field + path
resolver, the before_tool/phase-start consumers resolving via entry.path, the
flag-independent serialize channel for bound skills, and the migration of the
two bundled guides (research-guide, todo-guide).

Design: knowledge-base/knowledge/features/agent_skills.md (Slice 3).
Plan:   knowledge-base/knowledge/superpowers/plans/2026-06-19-skills-slice-3.md
"""

from shared.runtime.core.srw_manifest_config import (
    srw_config_fragment as _srw_config_fragment,
)

import pytest

from shared.runtime.core.loader import InstructionFileEntry, load_agent_config_from_dict


# ---------------------------------------------------------------------------
# Task 1: InstructionFileEntry skill: field + path resolver + XOR validation
# ---------------------------------------------------------------------------


def test_file_entry_path_is_the_file():
    e = InstructionFileEntry(
        trigger="before_tool:next_phase_todos", file="todo_guide.md"
    )
    assert e.path == "todo_guide.md"
    assert e.trigger_type == "before_tool"
    assert e.trigger_target == "next_phase_todos"


def test_skill_entry_path_is_skill_md():
    e = InstructionFileEntry(
        trigger="phase_start:tactical", skill="research-guide", enforce=False
    )
    assert e.path == "skills/research-guide/SKILL.md"
    assert e.trigger_type == "phase_start"
    assert e.trigger_target == "tactical"


def test_entry_requires_exactly_one_of_file_or_skill():
    with pytest.raises(ValueError):
        InstructionFileEntry(trigger="phase:tactical")  # neither
    with pytest.raises(ValueError):
        InstructionFileEntry(trigger="phase:tactical", file="x.md", skill="x")  # both


def test_parse_skill_binding_from_config():
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "instruction_files": [
                {
                    "skill": "research-guide",
                    "trigger": "phase_start:tactical",
                    "enforce": False,
                },
                {
                    "skill": "verify-before-done",
                    "trigger": "before_tool:todo_complete",
                    "phases": ["tactical"],
                    "read_scope": "phase",
                    "max_read_age_turns": 20,
                },
                {
                    "file": "todo_guide.md",
                    "trigger": "before_tool:next_phase_todos",
                },
            ],
        }
    )
    by_path = {e.path: e for e in cfg.instruction_files}
    assert "skills/research-guide/SKILL.md" in by_path
    assert by_path["skills/research-guide/SKILL.md"].skill == "research-guide"
    assert by_path["skills/research-guide/SKILL.md"].enforce is False
    verification = by_path["skills/verify-before-done/SKILL.md"]
    assert verification.phases == ["tactical"]
    assert verification.read_scope == "phase"
    assert verification.max_read_age_turns == 20
    assert by_path["todo_guide.md"].file == "todo_guide.md"
    assert by_path["todo_guide.md"].enforce is True  # default


@pytest.mark.parametrize(
    "kwargs",
    [
        {"phases": "tactical"},
        {"phases": ["invalid"]},
        {"read_scope": "turn"},
        {"max_read_age_turns": 0},
        {"max_read_age_turns": True},
    ],
)
def test_instruction_activation_options_validate(kwargs):
    with pytest.raises(ValueError):
        InstructionFileEntry(
            trigger="before_tool:todo_complete",
            skill="verify-before-done",
            **kwargs,
        )


# ---------------------------------------------------------------------------
# Task 2: before_tool + phase consumers resolve via entry.path
# ---------------------------------------------------------------------------

from types import SimpleNamespace  # noqa: E402

from tests._fs_backend import FilesystemTestBackend  # noqa: E402
from agent.core.workspace import WorkspaceManager  # noqa: E402
from agent.tools.context import ToolContext  # noqa: E402
from agent.tools.registry import apply_instruction_enforcement  # noqa: E402


def _ctx(tmp_path, entries):
    ws = WorkspaceManager(job_id="t", backend=FilesystemTestBackend(tmp_path))
    ctx = ToolContext(workspace_manager=ws)
    ctx._instruction_files = entries
    ctx._llm_config = None
    return ctx


def test_before_tool_gate_targets_skill_path(tmp_path):
    ctx = _ctx(
        tmp_path,
        [
            InstructionFileEntry(
                trigger="before_tool:next_phase_todos", skill="todo-guide"
            )
        ],
    )
    assert ctx.get_enforcement_files("next_phase_todos") == [
        "skills/todo-guide/SKILL.md"
    ]
    # gate closed until the skill path is read
    assert ctx.check_tool_enforcement("next_phase_todos") is not None
    ctx.record_file_read(
        "skills/todo-guide/SKILL.md"
    )  # what use_skill / read_file record
    assert ctx.check_tool_enforcement("next_phase_todos") is None


def test_phase_binding_targets_skill_path(tmp_path):
    ctx = _ctx(
        tmp_path,
        [
            InstructionFileEntry(
                trigger="phase_start:tactical",
                skill="research-guide",
                enforce=False,
            )
        ],
    )
    entries = ctx.get_phase_instruction_files("tactical")
    assert len(entries) == 1 and entries[0].path == "skills/research-guide/SKILL.md"
    assert ctx.get_phase_instruction_files("strategic") == []


def test_apply_enforcement_wrapper_uses_skill_path(tmp_path):
    ctx = _ctx(
        tmp_path,
        [
            InstructionFileEntry(
                trigger="before_tool:next_phase_todos", skill="todo-guide"
            )
        ],
    )
    calls = []
    tool = SimpleNamespace(
        name="next_phase_todos", func=lambda *a, **k: (calls.append(1), "OK")[1]
    )
    apply_instruction_enforcement([tool], ctx)
    blocked = tool.func()
    assert "skills/todo-guide/SKILL.md" in blocked and calls == []  # nudged, not run
    ctx.record_file_read("skills/todo-guide/SKILL.md")
    assert tool.func() == "OK" and calls == [1]  # gate opened


@pytest.mark.asyncio
async def test_apply_enforcement_wraps_async_tool_coroutine(tmp_path):
    """Async @tool functions (cite_web / cite_document) are invoked via
    .coroutine (.ainvoke), never .func — so enforcement must wrap the coroutine,
    or the before_tool gate is a silent no-op for them."""
    ctx = _ctx(
        tmp_path,
        [
            InstructionFileEntry(
                trigger="before_tool:cite_web", skill="cite-as-you-write"
            )
        ],
    )
    calls = []

    async def _cite(*a, **k):
        calls.append(1)
        return "CITED"

    tool = SimpleNamespace(name="cite_web", func=None, coroutine=_cite)
    apply_instruction_enforcement([tool], ctx)

    blocked = await tool.coroutine()
    assert "skills/cite-as-you-write/SKILL.md" in blocked and calls == []  # nudged
    ctx.record_file_read("skills/cite-as-you-write/SKILL.md")
    assert await tool.coroutine() == "CITED" and calls == [1]  # gate opened


# ---------------------------------------------------------------------------
# Task 3: bound-skill content rides the flag-independent instructions channel
# ---------------------------------------------------------------------------

from shared.runtime.core.loader import serialize_resolved_config  # noqa: E402


def test_serialize_freezes_bound_skill_md(tmp_path):
    cfg = load_agent_config_from_dict(
        {
            "agent_id": "t",
            "display_name": "T",
            "instruction_files": [
                {
                    "skill": "word-count",
                    "trigger": "phase_start:tactical",
                    "enforce": False,
                },
                {
                    "skill": "verify-before-done",
                    "trigger": "before_tool:todo_complete",
                    "phases": ["tactical"],
                    "read_scope": "phase",
                    "max_read_age_turns": 20,
                },
            ],
        }
    )
    cfg._deployment_dir = str(
        tmp_path
    )  # matrix/prompt resolvers need a dir; files absent → None
    blob = serialize_resolved_config(cfg)
    # Frozen under the skill name (not "SKILL"), independent of the catalog flag.
    assert "word-count" in blob["instructions"]
    assert "Word Count" in blob["instructions"]["word-count"]
    assert "SKILL" not in blob["instructions"]  # no stem collision
    verify_binding = next(
        entry
        for entry in blob["agent"]["instruction_files"]
        if entry.get("skill") == "verify-before-done"
    )
    assert verify_binding["phases"] == ["tactical"]
    assert verify_binding["read_scope"] == "phase"
    assert verify_binding["max_read_age_turns"] == 20


# ---------------------------------------------------------------------------
# Task 5: research_guide migrated to a once-per-phase bundled skill binding
# ---------------------------------------------------------------------------

from pathlib import Path as _P  # noqa: E402

from shared.runtime.core.skill_format import parse_skill_md, skill_identity  # noqa: E402


def test_research_guide_skill_exists_and_parses():
    md = (_P("config/skills/research-guide/SKILL.md")).read_text(encoding="utf-8")
    fm, body = parse_skill_md(md)
    name, _desc = skill_identity(fm)
    assert name == "research-guide"
    assert "Research Workflow" in body  # the migrated body survived


def test_scholar_binds_research_guide_as_skill():
    import yaml

    cfg = _srw_config_fragment(
        yaml.safe_load(_P("config/experts/scholar/config.yaml").read_text())
    )
    entries = cfg["instruction_files"]
    research = [e for e in entries if e.get("skill") == "research-guide"]
    assert len(research) == 1
    assert research[0]["trigger"] == "phase_start:tactical"
    assert research[0]["enforce"] is False
    assert not any(
        e.get("file") == "research_guide.md" for e in entries
    )  # old ref gone


# ---------------------------------------------------------------------------
# Task 6: todo_guide migrated to a bundled skill + matrix special-case removed
# ---------------------------------------------------------------------------


def test_todo_guide_skill_exists_and_parses():
    md = (_P("config/skills/todo-guide/SKILL.md")).read_text(encoding="utf-8")
    fm, body = parse_skill_md(md)
    name, _desc = skill_identity(fm)
    assert name == "todo-guide"
    # Body is real and on-topic — anchored on the tool it gates rather than on
    # exact wording, so prose edits (e.g. the L3 phase-patterns offload) don't
    # break this. Detailed examples live in references/phase-patterns.md.
    assert "next_phase_todos" in body
    assert len(body) > 500


def _bindings_source(config_path: str) -> dict:
    """A bundled config's raw YAML, or — for the public base name
    ``worker_base`` — the merged worker role base (``expert_base`` + the worker
    overlay, where ``instruction_files`` lives since the U1 split)."""
    import yaml

    from shared.runtime.core.loader import load_role_base

    if config_path == "worker_base":
        return load_role_base("worker")
    return _srw_config_fragment(yaml.safe_load(_P(config_path).read_text()))


def test_worker_base_binds_todo_guide_as_skill():
    cfg = _bindings_source("worker_base")
    entries = cfg["instruction_files"]
    todo = [e for e in entries if e.get("skill") == "todo-guide"]
    assert len(todo) == 1
    assert todo[0]["trigger"] == "before_tool:next_phase_todos"
    assert todo[0]["enforce"] is True
    assert not any(e.get("file") == "todo_guide.md" for e in entries)


def test_interactive_designer_uses_an_action_gate_not_setup_injection():
    import yaml

    cfg = _srw_config_fragment(
        yaml.safe_load(
            _P("config/experts/designer-interactive/config.yaml").read_text()
        )
    )
    assert cfg["instruction_files"] == [
        {
            "file": "design_guide.md",
            "trigger": "before_tool:write_file",
            "enforce": True,
        }
    ]


@pytest.mark.parametrize(
    "config_path",
    [
        "worker_base",
        "config/experts/scholar/config.yaml",
        "config/experts/product-qa/config.yaml",
        "config/experts/designer/config.yaml",
    ],
)
def test_verify_before_done_is_passive_and_freshness_scoped(config_path):
    cfg = _bindings_source(config_path)
    entries = [
        entry
        for entry in cfg["instruction_files"]
        if entry.get("skill") == "verify-before-done"
    ]
    assert {entry["trigger"] for entry in entries} == {
        "before_tool:todo_complete",
        "before_tool:job_complete",
    }
    by_trigger = {entry["trigger"]: entry for entry in entries}
    assert by_trigger["before_tool:todo_complete"]["phases"] == ["tactical"]
    assert by_trigger["before_tool:job_complete"]["phases"] == ["strategic"]
    for entry in entries:
        assert entry["enforce"] is True
        assert entry["read_scope"] == "phase"
        assert entry["max_read_age_turns"] == 20


def test_runtime_and_cockpit_schemas_accept_bounded_skill_bindings():
    import json

    import jsonschema

    runtime = json.loads(_P("config/schema.json").read_text())["properties"][
        "instruction_files"
    ]
    cockpit = json.loads(_P("cockpit/src/assets/schema.json").read_text())[
        "properties"
    ]["instruction_files"]
    assert runtime == cockpit

    bindings = [
        {
            "skill": "research-guide",
            "trigger": "phase_start:tactical",
            "enforce": False,
        },
        {
            "skill": "verify-before-done",
            "trigger": "before_tool:todo_complete",
            "enforce": True,
            "phases": ["tactical"],
            "read_scope": "phase",
            "max_read_age_turns": 20,
        },
    ]
    jsonschema.validate(bindings, runtime)
    for config_path in (
        "worker_base",
        "config/experts/assistant/config.yaml",
        "config/experts/designer/config.yaml",
        "config/experts/designer-interactive/config.yaml",
        "config/experts/product-qa/config.yaml",
        "config/experts/scholar/config.yaml",
    ):
        config = _bindings_source(config_path)
        jsonschema.validate(config["instruction_files"], runtime)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(
            [{"file": "x.md", "skill": "x", "trigger": "phase_start:tactical"}],
            runtime,
        )


def test_todo_guide_dropped_from_instruction_matrix():
    from shared.runtime.core.loader import InstructionMatrixResolver

    assert "todo_guide" not in InstructionMatrixResolver.HARDCODED_DEFAULTS


# ---------------------------------------------------------------------------
# U2 WP2: the phase skills — worker bindings, expert-local overrides, freeze
# ---------------------------------------------------------------------------

from shared.runtime.core.loader import (  # noqa: E402
    PHASE_SKILLS,
    chain_root,
    load_agent_config,
    load_and_merge_config,
    load_role_base,
    resolve_bound_skill_dir,
    resolve_config_path,
)

_PHASE_BINDINGS = {
    "strategic-phase": "phase_start:strategic",
    "tactical-phase": "phase_start:tactical",
}


def _phase_bindings(entries):
    return {e["skill"]: e for e in entries if e.get("skill") in _PHASE_BINDINGS}


def test_worker_base_binds_both_phase_skills():
    entries = load_role_base("worker")["instruction_files"]
    # At the top of the list, before the enforced action gates.
    assert [e.get("skill") for e in entries[:2]] == list(_PHASE_BINDINGS)
    for skill, trigger in _PHASE_BINDINGS.items():
        binding = _phase_bindings(entries)[skill]
        assert binding["trigger"] == trigger
        assert binding["enforce"] is False
        assert "file" not in binding
    # The session overlay has no phase loop and no phase bindings.
    assert not _phase_bindings(load_role_base("session").get("instruction_files") or [])


def _worker_expert_leaves():
    leaves = [
        p
        for p in sorted(_P("config/experts").glob("*/config.yaml"))
        if chain_root(str(p)) == "worker_base"
    ]
    assert len(leaves) >= 6
    return leaves


@pytest.mark.parametrize("leaf", _worker_expert_leaves(), ids=lambda p: p.parent.name)
def test_every_worker_expert_resolves_both_phase_bindings(leaf):
    """deep_merge replaces lists wholesale: an expert that declares its own
    instruction_files (scholar, product-qa, designer) must restate the two
    phase bindings or silently lose its phase guidance."""
    for role in (None, "worker"):
        data = load_and_merge_config(str(leaf), role=role)
        bindings = _phase_bindings(data["instruction_files"])
        assert set(bindings) == set(_PHASE_BINDINGS), (leaf.parent.name, role)
        for skill, trigger in _PHASE_BINDINGS.items():
            assert bindings[skill]["trigger"] == trigger
            assert bindings[skill]["enforce"] is False
    cfg = load_agent_config(str(leaf), str(leaf.parent))
    parsed = {e.skill: e for e in cfg.instruction_files if e.skill in _PHASE_BINDINGS}
    assert {e.path for e in parsed.values()} == {
        "skills/strategic-phase/SKILL.md",
        "skills/tactical-phase/SKILL.md",
    }


def test_expert_local_phase_skill_overrides_bundled():
    """Location-primary like prompt files: the developer ships its own
    skills/<phase>-phase/SKILL.md, which outranks config/skills/<name>; an
    expert without one (writer) and a config without a deployment dir fall
    back to the bundled skill. Binding and workspace path do not change."""
    _dev_path, dev_dir = resolve_config_path("developer")
    _writer_path, writer_dir = resolve_config_path("writer")
    bundled = _P("config/skills").resolve()
    for phase, skill in PHASE_SKILLS.items():
        local = resolve_bound_skill_dir(skill, dev_dir)
        assert local == _P(dev_dir) / "skills" / skill
        assert (local / "SKILL.md").is_file()
        assert resolve_bound_skill_dir(skill, writer_dir) == bundled / skill
        assert resolve_bound_skill_dir(skill, None) == bundled / skill
        assert resolve_bound_skill_dir(skill, "/nonexistent/dir") == bundled / skill
        fm, body = parse_skill_md((local / "SKILL.md").read_text(encoding="utf-8"))
        assert skill_identity(fm)[0] == skill  # same name -> same binding
        assert f"You are in {phase.upper()} mode." in body
    # The developer's body is the developer's, not the generic one.
    dev_strategic = (
        resolve_bound_skill_dir("strategic-phase", dev_dir) / "SKILL.md"
    ).read_text(encoding="utf-8")
    assert "tdd_phase" in dev_strategic
    assert "tdd_phase" not in (bundled / "strategic-phase" / "SKILL.md").read_text()


def test_serialize_freezes_expert_local_phase_skill():
    dev_path, dev_dir = resolve_config_path("developer")
    cfg = load_agent_config(dev_path, dev_dir)
    blob = serialize_resolved_config(cfg, model=cfg.llm.model)
    frozen = blob["instructions"]
    for skill in PHASE_SKILLS.values():
        assert frozen[skill] == (_P(dev_dir) / "skills" / skill / "SKILL.md").read_text(
            encoding="utf-8"
        )
        assert frozen[skill].startswith("---\nname: " + skill)
    assert "tdd_phase" in frozen["strategic-phase"]
    # An expert without a local override freezes the bundled body.
    writer_path, writer_dir = resolve_config_path("writer")
    wcfg = load_agent_config(writer_path, writer_dir)
    wblob = serialize_resolved_config(wcfg, model=wcfg.llm.model)
    assert wblob["instructions"]["strategic-phase"] == (
        _P("config/skills/strategic-phase/SKILL.md").read_text(encoding="utf-8")
    )
    # The bindings and the workspace paths are unchanged by the override.
    dev_bindings = [
        e
        for e in blob["agent"]["instruction_files"]
        if e["skill"] in PHASE_SKILLS.values()
    ]
    assert [e["trigger"] for e in dev_bindings] == [
        "phase_start:strategic",
        "phase_start:tactical",
    ]


def test_runtime_and_cockpit_schemas_agree_on_the_live_voice_entry():
    """The roster entry's ``driver: live`` + ``live`` block and
    ``phase_settings.strategic_tools`` (live_session_jobs §6.3): one definition
    in both schemas, and the bundled receptionist validates against it."""
    import json

    import jsonschema
    import yaml

    runtime = json.loads(_P("config/schema.json").read_text())
    cockpit = json.loads(_P("cockpit/src/assets/schema.json").read_text())
    entry = runtime["$defs"]["subagentRosterEntry"]
    assert entry == cockpit["$defs"]["subagentRosterEntry"]
    assert entry["properties"]["driver"]["enum"] == ["live"]
    live = entry["properties"]["live"]["properties"]
    assert {
        "persona",
        "persona_file",
        "facts",
        "facts_file",
        "greeting",
        "voice",
        "answer_timeout_s",
        "texts",
    } == set(live)
    assert set(live["texts"]["properties"]) == {"fallback", "follow_up", "goodbye"}
    strategic_tools = runtime["properties"]["phase_settings"]["properties"][
        "strategic_tools"
    ]
    assert (
        strategic_tools
        == cockpit["properties"]["phase_settings"]["properties"]["strategic_tools"]
    )

    validator = jsonschema.Draft202012Validator(runtime)
    desk = yaml.safe_load(_P("config/experts/receptionist/config.yaml").read_text())
    authored = desk["spec"]["runtime"]["config"]["config"]
    errors = [e for e in validator.iter_errors(authored) if e.validator != "required"]
    assert not errors, [e.message for e in errors]
    bad = json.loads(json.dumps(authored))
    voice = bad["subagents"]["roster"]["voice"]
    voice["live"]["persona_file"] = "persona.txt"
    voice["driver"] = "phone"
    messages = [e.message for e in validator.iter_errors(bad)]
    assert any("'phone' is not one of ['live']" in m for m in messages)
    assert any("should not be valid" in m for m in messages)
