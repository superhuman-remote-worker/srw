"""Whether a job plans in phases is written into the frozen configuration.

``resolve_config`` rewrites a worker job's merged config when it does not plan
in phases (``phases`` absent or false;
``shared.runtime.core.loader.apply_no_phases``), and
``serialize_resolved_config`` freezes the direct task's to-dos and
instructions under the keys the worker already reads. A job with
``phases: true`` keeps the phase-skill floor and the procedure's texts. Either
way the blob states the effective value. These tests resolve every bundled
worker expert, an inline expert and bare ``worker_base`` and run the result
through the worker's unchanged loaders, getters and phase-gate expression.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from orchestrator.services.config_resolver import resolve_config
from shared.job_phases import (
    DIRECT_TASK_SKILL,
    PHASE_PROCEDURE_SKILLS,
    PHASE_PROCEDURE_TOOLS,
)
from shared.runtime.core.loader import (
    DIRECT_TASK_INSTRUCTION_TYPES,
    InstructionMatrixResolver,
    apply_no_phases,
    get_all_tool_names,
    get_project_root,
    load_agent_config_from_dict,
    load_config_from_resolved,
    serialize_resolved_config,
)

ROOT = Path(__file__).resolve().parents[1]
EXPERTS = ROOT / "config" / "experts"
TEMPLATES = ROOT / "config" / "templates"
MODEL = "deepseek-flash"


def _manifest(name: str) -> dict:
    from shared.manifests import parse_documents

    return parse_documents((EXPERTS / name / "config.yaml").read_text())[0]


def _worker_experts() -> list[str]:
    return sorted(
        path.parent.name
        for path in EXPERTS.glob("*/config.yaml")
        if (_manifest(path.parent.name).get("metadata") or {})
        .get("annotations", {})
        .get("srw.io/expert-type")
        == "worker"
    )


WORKER_EXPERTS = _worker_experts()


def _row(name: str) -> dict:
    """The resolver row of a bundled expert, as the catalogue stores it."""
    from shared.runtime.core.srw_manifest_config import srw_private_config

    private = srw_private_config(_manifest(name))
    return {
        "name": name,
        "expert_type": "worker",
        "harness_adapter": "srw/v1",
        "harness_config_name": private["config_name"],
        "harness_asset_name": private.get("asset_name", name),
        "harness_config_layers": private.get("layers", []),
        "config": private.get("config") or {},
        "prompts": {},
    }


def _inline_row(private: dict) -> dict:
    """The row admission builds from ``{"inline": {"runtime": ...}}``."""
    from orchestrator.schemas.inline_expert import InlineExpertSelection
    from orchestrator.services.inline_expert import admit_inline_expert

    selection = InlineExpertSelection.model_validate(
        {"inline": {"runtime": {"adapter": "srw/v1", "config": copy.deepcopy(private)}}}
    )
    return admit_inline_expert(
        selection.inline, role="worker", trusted_image="srw-agent:installed"
    )


INLINE = {
    "config_name": "worker_base",
    "config": {
        "agent_id": "inline-helper",
        "display_name": "Inline Helper",
        "tools": {"core": ["next_phase_todos", "todo_complete", "todo_list"]},
        "instruction_files": [
            {"skill": "strategic-phase", "trigger": "phase_start:strategic"},
            {"skill": "tactical-phase", "trigger": "phase_start:tactical"},
            {"skill": "research-guide", "trigger": "phase_start:tactical"},
            {
                "skill": "todo-guide",
                "trigger": "before_tool:next_phase_todos",
                "enforce": True,
            },
        ],
    },
}


def _resolve(source: str, *, capture: dict | None = None, **override) -> dict:
    request = {"llm": {"model": MODEL}, **override}
    if source == "worker_base":
        return resolve_config(
            base_config_name="worker_base",
            expert_type="worker",
            request_override=request,
            capture=capture,
        )
    row = _inline_row(INLINE) if source == "inline" else _row(source)
    return resolve_config(
        base_config_name="worker_base",
        expert_row=row,
        expert_type="worker",
        request_override=request,
        capture=capture,
    )


def _states_phases(source: str) -> bool:
    """True when the source's own configuration says ``phases: true``."""
    if source in ("worker_base", "inline"):
        return False
    return _row(source)["config"].get("phases") is True


def _strategic_gate_refusals(config, names: list[str]) -> set[str]:
    """The names the worker's per-call gate refuses in a strategic phase
    (``graph.create_audited_tool_node``)."""
    from agent.tools.registry import TOOL_REGISTRY, filter_tools_by_phase

    allowed = set(filter_tools_by_phase(names, "strategic")) | (
        set(config.phase_settings.strategic_tools or ()) & set(names)
    )
    gated = {name for name in names if name in TOOL_REGISTRY}
    return gated - allowed


SOURCES = [*WORKER_EXPERTS, "worker_base", "inline"]

#: The bundled worker experts whose files say ``phases: true``.
PHASED_EXPERTS = {
    "bughunter",
    "critic",
    "curator",
    "designer",
    "developer",
    "product-qa",
    "receptionist",
    "scholar",
}


def test_every_bundled_worker_expert_is_covered():
    assert {
        "engineer",
        "general-worker",
        "designer",
        "scholar",
        "critic",
        "developer",
    } <= set(WORKER_EXPERTS)


def test_which_bundled_experts_plan_in_phases():
    from orchestrator.services.expert_phases_backfill import SHIPPED_WITHOUT_PHASES

    assert {name for name in WORKER_EXPERTS if _states_phases(name)} == PHASED_EXPERTS
    # The startup backfill leaves exactly the others' catalogue copies alone.
    assert set(WORKER_EXPERTS) - PHASED_EXPERTS == set(SHIPPED_WITHOUT_PHASES)


@pytest.mark.parametrize("source", SOURCES)
class TestTheDefault:
    def test_the_blob_states_the_effective_value(self, source):
        blob = _resolve(source)
        assert blob["agent"]["phases"] is _states_phases(source)

    def test_without_the_key_the_job_runs_as_its_expert_says(self, source):
        unset = _resolve(source)
        named = _resolve(source, phases=_states_phases(source))
        unset.pop("resolved_at")
        named.pop("resolved_at")
        assert named == unset

    def test_only_an_expert_that_says_so_gets_the_procedure(self, source):
        agent = _resolve(source)["agent"]
        skills = {e.get("skill") for e in agent["instruction_files"]}
        if _states_phases(source):
            assert "next_phase_todos" in agent["tools"]["core"]
            assert DIRECT_TASK_SKILL not in skills
        else:
            assert not set(agent["tools"]["core"]) & set(PHASE_PROCEDURE_TOOLS)
            assert agent["instruction_files"][0]["skill"] == DIRECT_TASK_SKILL
            assert not skills & set(PHASE_PROCEDURE_SKILLS)


@pytest.mark.parametrize("source", SOURCES)
class TestNoPhasesBlob:
    def test_tools_bindings_and_gate(self, source):
        blob = _resolve(source, phases=False)
        agent = blob["agent"]
        assert agent["phases"] is False

        core = agent["tools"]["core"]
        assert not set(core) & set(PHASE_PROCEDURE_TOOLS)
        assert "job_complete" in core

        bindings = agent["instruction_files"]
        direct = [e for e in bindings if e.get("skill") == DIRECT_TASK_SKILL]
        assert direct == [
            {
                "skill": DIRECT_TASK_SKILL,
                "trigger": "phase_start:strategic",
                "file": None,
                "enforce": False,
                "phases": None,
                "read_scope": "job",
                "max_read_age_turns": None,
            }
        ]
        assert bindings[0]["skill"] == DIRECT_TASK_SKILL
        assert not [e for e in bindings if e.get("skill") in PHASE_PROCEDURE_SKILLS]
        assert not [e for e in bindings if e["trigger"].endswith(":tactical")]
        assert not [
            e
            for e in bindings
            if e["trigger"].startswith("before_tool:")
            and e["trigger"].split(":", 1)[1] in PHASE_PROCEDURE_TOOLS
        ]
        assert blob["instructions"][DIRECT_TASK_SKILL].startswith(
            "---\nname: direct-task"
        )

        config = load_config_from_resolved(blob)
        names = get_all_tool_names(config)
        assert "job_complete" in names
        assert not set(names) & set(PHASE_PROCEDURE_TOOLS)
        assert _strategic_gate_refusals(config, names) == set()

        from agent.tools.registry import TOOL_REGISTRY

        tactical_only = {
            name
            for name, meta in TOOL_REGISTRY.items()
            if "strategic" not in meta.get("phases", ["strategic", "tactical"])
        }
        # Datasource tools are bound at dispatch; the list covers them too.
        assert tactical_only <= set(config.phase_settings.strategic_tools)
        assert "job_complete" in config.phase_settings.strategic_tools

    def test_the_three_todo_getters_return_one_todo(self, source):
        from agent.core.phase import (
            get_initial_strategic_todos,
            get_resume_strategic_todos,
            get_transition_strategic_todos,
        )

        config = load_config_from_resolved(_resolve(source, phases=False))
        names = get_all_tool_names(config)
        for getter in (
            get_initial_strategic_todos,
            get_resume_strategic_todos,
            get_transition_strategic_todos,
        ):
            todos = getter(config, tool_names=names)
            assert len(todos) == 1, getter.__name__
            assert "job_complete" in todos[0].content

    def test_the_policy_capture_already_carries_the_rewrite(self, source):
        # The captured fragment is stored as the job's policy, whose `tools`
        # are merged back onto the frozen blob at every dispatch.
        from orchestrator.services.manifest_execution_snapshot import (
            apply_srw_delivery_bindings,
        )

        capture: dict = {}
        blob = _resolve(source, capture=capture, phases=False)
        policy = capture["merged_fragment"]
        core = policy["tools"]["core"]
        assert "job_complete" in core
        assert not set(core) & set(PHASE_PROCEDURE_TOOLS)

        # A dispatch delivers the blob with the stored policy as its override.
        delivered, checked = apply_srw_delivery_bindings(blob, policy, dict(policy))
        for tools in (delivered["agent"]["tools"], checked["tools"]):
            assert "job_complete" in tools["core"]
            assert not set(tools["core"]) & set(PHASE_PROCEDURE_TOOLS)

    def test_the_phased_blob_keeps_the_procedure(self, source):
        blob = _resolve(source, phases=True)
        agent = blob["agent"]
        assert agent["phases"] is True
        assert {"strategic-phase", "tactical-phase"} <= {
            e.get("skill") for e in agent["instruction_files"]
        }
        assert DIRECT_TASK_SKILL not in {
            e.get("skill") for e in agent["instruction_files"]
        }
        assert "next_phase_todos" in agent["tools"]["core"]
        assert not set(DIRECT_TASK_INSTRUCTION_TYPES) & set(blob["instructions"])
        assert DIRECT_TASK_SKILL not in blob["instructions"]

    def test_the_phased_texts_are_the_procedure_files(self, source):
        blob = _resolve(source, phases=True)
        deployment = (
            None if source in ("worker_base", "inline") else str(EXPERTS / source)
        )
        resolver = InstructionMatrixResolver(deployment, blob["model_family"])
        for key in (
            "instructions",
            "strategic_todos_initial",
            "strategic_todos_transition",
            "strategic_todos_resume",
        ):
            assert blob["instructions"][key] == resolver.load(key), key

    def test_on_and_off_read_as_true_and_false(self, source):
        for word, value in (("on", True), ("off", False)):
            spelled = _resolve(source, phases=word)
            stated = _resolve(source, phases=value)
            spelled.pop("resolved_at")
            stated.pop("resolved_at")
            assert spelled["agent"]["phases"] is value
            assert spelled == stated


class TestTheTransformAlone:
    def test_it_does_not_modify_its_input_and_is_idempotent(self):
        data = {
            "tools": {"core": ["todo_list", "next_phase_todos", "mark_complete"]},
            "instruction_files": [
                {"skill": "strategic-phase", "trigger": "phase_start:strategic"},
                {"file": "guide.md", "trigger": "phase:tactical", "enforce": False},
                {"skill": "verify-before-done", "trigger": "before_tool:job_complete"},
                {"skill": "x-gate", "trigger": "before_tool:todo_complete"},
                {"skill": "cite-as-you-write", "trigger": "before_tool:cite_web"},
            ],
            "phase_settings": {"min_todos": 2, "strategic_tools": ["sql_query"]},
        }
        before = copy.deepcopy(data)
        once = apply_no_phases(data)
        assert data == before
        assert apply_no_phases(once) == once

        assert once["tools"]["core"] == ["job_complete"]
        assert once["instruction_files"] == [
            {
                "skill": DIRECT_TASK_SKILL,
                "trigger": "phase_start:strategic",
                "enforce": False,
            },
            {"file": "guide.md", "trigger": "phase_start:strategic", "enforce": False},
            {"skill": "cite-as-you-write", "trigger": "before_tool:cite_web"},
        ]
        strategic_tools = once["phase_settings"]["strategic_tools"]
        assert strategic_tools[0] == "sql_query"
        assert strategic_tools[-1] == "job_complete"
        assert len(strategic_tools) == len(set(strategic_tools))
        assert {"request_replan", "web_search", "sql_execute"} <= set(strategic_tools)
        # No new phase_settings keys.
        assert set(once["phase_settings"]) == {"min_todos", "strategic_tools"}

    def test_the_order_of_the_remaining_core_tools_is_kept(self):
        out = apply_no_phases(
            {"tools": {"core": ["send_message", "todo_list", "job_complete", "x"]}}
        )
        assert out["tools"]["core"] == ["send_message", "job_complete", "x"]


class TestStandingInstructions:
    def test_the_framework_default_is_replaced(self):
        blob = _resolve("general-worker", phases=False)
        expected = (TEMPLATES / "direct_task_instructions.md").read_text()
        assert blob["instructions"]["instructions"] == expected

    def test_the_framework_default_is_replaced_by_default(self):
        blob = _resolve("general-worker")
        expected = (TEMPLATES / "direct_task_instructions.md").read_text()
        assert blob["instructions"]["instructions"] == expected

    def test_an_experts_own_file_is_kept(self):
        blob = _resolve("engineer", phases=False)
        own = (EXPERTS / "engineer" / "instructions.md").read_text()
        assert blob["instructions"]["instructions"] == own

    def test_the_default_on_a_family_with_its_own_variant_is_replaced(self):
        blob = resolve_config(
            base_config_name="worker_base",
            expert_type="worker",
            request_override={
                "llm": {"model": "openai/gpt-oss-120b"},
                "phases": False,
            },
        )
        expected = (TEMPLATES / "direct_task_instructions.md").read_text()
        assert blob["instructions"]["instructions"] == expected

    def test_the_direct_task_instructions_avoid_the_procedure(self):
        text = (TEMPLATES / "direct_task_instructions.md").read_text()
        assert len(text) <= 1500
        lowered = text.lower()
        for word in ("plan", "phase", "todo", "strategic", "tactical"):
            assert word not in lowered, word
        for tool in PHASE_PROCEDURE_TOOLS:
            assert tool not in text, tool


def _direct_task_config(deployment_dir: Path, **extra) -> object:
    data = {
        "agent_id": "tmp-expert",
        "display_name": "Temporary Expert",
        "phases": False,
        "tools": {"core": ["job_complete"]},
        "instruction_files": [
            {
                "skill": DIRECT_TASK_SKILL,
                "trigger": "phase_start:strategic",
                "enforce": False,
            }
        ],
        **extra,
    }
    return load_agent_config_from_dict(data, deployment_dir=str(deployment_dir))


TWO_TODOS = "todos:\n  - id: 1\n    content: First\n  - id: 2\n    content: Second\n"
ONE_TODO = "todos:\n  - id: 1\n    content: The expert's own task todo\n"


class TestExpertOwnDirectTaskFiles:
    def test_a_two_todo_template_is_ignored_for_the_frameworks(self, tmp_path):
        (tmp_path / "direct_task_todos_initial.yaml").write_text(TWO_TODOS)
        (tmp_path / "direct_task_todos_resume.yaml").write_text(TWO_TODOS)
        frozen = serialize_resolved_config(_direct_task_config(tmp_path), model=MODEL)
        initial = (TEMPLATES / "direct_task_todos_initial.yaml").read_text()
        resume = (TEMPLATES / "direct_task_todos_resume.yaml").read_text()
        assert frozen["instructions"]["strategic_todos_initial"] == initial
        assert frozen["instructions"]["strategic_todos_transition"] == initial
        assert frozen["instructions"]["strategic_todos_resume"] == resume

    def test_a_one_todo_template_of_the_expert_wins(self, tmp_path):
        (tmp_path / "direct_task_todos_initial.yaml").write_text(ONE_TODO)
        frozen = serialize_resolved_config(_direct_task_config(tmp_path), model=MODEL)
        assert frozen["instructions"]["strategic_todos_initial"] == ONE_TODO
        assert frozen["instructions"]["strategic_todos_transition"] == ONE_TODO

    def test_an_unparseable_template_is_ignored(self, tmp_path):
        (tmp_path / "direct_task_todos_initial.yaml").write_text(
            "todos: [unterminated\n"
        )
        frozen = serialize_resolved_config(_direct_task_config(tmp_path), model=MODEL)
        assert (
            frozen["instructions"]["strategic_todos_initial"]
            == (TEMPLATES / "direct_task_todos_initial.yaml").read_text()
        )

    def test_own_instructions_and_skill_win(self, tmp_path):
        (tmp_path / "instructions.md").write_text("Own instructions.\n")
        skill = tmp_path / "skills" / DIRECT_TASK_SKILL
        skill.mkdir(parents=True)
        body = "---\nname: direct-task\ndescription: own\n---\n\nOwn method.\n"
        (skill / "SKILL.md").write_text(body)
        frozen = serialize_resolved_config(_direct_task_config(tmp_path), model=MODEL)
        assert frozen["instructions"]["instructions"] == "Own instructions.\n"
        assert frozen["instructions"][DIRECT_TASK_SKILL] == body

    def test_without_own_files_the_frameworks_are_frozen(self, tmp_path):
        frozen = serialize_resolved_config(_direct_task_config(tmp_path), model=MODEL)
        assert (
            frozen["instructions"]["instructions"]
            == (TEMPLATES / "direct_task_instructions.md").read_text()
        )
        assert (
            frozen["instructions"][DIRECT_TASK_SKILL]
            == (
                get_project_root()
                / "config"
                / "skills"
                / DIRECT_TASK_SKILL
                / "SKILL.md"
            ).read_text()
        )


class TestDirectTaskInstructionKeys:
    """``direct_task_todos_initial`` / ``direct_task_todos_resume`` /
    ``direct_task_instructions``: the framework files of a job without
    phases."""

    KEYS = {
        "direct_task_todos_initial": "direct_task_todos_initial.yaml",
        "direct_task_todos_resume": "direct_task_todos_resume.yaml",
        "direct_task_instructions": "direct_task_instructions.md",
    }

    def test_the_keys_have_framework_defaults(self):
        matrix = yaml.safe_load(
            (get_project_root() / "config" / "model_config_matrix.yaml").read_text()
        )
        assert set(DIRECT_TASK_INSTRUCTION_TYPES) == set(self.KEYS)
        for key, filename in self.KEYS.items():
            assert InstructionMatrixResolver.HARDCODED_DEFAULTS[key] == filename
            assert matrix["default"]["instructions"][key] == filename
            assert InstructionMatrixResolver().exists(key)

    def test_the_todo_templates_hold_one_todo_each(self):
        from shared.runtime.core.loader import load_strategic_todos_template

        (initial,) = load_strategic_todos_template("direct_task_todos_initial.yaml")
        assert initial["content"] == (
            "Do the task in the brief. When it is done, call job_complete."
        )
        (resume,) = load_strategic_todos_template("direct_task_todos_resume.yaml")
        assert resume["content"] == (
            "Continue the task in the brief, taking the message above into "
            "account. When the task is done, call job_complete."
        )

    def test_a_config_the_resolver_did_not_rewrite_does_not_freeze_them(self):
        # No direct-task binding (a session, or a blob from before the
        # setting): the setting's default alone freezes nothing.
        from shared.runtime.core.loader import AgentConfig

        config = AgentConfig(agent_id="test", display_name="Test")
        frozen = serialize_resolved_config(config, model="gpt-4o")
        assert not set(self.KEYS) & set(frozen["instructions"])

    def test_a_phased_job_does_not_freeze_them(self, tmp_path):
        frozen = serialize_resolved_config(
            _direct_task_config(tmp_path, phases=True), model=MODEL
        )
        assert not set(self.KEYS) & set(frozen["instructions"])
        resolver = InstructionMatrixResolver(None, frozen["model_family"])
        assert frozen["instructions"]["strategic_todos_initial"] == resolver.load(
            "strategic_todos_initial"
        )

    def test_the_direct_task_skill_is_catalog_hidden(self):
        from shared.runtime.core.skill_format import (
            is_catalog_hidden,
            parse_skill_md,
            skill_identity,
        )

        md = (
            get_project_root() / "config" / "skills" / DIRECT_TASK_SKILL / "SKILL.md"
        ).read_text()
        front, body = parse_skill_md(md)
        name, description = skill_identity(front)
        assert name == DIRECT_TASK_SKILL
        assert "\n" not in description.strip()
        assert is_catalog_hidden(front)
        assert body.lstrip().startswith("# Do the task directly")


class TestRolesAndSubagents:
    @pytest.mark.parametrize("expert", ["assistant", "engineer"])
    @pytest.mark.parametrize("value", [False, True])
    def test_the_session_role_ignores_the_key(self, expert, value):
        # A session keeps every key it inherits (the session overlay prunes
        # nothing); the setting changes nothing else in its blob.
        unset = resolve_config(base_config_name=expert, expert_type="session")
        named = resolve_config(
            base_config_name=expert,
            expert_type="session",
            request_override={"phases": value},
        )
        unset.pop("resolved_at")
        named.pop("resolved_at")
        assert named["agent"].pop("phases") is value
        assert "phases" not in unset["agent"]
        assert named == unset

    def test_the_subagent_role_prunes_the_key(self):
        from shared.runtime.core.loader import load_role_base

        assert "phases" in load_role_base("subagent")["$ignore_keys"]
        # The critic's own file says phases: true; the role prunes it too.
        unset = resolve_config(base_config_name="critic", expert_type="subagent")
        named = resolve_config(
            base_config_name="critic",
            expert_type="subagent",
            request_override={"phases": False},
        )
        unset.pop("resolved_at")
        named.pop("resolved_at")
        assert "phases" not in named["agent"]
        assert "phases" not in unset["agent"]
        assert named == unset

    def test_roster_entries_keep_their_tools(self):
        phased = _resolve("scholar")
        direct = _resolve("scholar", phases=False)
        assert phased["agent"]["phases"] is True
        assert (
            direct["agent"]["subagents"]["roster"]
            == phased["agent"]["subagents"]["roster"]
        )

    def test_apply_is_not_called_for_a_phased_job(self, monkeypatch):
        from orchestrator.services import config_resolver

        def refuse(_data):
            raise AssertionError("a job with phases was rewritten")

        monkeypatch.setattr(config_resolver, "apply_no_phases", refuse)
        _resolve("engineer", phases=True)
        _resolve("developer")
        with pytest.raises(AssertionError):
            _resolve("engineer")
        with pytest.raises(AssertionError):
            _resolve("developer", phases=False)


class TestProcedureSkillsLeaveTheMenu:
    """A job without phases is not offered the phase-procedure skills as
    optional ``use_skill`` entries; a phased job's menu is untouched."""

    @staticmethod
    def _blob(bindings):
        return {
            "agent": {"instruction_files": bindings},
            "skills": {
                "menu": [
                    {"name": "todo-guide"},
                    {"name": "verify-before-done"},
                    {"name": "pdf-report"},
                ],
                "files": {"todo-guide": {}, "verify-before-done": {}, "pdf-report": {}},
            },
        }

    def test_a_blob_without_phases_drops_the_procedure_skills(self):
        from shared.runtime.core.skill_resolution import filter_bound_skills

        blob = filter_bound_skills(
            self._blob([{"skill": "direct-task", "trigger": "phase_start:strategic"}])
        )
        assert [m["name"] for m in blob["skills"]["menu"]] == ["pdf-report"]
        assert sorted(blob["skills"]["files"]) == ["pdf-report"]

    def test_a_phased_blob_keeps_unbound_skills(self):
        from shared.runtime.core.skill_resolution import filter_bound_skills

        blob = filter_bound_skills(
            self._blob(
                [{"skill": "strategic-phase", "trigger": "phase_start:strategic"}]
            )
        )
        assert [m["name"] for m in blob["skills"]["menu"]] == [
            "todo-guide",
            "verify-before-done",
            "pdf-report",
        ]
