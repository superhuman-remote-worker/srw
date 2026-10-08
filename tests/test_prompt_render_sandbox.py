"""Security audit 2026-08-27, finding #2: prompt rendering is sandboxed and the
DB-prompt fence cannot be switched off by a caller-supplied override.

Two halves of one chain. (1) ``render_instruction_content`` ran prompt text
through a plain ``jinja2.Environment``; DB-authored expert prompts reach it, so
``{{ ''.__class__.__mro__[1].__subclasses__() }}`` was code execution inside
the agent process. (2) The only thing keeping a DB phase prompt away from that
renderer was the ``_db_prompt_keys`` marker in ``config.extra`` — which a
job/thread ``config_override`` (or a roster entry, or the expert's own config
fragment) could clear through the same deep-merge every authored layer rides.

The fix: every render goes through ``jinja2.sandbox.ImmutableSandboxedEnvironment``
and fails closed with ``PromptRenderSecurityError``; every caller-authored layer
passes ``strip_loader_owned_keys`` before it merges, and the DB-loading path
writes the markers LAST. Bundled prompts must render byte-identically.
"""

import dataclasses
import re
from pathlib import Path

import jinja2
import pytest

from orchestrator.services.config_resolver import resolve_config
from shared.runtime.core.loader import (
    LOADER_OWNED_KEY_PREFIX,
    PromptRenderSecurityError,
    _has_shell_tools,
    deep_merge,
    get_phase_system_prompt,
    get_system_prompt,
    load_agent_config_from_dict,
    load_config_from_resolved,
    render_instruction_content,
    strip_loader_owned_keys,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]

# The three shapes from the audit brief: attribute traversal into the type
# hierarchy, the template's own ``self`` reference into module globals, and the
# same traversal inside a loop (no output, so an unsandboxed render "succeeds").
_ESCAPE_PAYLOADS = [
    "{{ ''.__class__.__mro__[1].__subclasses__() }}",
    "{{ self.__init__.__globals__ }}",
    "{% for c in ().__class__.__base__.__subclasses__() %}{% endfor %}",
]

_BASE = "BASE {agent_display_name} ID:{expert_identity} C:{prompt_content}"


def _config(_persona_source=None, _db_prompt_keys=None, **resolved_prompts):
    data = {
        "agent_id": "t",
        "display_name": "T",
        "_resolved_prompts": resolved_prompts,
    }
    if _persona_source is not None:
        data["_persona_source"] = _persona_source
    if _db_prompt_keys is not None:
        data["_db_prompt_keys"] = _db_prompt_keys
    return load_agent_config_from_dict(data)


# ── (a) the render is sandboxed and fails closed ─────────────────────────────


@pytest.mark.parametrize("payload", _ESCAPE_PAYLOADS)
def test_sandbox_escape_payload_is_refused(payload):
    """A template reaching for Python internals raises; nothing is rendered."""
    with pytest.raises(PromptRenderSecurityError) as excinfo:
        render_instruction_content(payload, ["kb_write"], origin="unit")
    assert "unit" in str(excinfo.value)
    assert "<class" not in str(excinfo.value)


@pytest.mark.parametrize("payload", _ESCAPE_PAYLOADS)
def test_unfenced_db_phase_prompt_is_refused_by_the_worker_render(payload):
    """The bypass shape: a DB tactical prompt whose ``_db_prompt_keys`` marker
    was dropped reaches the Jinja render unfenced. It must be refused, not
    executed — and the refusal names the expert and the prompt key."""
    config = _config(systemprompt=_BASE, tactical=payload)
    with pytest.raises(PromptRenderSecurityError) as excinfo:
        get_phase_system_prompt(config, is_strategic=False, tool_names=["kb_write"])
    assert "'tactical'" in str(excinfo.value)
    assert "'t'" in str(excinfo.value)


@pytest.mark.parametrize("payload", _ESCAPE_PAYLOADS)
def test_unfenced_db_base_prompt_is_refused_by_the_phase_agnostic_render(payload):
    """Same for the phase-agnostic worker prompt (a smuggled ``systemprompt``)."""
    config = _config(systemprompt=payload)
    with pytest.raises(PromptRenderSecurityError):
        get_system_prompt(config, tool_names=["kb_write"])


@pytest.mark.parametrize("payload", _ESCAPE_PAYLOADS)
def test_fenced_db_phase_prompt_never_reaches_the_renderer(payload):
    """With the marker intact the fence strips the braces first, so the
    payload is inert prose inside ``<expert_workflow>`` — no raise, no class
    list, no traversal."""
    config = _config(_db_prompt_keys=["tactical"], systemprompt=_BASE, tactical=payload)
    out = get_phase_system_prompt(config, is_strategic=False, tool_names=["kb_write"])
    assert "<expert_workflow" in out
    assert "<class" not in out
    assert "{{" not in out and "{%" not in out


@pytest.mark.parametrize("tree", ["src", "orchestrator"])
def test_render_has_no_unsandboxed_environment_left_under_src(tree):
    """Every ``jinja2`` import under ``src/`` **and** ``orchestrator/`` must come
    from ``jinja2.sandbox``. A plain ``Environment`` / ``Template`` anywhere in
    either tree would be a second, unsandboxed render path — and the
    orchestrator is where prompt text is resolved and frozen, so the
    single-render-site guarantee has to be enforced on both sides of the
    dispatch, not only in the agent."""
    offenders = []
    pattern = re.compile(r"^\s*(from|import)\s+jinja2\b")
    for path in (_REPO_ROOT / tree).rglob("*.py"):
        for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if pattern.match(line) and "jinja2.sandbox" not in line:
                offenders.append(
                    f"{path.relative_to(_REPO_ROOT)}:{lineno}: {line.strip()}"
                )
    assert not offenders, "\n".join(offenders)


# ── (c) bundled prompts render byte-identically in the sandbox ───────────────

_BUNDLED = [
    "config/prompts/systemprompt.txt",
    "config/prompts/systemprompt_interactive.txt",
    "config/experts/developer/skills/tactical-phase/SKILL.md",
    "config/templates/strategic_todos_resume.yaml",
]


def _reference_render(content, tools, **kw):
    """What the pre-fix plain ``jinja2.Environment`` produced, same context."""
    env = jinja2.Environment(keep_trailing_newline=True)
    tool_set = set(tools)
    context = dict(kw.get("extra_context") or {})
    context.update(
        tools=tools,
        has_tool=lambda name: name in tool_set,
        has_shell=_has_shell_tools(tool_set),
        protected_cloud=kw.get("protected_cloud", False),
    )
    return env.from_string(content).render(**context)


@pytest.mark.parametrize("rel", _BUNDLED)
@pytest.mark.parametrize("tools_on", [True, False])
def test_bundled_prompt_renders_byte_identically_in_the_sandbox(rel, tools_on):
    path = _REPO_ROOT / rel
    content = path.read_text(encoding="utf-8")
    assert "{%" in content, f"{rel} carries no Jinja markers — pick a templated file"
    tools = (
        re.findall(r'has_tool\(\s*["\']([^"\']+)["\']\s*\)', content) + ["run_command"]
        if tools_on
        else []
    )
    for kw in (
        {},
        {"protected_cloud": True},
        {"extra_context": {"caller_flag": True}},
        {"extra_context": {"caller_flag": False}},
    ):
        assert render_instruction_content(content, tools, **kw) == _reference_render(
            content, tools, **kw
        )


# ── (b) the fence marker is loader-owned: no override can set or clear it ────


def test_strip_loader_owned_keys_contract():
    override = {
        "llm": {"model": "m", "_not_stripped_below_top_level": 1},
        "_db_prompt_keys": [],
        "_persona_source": None,
        "_resolved_prompts": {"systemprompt": "pwned"},
        "extra": {
            "shell": {"mode": "persistent"},
            "_db_prompt_keys": [],
            "extra": {"_persona_source": "disk", "keep": 1},
        },
    }
    snapshot = repr(override)
    cleaned = strip_loader_owned_keys(override)
    assert cleaned == {
        "llm": {"model": "m", "_not_stripped_below_top_level": 1},
        "extra": {"shell": {"mode": "persistent"}, "extra": {"keep": 1}},
    }
    assert repr(override) == snapshot, "input must not be mutated"
    assert strip_loader_owned_keys(None) is None
    assert strip_loader_owned_keys([1]) == [1]
    assert LOADER_OWNED_KEY_PREFIX == "_"


_ROW = {
    "expert_type": "session",
    "name": "sess-helper",
    "config": {},
    "prompts": {"persona": "PERSONA-SENTINEL", "tactical": "TAC-SENTINEL"},
}

_UNFENCE_FORMS = [
    {"_db_prompt_keys": []},
    {"_db_prompt_keys": None},
    {"_db_prompt_keys": ["persona"]},
    {"_persona_source": None},
    {"_persona_source": "disk"},
    {"extra": {"_db_prompt_keys": [], "_persona_source": None}},
    {"extra": {"extra": {"_db_prompt_keys": []}}},
    {"_resolved_prompts": {"tactical": "{{ ''.__class__ }}"}},
]


@pytest.mark.parametrize("form", _UNFENCE_FORMS, ids=lambda f: str(f))
@pytest.mark.parametrize(
    "layer",
    ["request_override", "project_overrides", "user_settings", "db_overrides"],
)
def test_no_authored_layer_can_change_which_db_prompts_are_fenced(layer, form):
    blob = resolve_config(
        base_config_name="persistent_defaults",
        expert_row=_ROW,
        expert_type="session",
        **{layer: {"llm": {"temperature": 0.3}, **form}},
    )
    agent = blob["agent"]
    assert set(agent["_db_prompt_keys"]) == {"persona", "tactical"}
    assert agent["_persona_source"] == "db"
    assert blob["prompts"]["tactical"] == "TAC-SENTINEL"
    assert agent["llm"]["temperature"] == 0.3  # the honest part of the layer merged
    nested = agent.get("extra") or {}
    assert not any(k.startswith("_") for k in nested)


@pytest.mark.parametrize("form", _UNFENCE_FORMS[:5], ids=lambda f: str(f))
def test_expert_rows_own_config_fragment_cannot_unmark_itself(form):
    row = {**_ROW, "config": {"llm": {"temperature": 0.7}, **form}}
    blob = resolve_config(
        base_config_name="persistent_defaults", expert_row=row, expert_type="session"
    )
    assert set(blob["agent"]["_db_prompt_keys"]) == {"persona", "tactical"}
    assert blob["agent"]["_persona_source"] == "db"


def test_base_defaults_layer_is_stripped_too():
    blob = resolve_config(
        base_config_name="persistent_defaults",
        base_defaults={"llm": {"model": "floor-model"}, "_db_prompt_keys": ["x"]},
        expert_row=_ROW,
        expert_type="session",
    )
    assert set(blob["agent"]["_db_prompt_keys"]) == {"persona", "tactical"}


def test_a_bundled_resolve_still_carries_no_marker():
    """No expert row → the DB-loading path never ran → no marker, even when an
    override tries to plant one (which would fence a trusted bundled prompt)."""
    blob = resolve_config(
        base_config_name="persistent_defaults",
        request_override={"_db_prompt_keys": ["tactical"], "_persona_source": "db"},
    )
    assert "_db_prompt_keys" not in blob["agent"]
    assert "_persona_source" not in blob["agent"]


def test_orchestrator_override_cannot_unfence_the_rendered_session_prompt():
    """End to end: resolve with the attack override → hydrate as the agent does
    → the DB persona still renders inside ``<user_persona>``."""
    blob = resolve_config(
        base_config_name="persistent_defaults",
        expert_row=_ROW,
        expert_type="session",
        request_override={"extra": {"_persona_source": None, "_db_prompt_keys": []}},
    )
    config = load_config_from_resolved(blob)
    out = get_phase_system_prompt(
        config, is_strategic=False, prompt_type="interactive", tool_names=[]
    )
    assert "<user_persona" in out
    assert "PERSONA-SENTINEL" in out


def _hydrated_session_config():
    blob = resolve_config(
        base_config_name="persistent_defaults", expert_row=_ROW, expert_type="session"
    )
    return load_config_from_resolved(blob)


@pytest.mark.parametrize("form", _UNFENCE_FORMS, ids=lambda f: str(f))
def test_agent_side_override_merge_cannot_unfence(form):
    """The agent-side seams (job ``config_override``, session attach, live
    ``config.update``) all do asdict → strip → deep_merge → reload; the strip
    is what keeps the markers."""
    hydrated = _hydrated_session_config()
    base = dataclasses.asdict(hydrated)

    merged = deep_merge(base, strip_loader_owned_keys(form))
    rebuilt = load_agent_config_from_dict(
        merged, deployment_dir=hydrated._deployment_dir
    )
    assert set(rebuilt.extra["_db_prompt_keys"]) == {"persona", "tactical"}
    assert rebuilt.extra["_persona_source"] == "db"
    out = get_phase_system_prompt(
        rebuilt, is_strategic=False, prompt_type="interactive", tool_names=[]
    )
    assert "<user_persona" in out


# Agent-side the markers sit under ``extra`` (``dataclasses.asdict`` shape), so
# these are the override shapes that reach them through a bare deep_merge: a
# top-level unknown key wins over the nested extra in
# ``load_agent_config_from_dict``, and ``extra.*`` merges straight in.
_AGENT_SIDE_BYPASS_FORMS = [
    {"_db_prompt_keys": []},
    {"_db_prompt_keys": ["persona"]},
    {"_persona_source": "disk"},
    {"extra": {"_db_prompt_keys": [], "_persona_source": None}},
]


@pytest.mark.parametrize("form", _AGENT_SIDE_BYPASS_FORMS, ids=lambda f: str(f))
def test_control_bare_agent_side_merge_is_the_bypass(form):
    """Documents why the strip is load-bearing: the same merge WITHOUT it moves
    a marker (this is the audit's bypass, reproduced on purpose)."""
    hydrated = _hydrated_session_config()
    bare = load_agent_config_from_dict(
        deep_merge(dataclasses.asdict(hydrated), form),
        deployment_dir=hydrated._deployment_dir,
    )
    assert not (
        set(bare.extra.get("_db_prompt_keys") or ()) == {"persona", "tactical"}
        and bare.extra.get("_persona_source") == "db"
    )


def test_live_session_sanitizer_drops_loader_owned_keys():
    from agent.api.persistent_app import _sanitize_live_session_config_override

    sanitized = _sanitize_live_session_config_override(
        {
            "llm": {"model": "m"},
            "_db_prompt_keys": [],
            "extra": {"_persona_source": None, "shell": {"mode": "persistent"}},
        }
    )
    assert sanitized == {
        "llm": {"model": "m"},
        "extra": {"shell": {"mode": "persistent"}},
    }


def test_roster_entry_cannot_unmark_a_db_child():
    """A roster entry's sibling keys merge on top of the DB target AFTER the
    roster marks it; they are authored and must not carry the markers."""
    from shared.runtime.core.subagent_roster import resolve_subagent_roster

    child_id = "11111111-2222-4333-8444-555555555555"
    parent = {
        "agent_id": "parent",
        "display_name": "Parent",
        "llm": {"model": "parent-model"},
        "subagents": {
            "roster": {
                "helper": {
                    "$ref": child_id,
                    "_db_prompt_keys": [],
                    "_persona_source": None,
                    "extra": {"_db_prompt_keys": []},
                }
            }
        },
    }
    rows = {
        child_id: {
            "id": child_id,
            "name": "helper-expert",
            "expert_type": "subagent",
            "config": {"_db_prompt_keys": []},
            "prompts": {"persona": "CHILD-PERSONA"},
        }
    }
    resolved = resolve_subagent_roster(parent, db_refs=rows)
    entry = resolved["subagents"]["roster"]["helper"]
    assert entry["_persona_source"] == "db"
    assert entry["_db_prompt_keys"] == ["persona"]
    assert entry["prompts"]["persona"] == "CHILD-PERSONA"


def test_every_override_seam_calls_the_strip():
    """Source contract: the strip is wired at each caller-authored merge seam.
    (The functional tests above prove the helper; this pins where it runs.)"""
    expected = {
        "src/agent/agent.py": 1,
        "src/agent/api/persistent_app.py": 2,
        "src/agent/api/session_attach.py": 1,
        "src/shared/runtime/core/expert_resolution.py": 1,
        "src/shared/runtime/core/subagent_roster.py": 1,
        "src/orchestrator/services/config_resolver.py": 2,
    }
    for rel, minimum in expected.items():
        src = (_REPO_ROOT / rel).read_text(encoding="utf-8")
        assert src.count("strip_loader_owned_keys(") >= minimum, rel
