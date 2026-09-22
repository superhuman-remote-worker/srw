"""The use_skill agent tool (Agent Skills, Slice 2).

Loads skills/<name>/SKILL.md (L2) from the workspace. Exercised against the
filesystem test backend, mirroring how other workspace-tool tests build a
WorkspaceManager + ToolContext.
"""

from unittest.mock import patch

from tests._fs_backend import FilesystemTestBackend
from agent.core.workspace import WorkspaceManager
from agent.tools.context import ToolContext
from agent.tools.workspace.skills import create_skill_tools


def _use_skill(tmp_path, *, allowed=("hello-skill", "nope"), menu=None):
    ws = WorkspaceManager(
        job_id="t", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ctx = ToolContext(
        workspace_manager=ws,
        config={
            "_resolved_skills": {
                "menu": (
                    menu if menu is not None else [{"name": name} for name in allowed]
                ),
                "files": {},
            }
        },
    )
    tools = {t.name: t for t in create_skill_tools(ctx)}
    return ws, tools["use_skill"]


def test_use_skill_returns_body(tmp_path):
    ws, use_skill = _use_skill(tmp_path)
    ws.backend.mkdir("skills/hello-skill")
    ws.write_file(
        "skills/hello-skill/SKILL.md", "---\nname: hello-skill\n---\nBODY-HERE"
    )
    out = use_skill.invoke({"skill_name": "hello-skill"})
    assert "BODY-HERE" in out
    assert "hello-skill" in out


def test_use_skill_records_the_exact_instruction_version(tmp_path):
    ws, use_skill = _use_skill(tmp_path)
    body = "---\nname: hello-skill\n---\nVERSIONED-BODY"
    ws.backend.mkdir("skills/hello-skill")
    ws.write_file("skills/hello-skill/SKILL.md", body)

    with patch.object(ToolContext, "record_file_read", autospec=True) as record:
        out = use_skill.invoke({"skill_name": "hello-skill"})

    assert "VERSIONED-BODY" in out
    assert record.call_args.args[1:] == ("skills/hello-skill/SKILL.md", body)


def test_use_skill_missing_is_friendly(tmp_path):
    _ws, use_skill = _use_skill(tmp_path)
    out = use_skill.invoke({"skill_name": "nope"})
    assert "not found" in out.lower()


def test_use_skill_metadata_registered():
    from agent.tools.workspace.skills import SKILL_TOOLS_METADATA

    assert "use_skill" in SKILL_TOOLS_METADATA
    assert SKILL_TOOLS_METADATA["use_skill"]["category"] == "workspace"


# --- Slice 4: script-availability note on shell-less tiers ---


class _ShellFsBackend(FilesystemTestBackend):
    """A test backend that reports a shell (mirrors RemoteBackend/sandbox)."""

    @property
    def supports_shell(self) -> bool:
        return True


def _use_skill_on(backend, tmp_path):
    ws = WorkspaceManager(job_id="t", base_path=tmp_path, backend=backend)
    ctx = ToolContext(
        workspace_manager=ws,
        config={
            "_resolved_skills": {
                "menu": [{"name": "word-count"}, {"name": "hello-skill"}],
                "files": {},
            }
        },
    )
    tools = {t.name: t for t in create_skill_tools(ctx)}
    return ws, tools["use_skill"]


def _make_script_skill(ws):
    ws.backend.mkdir("skills/word-count/scripts")
    ws.write_file(
        "skills/word-count/SKILL.md",
        "---\nname: word-count\n---\nRun skills/word-count/scripts/wordcount.py",
    )
    ws.write_file("skills/word-count/scripts/wordcount.py", "print('x')")


def test_script_skill_on_lite_tier_appends_upgrade_note(tmp_path):
    # FilesystemTestBackend.supports_shell == False (the virtual-tier case)
    ws, use_skill = _use_skill_on(FilesystemTestBackend(tmp_path), tmp_path)
    _make_script_skill(ws)
    out = use_skill.invoke({"skill_name": "word-count"})
    assert "Run skills/word-count/scripts/wordcount.py" in out  # body still delivered
    assert "request_workspace_upgrade" in out  # the affordance is named
    assert "cannot be executed" in out.lower()


def test_script_skill_with_shell_has_no_note(tmp_path):
    ws, use_skill = _use_skill_on(_ShellFsBackend(tmp_path), tmp_path)
    _make_script_skill(ws)
    out = use_skill.invoke({"skill_name": "word-count"})
    assert "wordcount.py" in out  # body still delivered
    assert "request_workspace_upgrade" not in out  # shell present → no nag


def test_prompt_only_skill_on_lite_tier_has_no_note(tmp_path):
    ws, use_skill = _use_skill_on(FilesystemTestBackend(tmp_path), tmp_path)
    ws.backend.mkdir("skills/hello-skill")
    ws.write_file(
        "skills/hello-skill/SKILL.md", "---\nname: hello-skill\n---\nJust guidance."
    )
    out = use_skill.invoke({"skill_name": "hello-skill"})
    assert "Just guidance." in out
    assert "request_workspace_upgrade" not in out  # no scripts/ → no note


def test_stale_skill_bytes_are_inert_when_name_leaves_scoped_menu(tmp_path):
    ws, use_skill = _use_skill(tmp_path, allowed=())
    ws.backend.mkdir("skills/present-with-canvas")
    ws.write_file(
        "skills/present-with-canvas/SKILL.md",
        "---\nname: present-with-canvas\n---\nSTALE-CANVAS-GUIDANCE",
    )

    out = use_skill.invoke({"skill_name": "present-with-canvas"})

    assert "not available" in out.lower()
    assert "STALE-CANVAS-GUIDANCE" not in out


def test_use_skill_refuses_managed_app_guide_workspace_bytes(tmp_path):
    ws, use_skill = _use_skill(
        tmp_path,
        menu=[
            {
                "name": "app-guide",
                "system_managed": True,
                "loader_tool": "read_product_guide",
            }
        ],
    )
    ws.backend.mkdir("skills/app-guide")
    ws.write_file(
        "skills/app-guide/SKILL.md",
        "---\nname: app-guide\n---\nSTALE-OR-USER-CONTROLLED-GUIDANCE",
    )

    out = use_skill.invoke({"skill_name": "app-guide"})

    assert "read_product_guide" in out
    assert "managed by the running SRW product" in out
    assert "STALE-OR-USER-CONTROLLED-GUIDANCE" not in out


def _use_skill_with_binding(
    tmp_path, *, menu_names=(), bound_skill="verify-before-done"
):
    """A use_skill bound to a workspace whose menu omits a bound skill.

    Mirrors the orchestrator's filter_bound_skills: the bound skill rides the
    frozen instructions channel (skills/<name>/SKILL.md on disk + an
    instruction_files binding), not the optional catalog menu.
    """
    from shared.runtime.core.loader import InstructionFileEntry

    ws = WorkspaceManager(
        job_id="t", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ctx = ToolContext(
        workspace_manager=ws,
        config={
            "_resolved_skills": {
                "menu": [{"name": name} for name in menu_names],
                "files": {},
            }
        },
    )
    ctx._instruction_files = [
        InstructionFileEntry(
            trigger="before_tool:todo_complete",
            skill=bound_skill,
            enforce=True,
        )
    ]
    ctx._llm_config = None
    tools = {t.name: t for t in create_skill_tools(ctx)}
    return ws, ctx, tools["use_skill"]


def test_use_skill_loads_bound_skill_via_authorized_binding(tmp_path):
    """A gate-required bound skill is loadable through its advertised route.

    Regression for worker_verification_skill_tool_contract_mismatch: the menu
    refused verify-before-done (filtered by design) while the gate required
    it, and only read_file satisfied the prerequisite. use_skill must also
    serve currently bound skills; absence from the optional menu alone is not
    a refusal. Records the same versioned read so the gate is satisfied.
    """
    import hashlib
    from unittest.mock import patch

    ws, ctx, use_skill = _use_skill_with_binding(tmp_path, menu_names=("free-skill",))
    body = "---\nname: verify-before-done\n---\nBOUND-BODY-v1"
    ws.backend.mkdir("skills/verify-before-done")
    ws.write_file("skills/verify-before-done/SKILL.md", body)

    real_record = ToolContext.record_file_read
    with patch.object(
        ToolContext, "record_file_read", autospec=True, side_effect=real_record
    ) as record:
        out = use_skill.invoke({"skill_name": "verify-before-done"})

    assert "BOUND-BODY-v1" in out
    assert "not available" not in out.lower()
    assert record.call_args.args[1:] == ("skills/verify-before-done/SKILL.md", body)
    assert ctx.check_tool_enforcement("todo_complete") is None
    expected_version = "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest()
    assert (
        ctx._instruction_read_stamps["skills/verify-before-done/SKILL.md"][
            "content_version"
        ]
        == expected_version
    )


def test_use_skill_still_refuses_unbound_workspace_skill(tmp_path):
    """The bound-skill route must not become an arbitrary workspace reader.

    A skill file present on disk without a menu entry AND without an
    instruction binding stays refused; workspace bytes alone grant nothing.
    """
    ws, _ctx, use_skill = _use_skill_with_binding(
        tmp_path, menu_names=("free-skill",), bound_skill="verify-before-done"
    )
    ws.backend.mkdir("skills/attacker-skill")
    ws.write_file(
        "skills/attacker-skill/SKILL.md",
        "---\nname: attacker-skill\n---\nATTACKER-BYTES",
    )

    out = use_skill.invoke({"skill_name": "attacker-skill"})

    assert "not available" in out.lower()
    assert "ATTACKER-BYTES" not in out


def _use_skill_phase_scoped(tmp_path):
    from shared.runtime.core.loader import InstructionFileEntry

    ws = WorkspaceManager(
        job_id="t", base_path=tmp_path, backend=FilesystemTestBackend(tmp_path)
    )
    ctx = ToolContext(
        workspace_manager=ws,
        config={"_resolved_skills": {"menu": [], "files": {}}},
    )
    ctx._instruction_files = [
        InstructionFileEntry(
            trigger="before_tool:todo_complete",
            skill="verify-before-done",
            enforce=True,
            phases=["tactical"],
            read_scope="phase",
            max_read_age_turns=20,
        )
    ]
    ctx._llm_config = None
    tools = {t.name: t for t in create_skill_tools(ctx)}
    return ws, ctx, tools["use_skill"]


def test_bound_use_skill_read_expires_on_phase_change(tmp_path):
    """A bound use_skill read must not permanently satisfy the gate.

    Phase/freshness invalidation is intentional: moving to a new concrete
    phase legitimately re-arms the prerequisite, and one correct new read
    (via either advertised route) allows progress again.
    """
    ws, ctx, use_skill = _use_skill_phase_scoped(tmp_path)
    body = "---\nname: verify-before-done\n---\nPHASED-BODY"
    ws.backend.mkdir("skills/verify-before-done")
    ws.write_file("skills/verify-before-done/SKILL.md", body)

    ctx.set_current_phase("tactical", phase_number=2, turn_count=5)
    out = use_skill.invoke({"skill_name": "verify-before-done"})
    assert "PHASED-BODY" in out
    assert ctx.check_tool_enforcement("todo_complete") is None

    ctx.set_current_phase("tactical", phase_number=4, turn_count=9)
    assert ctx.check_tool_enforcement("todo_complete") is not None

    out = use_skill.invoke({"skill_name": "verify-before-done"})
    assert "PHASED-BODY" in out
    assert ctx.check_tool_enforcement("todo_complete") is None
