"""Golden renders of the worker-mode prompt texts.

A job runs in worker mode (the phased procedure) or in agent mode (one task in
one loop, without the phase tools). The 14 worker system-prompt scaffolds and
the engineer and designer instruction files describe the phased procedure only
inside ``{% if has_tool("next_phase_todos") %}`` blocks, so an agent-mode job
is not told about tools it does not have. A job that binds the phase tools must
get exactly the text it got before those blocks existed.

This module renders each of those files through ``render_instruction_content``
(the Jinja step the worker runs on the scaffold in ``_render_worker_prompt``
and on instruction files when it deploys them) with tool lists that contain the
phase tools, and stores the result under
``tests/fixtures/worker_mode_prompt_golden/``:

* ``manifest.json`` holds the tool lists (stored, so a later change to an
  expert's grants cannot move the golden) and, per tool list and file, the
  name of the fixture that holds the expected text;
* one ``<file>__<sha256 prefix>.txt`` per distinct render.

``tests/test_worker_mode_prompts_unchanged.py`` renders again and compares
byte for byte. The fixtures were written from the files as they were before
the agent-mode blocks were added. Regenerate only after a deliberate change to
worker-mode text, and review the fixture diff::

    .venv/bin/python -m tests._worker_mode_prompt_golden
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from shared.job_mode import PHASE_PROCEDURE_TOOLS

REPO = Path(__file__).resolve().parents[1]
GOLDEN_DIR = Path(__file__).parent / "fixtures" / "worker_mode_prompt_golden"
MANIFEST = GOLDEN_DIR / "manifest.json"

#: The tools of the phased procedure; an agent-mode job binds none of them.
PHASE_TOOLS = PHASE_PROCEDURE_TOOLS

#: Repository-relative paths of the rendered files: the 14 worker scaffolds
#: (not the Session or child scaffolds) and two experts' instruction files.
SOURCES = (
    "config/prompts/systemprompt.txt",
    "config/prompts/systemprompt_codex_spark.txt",
    "config/prompts/systemprompt_deepseek.txt",
    "config/prompts/systemprompt_gemini_4_argon.txt",
    "config/prompts/systemprompt_gemma.txt",
    "config/prompts/systemprompt_glm.txt",
    "config/prompts/systemprompt_glm_5_3.txt",
    "config/prompts/systemprompt_gpt_5.txt",
    "config/prompts/systemprompt_gpt_6_1_sol.txt",
    "config/prompts/systemprompt_gpt_oss.txt",
    "config/prompts/systemprompt_kimi_k3.txt",
    "config/prompts/systemprompt_minimax.txt",
    "config/prompts/systemprompt_minimax_m3.txt",
    "config/prompts/systemprompt_muse_spark_1_3.txt",
    "config/experts/engineer/instructions.md",
    "config/experts/designer/instructions.md",
)


def fixture_stem(source: str) -> str:
    """``systemprompt_gemma`` for a scaffold, ``engineer_instructions`` for an expert file."""
    path = Path(source)
    if path.parts[:2] == ("config", "experts"):
        return f"{path.parts[2]}_{path.stem}"
    return path.stem


def render(source: str, tool_names: list[str]) -> str:
    """Render one file the way the worker does, with ``tool_names`` loaded."""
    from shared.runtime.core.loader import render_instruction_content

    content = (REPO / source).read_text(encoding="utf-8")
    return render_instruction_content(content, tool_names, origin=source)


def current_tool_lists() -> dict[str, list[str]]:
    """Worker-mode tool lists: every registry tool, the phase tools alone, and
    each bundled worker expert's grants (every one of them binds the phase tools).

    The first two take every ``has_tool`` condition both ways; the expert lists
    are the combinations real jobs run with.
    """
    from agent.tools.registry import TOOL_REGISTRY
    from shared.runtime.core.loader import chain_root, load_and_merge_config

    lists = {
        "all_tools": sorted(set(TOOL_REGISTRY) | set(PHASE_TOOLS)),
        "phase_tools_only": sorted({*PHASE_TOOLS, "job_complete"}),
    }
    for leaf in sorted((REPO / "config").glob("experts/*/config.yaml")):
        if chain_root(str(leaf)) != "worker_base":
            continue
        tools = (load_and_merge_config(str(leaf)) or {}).get("tools") or {}
        lists[f"expert_{leaf.parent.name}"] = sorted(
            {
                name
                for group in tools.values()
                if isinstance(group, list)
                for name in group
                if isinstance(name, str) and name in TOOL_REGISTRY
            }
        )
    return lists


def load_manifest() -> dict:
    return json.loads(MANIFEST.read_text(encoding="utf-8"))


def read_fixture(name: str) -> str:
    # Bytes, decoded: no newline translation between the file and the compare.
    return (GOLDEN_DIR / name).read_bytes().decode("utf-8")


def write_golden() -> None:
    """(Re)write the manifest and one fixture per distinct render."""
    tool_lists = current_tool_lists()
    GOLDEN_DIR.mkdir(parents=True, exist_ok=True)
    for old in GOLDEN_DIR.glob("*.txt"):
        old.unlink()
    cases: dict[str, dict[str, str]] = {}
    for profile, tools in tool_lists.items():
        missing = sorted(set(PHASE_TOOLS) - set(tools))
        # The developer expert binds no todo_list; next_phase_todos is the gate.
        assert "next_phase_todos" in tools, (profile, missing)
        cases[profile] = {}
        for source in SOURCES:
            text = render(source, tools)
            digest = hashlib.sha256(text.encode("utf-8")).hexdigest()[:12]
            name = f"{fixture_stem(source)}__{digest}.txt"
            with open(GOLDEN_DIR / name, "w", encoding="utf-8", newline="") as fh:
                fh.write(text)
            cases[profile][source] = name
    manifest = {"tool_lists": tool_lists, "cases": cases}
    MANIFEST.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    write_golden()
    print(f"wrote {MANIFEST.relative_to(REPO)}")
