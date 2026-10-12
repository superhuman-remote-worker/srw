"""The session input owner must not reach back into the runtime or the loop.

``pyproject.toml`` carries the matching import-linter contract for static
imports. This guard additionally rejects dynamic retrieval (``importlib``,
``__import__``, ``sys.modules``, a module name in a string) and proves in a
fresh interpreter that importing the owner loads no application, runtime,
loop or graph module. It also pins that ``persistent_app`` composes the owner
instead of keeping a second copy of its state or operations.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import agent.api

OWNER_MODULE = "agent.api.session_input"

FORBIDDEN = (
    "agent.api.persistent_app",
    "agent.api.dual_app",
    "agent.api.app",
    "agent.api.turn_executor",
    "agent.persistent_graph",
    "agent.graph",
    "agent.agent",
    "langgraph",
)

# Names persistent_app defined before R3.3a; none may come back as a copy.
RETIRED_RUNTIME_NAMES = (
    "_loop_user_queue",
    "_queued_input_claims",
    "_input_delivery_reclaim_lock",
    "_protected_input_reclaim_task",
    "_loop_interrupt_flag",
    "_loop_interrupt_target_turn_id",
    "_hard_interrupt_event",
    "_awaiting_input",
    "_PINNED_INPUT_POLL_SECONDS",
    "_pinned_input_runtime_identity",
    "_transition_claimed_input",
    "_queue_claimed_input",
    "_reclaim_pending_pinned_inputs",
    "_schedule_protected_input_reclaim",
    "_accept_user_input",
    "_clear_loop_interrupt",
    "_signal_interrupt_for_turn",
    "_loop_admit_input_delivery",
    "_loop_defer_input_delivery",
    "_loop_cancel_input_delivery",
    "_loop_defer_and_requeue_input_delivery",
    "_loop_settle_input_delivery",
    "_wait_for_persistent_input",
    "_loop_get_user_input",
    "_loop_check_interrupt",
)

API_DIR = Path(agent.api.__file__).parent


def _tree(name: str) -> ast.Module:
    return ast.parse((API_DIR / f"{name}.py").read_text())


def test_owner_source_names_no_runtime_loop_or_factory_module():
    imported: list[str] = []
    for node in ast.walk(_tree("session_input")):
        if isinstance(node, ast.Import):
            imported.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            base = node.module or ""
            imported.append(base)
            imported.extend(f"{base}.{alias.name}" for alias in node.names)
        elif isinstance(node, ast.Call):
            func = node.func
            name = getattr(func, "attr", None) or getattr(func, "id", None)
            assert name not in {"import_module", "__import__"}, (
                "session_input imports dynamically"
            )
        elif isinstance(node, ast.Attribute) and node.attr == "modules":
            assert getattr(node.value, "id", None) != "sys", (
                "session_input reads sys.modules"
            )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for forbidden in ("persistent_app", "dual_app", "turn_executor"):
                assert forbidden not in node.value.split(), (
                    f"session_input names {forbidden} in a string"
                )
    for name in imported:
        for forbidden in FORBIDDEN:
            assert name != forbidden and not name.startswith(forbidden + "."), (
                f"session_input imports {name}"
            )


def test_importing_the_owner_loads_no_application_runtime_or_loop():
    code = f"""
import importlib, sys
importlib.import_module({OWNER_MODULE!r})
loaded = sorted(set({FORBIDDEN!r}) & sys.modules.keys())
assert not loaded, loaded
assert "fastapi" not in sys.modules, "session_input imported fastapi"
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_persistent_app_composes_one_owner_and_keeps_no_copy():
    tree = _tree("persistent_app")
    defined: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            defined.update(t.id for t in targets if isinstance(t, ast.Name))
    assert not set(RETIRED_RUNTIME_NAMES) & defined
    constructions = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and getattr(node.func, "id", None) == "SessionInputRuntime"
    ]
    assert len(constructions) == 1
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            assert not set(node.names) & set(RETIRED_RUNTIME_NAMES)


def test_other_agent_modules_reach_input_state_only_through_the_owner():
    for path in sorted(API_DIR.glob("*.py")):
        if path.name in {"session_input.py", "persistent_app.py"}:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.Attribute):
                assert node.attr not in RETIRED_RUNTIME_NAMES, (
                    f"{path.name} reads {node.attr}"
                )
