"""The attach and identity owners must not reach back into the runtime.

``pyproject.toml`` carries the matching import-linter contract for static
imports. This guard additionally rejects dynamic retrieval (``importlib``,
``__import__``, ``sys.modules``, a module name in a string) and proves in a
fresh interpreter that importing each owner loads no application factory,
runtime, loop or graph module. It also pins that ``persistent_app`` composes
exactly one owner of each and keeps no copy of their state or operations,
and that no other agent module reaches the retired names.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

import agent.api

OWNER_MODULES = (
    "agent.api.session_identity",
    "agent.api.session_workspace",
    "agent.api.session_attach",
)

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

# Names persistent_app defined before R3.3b; none may come back as a copy.
RETIRED_IDENTITY_NAMES = (
    "_thread_id",
    "_session_runtime_generation",
    "_session_runtime_attach_token",
    "_pinned_runtime_generation_enabled",
    "_pinned_status_identity_enabled",
    "_input_runtime_generation",
    "_session_generation",
    "_canonical_runtime_generation",
    "_pinned_status_identity_advertised",
    "_pinned_runtime_generation_advertised",
    "_adopt_attached_runtime_identity",
    "_clear_attached_runtime_identity",
    "_bind_attached_runtime_payload",
    "_current_input_runtime_identity",
    "_current_pinned_session_identity_fingerprint",
    "_attached_retirement_identity",
    "_current_stateless_lease_token",
    "_session_subagent_parent_authority",
)
RETIRED_ATTACH_NAMES = (
    "_pool_attach_lock",
    "_pool_attach_claim",
    "_pool_attach_runtime_generation",
    "_pool_attach_token",
    "_pool_attach_task",
    "_failed_attach_release_receipt",
    "_failed_attach_workspace_cleanup_context",
    "_retain_failed_attach_release_receipt",
    "_pool_heartbeat_status",
    "_subagent_batch_settle_advertised",
    "_subagent_fanout_advertised",
    "_session_subagent_advertisement",
    "_apply_session_subagent_advertisement",
    "_canonical_attach_workspace_identity",
    "_assert_attach_workspace_tier",
    "_assert_attach_workspace_payload",
    "_ATTACH_WORKSPACE_IDENTITY_UNSET",
    "_PROTECTED_CLOUD_SAFE_ERROR_CODES",
    "_protected_workspace_marker",
    "_validate_protected_cloud_mount",
    "_protected_workspace_delivery",
    "_ProtectedWorkspaceIdentity",
    "_protected_workspace_identity",
    "_strict_cleanup_partial_attach_local_resources",
    "_strict_cleanup_partial_sandbox_workspace",
    "_cleanup_failed_event_journal_attach",
    "_cleanup_failed_attach_until_proven",
    "_release_failed_attach_receipt_until_confirmed",
    "_release_delivered_attach_before_dedicated_exit",
    "MEMORY_EMBEDDING_ENV_KEYS",
    "_apply_session_embedding_env",
    "_attach_session_inner",
    "_attach_session",
    "_runtime_actor_context_for_attach",
    "_clear_attached_runtime_actor",
    "_run_pool_attach_transaction",
    "_admit_pool_session_attach",
    "_session_backend_is_lite",
    "_session_backend_is_vm",
    "_apply_datasource_enrichment_to_resolved",
    "_poll_workspace_ready",
)
RETIRED = RETIRED_IDENTITY_NAMES + RETIRED_ATTACH_NAMES

API_DIR = Path(agent.api.__file__).parent


def _tree(name: str) -> ast.Module:
    return ast.parse((API_DIR / f"{name}.py").read_text())


@pytest.mark.parametrize("module", OWNER_MODULES)
def test_owner_source_names_no_runtime_loop_or_factory_module(module):
    short = module.rsplit(".", 1)[1]
    imported: list[str] = []
    for node in ast.walk(_tree(short)):
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
                f"{short} imports dynamically"
            )
        elif isinstance(node, ast.Attribute) and node.attr == "modules":
            assert getattr(node.value, "id", None) != "sys", (
                f"{short} reads sys.modules"
            )
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            for forbidden in ("persistent_app", "dual_app", "turn_executor"):
                assert forbidden not in node.value.split(), (
                    f"{short} names {forbidden} in a string"
                )
    for name in imported:
        for forbidden in FORBIDDEN:
            assert name != forbidden and not name.startswith(forbidden + "."), (
                f"{short} imports {name}"
            )


@pytest.mark.parametrize("module", OWNER_MODULES)
def test_importing_the_owner_loads_no_application_runtime_or_loop(module):
    code = f"""
import importlib, sys
importlib.import_module({module!r})
loaded = sorted(set({FORBIDDEN!r}) & sys.modules.keys())
assert not loaded, loaded
assert "fastapi" not in sys.modules, "{module} imported fastapi"
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


def test_persistent_app_composes_one_of_each_owner_and_keeps_no_copy():
    tree = _tree("persistent_app")
    defined: set[str] = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            defined.add(node.name)
        elif isinstance(node, (ast.Assign, ast.AnnAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            defined.update(t.id for t in targets if isinstance(t, ast.Name))
    assert not set(RETIRED) & defined, sorted(set(RETIRED) & defined)
    for owner in ("SessionIdentityRuntime", "SessionAttachCoordinator"):
        constructions = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and getattr(node.func, "id", None) == owner
        ]
        assert len(constructions) == 1, owner
    for node in ast.walk(tree):
        if isinstance(node, ast.Global):
            assert not set(node.names) & set(RETIRED), node.names
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            assert node.id not in RETIRED, f"persistent_app reads {node.id}"


def test_other_agent_modules_reach_identity_and_attach_only_through_owners():
    for path in sorted(API_DIR.glob("*.py")):
        if path.name in {
            "persistent_app.py",
            "session_attach.py",
            "session_identity.py",
            "session_workspace.py",
        }:
            continue
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute):
                assert node.attr not in RETIRED_ATTACH_NAMES, (
                    f"{path.name} reads {node.attr}"
                )
                if isinstance(node.value, ast.Name) and node.value.id == "pa":
                    assert node.attr not in RETIRED_IDENTITY_NAMES, (
                        f"{path.name} reads pa.{node.attr}"
                    )
            elif isinstance(node, ast.Constant) and isinstance(node.value, str):
                # No ``getattr(pa, "_thread_id")``-style retrieval of the state.
                assert node.value not in RETIRED_IDENTITY_NAMES[:7], (
                    f"{path.name} names {node.value!r}"
                )
