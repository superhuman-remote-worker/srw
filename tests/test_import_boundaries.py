"""Exercise the checked-in architecture contracts with allowed and poisoned imports."""

import os
from pathlib import Path
import subprocess
import sys
import tomllib

import pytest


REPO = Path(__file__).resolve().parents[1]


@pytest.fixture
def boundary_tree(tmp_path):
    (tmp_path / "pyproject.toml").write_text((REPO / "pyproject.toml").read_text())
    for package in (
        "agent",
        "orchestrator",
        "orchestrator/routers",
        "orchestrator/schemas",
        "orchestrator/services",
        "mcp_server",
        "vm_controller",
        "shared",
        "shared/runtime",
        "shared/contracts",
    ):
        directory = tmp_path / "src" / package
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "__init__.py").write_text("")
    for module, body in {
        "shared/value.py": "VALUE = 1\n",
        "shared/contracts/item.py": "from shared.value import VALUE\n",
        "shared/runtime/provider.py": "from shared.contracts.item import VALUE\n",
        "agent/app.py": "from shared.runtime.provider import VALUE\n",
        "orchestrator/app.py": "from shared.runtime.provider import VALUE\n",
        "orchestrator/main.py": "from orchestrator.app import VALUE\n",
        "orchestrator/routers/contacts.py": "from shared.value import VALUE\n",
        "orchestrator/routers/tables.py": "from shared.value import VALUE\n",
        "orchestrator/routers/preferences.py": "from shared.value import VALUE\n",
        "orchestrator/routers/job_reads.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_queries.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_projection.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_reads.py": "from shared.value import VALUE\n",
        "orchestrator/schemas/job_create.py": "from orchestrator.services.job_create_ingress import VALUE\n",
        "orchestrator/services/job_create_ingress.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_scope.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_config.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_officer.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_workspace.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_datasources.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_delivery.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_creation.py": "from shared.value import VALUE\n",
        "orchestrator/services/job_admission_creator.py": "from shared.value import VALUE\n",
        "orchestrator/services/datasource_policy_errors.py": "from shared.value import VALUE\n",
        "orchestrator/services/officer_metadata.py": "from shared.value import VALUE\n",
        "orchestrator/services/preference_defaults.py": "from shared.value import VALUE\n",
        "orchestrator/services/session_workspace_policy.py": "from shared.value import VALUE\n",
        "orchestrator/services/manifest_legacy.py": "from shared.runtime.provider import VALUE\n",
        "mcp_server/app.py": "from shared.contracts.item import VALUE\n",
        "vm_controller/app.py": "from shared.value import VALUE\n",
    }.items():
        (tmp_path / "src" / module).write_text(body)
    # New extraction boundaries declare their source modules in the manifest.
    # Seed inert modules for those domains so poisoning exercises the real
    # contracts without loading the production application's infrastructure.
    config = tomllib.loads((REPO / "pyproject.toml").read_text())
    for contract in config["tool"]["importlinter"]["contracts"]:
        for module in contract.get("source_modules", []):
            if "*" in module:
                continue
            path = tmp_path / "src" / module.replace(".", "/")
            if path.is_dir() or path.with_suffix(".py").exists():
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.with_suffix(".py").write_text("from shared.value import VALUE\n")
    return tmp_path


def lint_boundaries(root):
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from importlinter.cli import lint_imports_command; lint_imports_command()",
            "--no-cache",
        ],
        cwd=root,
        env={**os.environ, "PYTHONPATH": str(root / "src"), "PYTHONSAFEPATH": "1"},
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_allowed_runtime_and_lightweight_dependencies_pass(boundary_tree):
    result = lint_boundaries(boundary_tree)
    assert result.returncode == 0, result.stdout + result.stderr
    # The generic manifest path also excludes the legacy harness adapter.
    # 24 since R1.B09 added its contract over control, delivery and retirement;
    # 25 since R1.B10 added session transport, projections and permissions.
    assert "Contracts: 25 kept, 0 broken" in result.stdout


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("agent/app.py", "orchestrator.app"),
        ("orchestrator/app.py", "agent.app"),
        ("mcp_server/app.py", "vm_controller.app"),
        ("vm_controller/app.py", "mcp_server.app"),
        ("shared/value.py", "agent.app"),
        ("shared/runtime/provider.py", "orchestrator.app"),
        ("shared/value.py", "shared.runtime.provider"),
        ("shared/contracts/item.py", "shared.runtime.provider"),
        ("orchestrator/routers/manifests.py", "orchestrator.main"),
        ("orchestrator/services/manifests.py", "shared.runtime.provider"),
        ("orchestrator/services/manifests.py", "orchestrator.services.manifest_legacy"),
        ("shared/__init__.py", "shared.runtime.provider"),
        ("mcp_server/app.py", "shared.runtime.provider"),
        ("vm_controller/app.py", "shared.runtime.provider"),
        ("agent/app.py", "src.core.loader"),
        ("orchestrator/app.py", "services.canvas"),
        ("orchestrator/app.py", "database.postgres"),
        ("orchestrator/routers/contacts.py", "orchestrator.main"),
        ("orchestrator/routers/tables.py", "orchestrator.main"),
        ("orchestrator/routers/preferences.py", "orchestrator.main"),
        ("orchestrator/routers/job_reads.py", "orchestrator.main"),
        ("orchestrator/routers/job_inspection.py", "orchestrator.main"),
        ("orchestrator/routers/expert_catalog.py", "orchestrator.main"),
        ("orchestrator/services/provider_catalog.py", "orchestrator.main"),
        ("orchestrator/schemas/job_runtime.py", "orchestrator.main"),
        ("orchestrator/services/job_queries.py", "orchestrator.main"),
        ("orchestrator/services/job_projection.py", "orchestrator.main"),
        ("orchestrator/services/job_reads.py", "orchestrator.main"),
        ("orchestrator/routers/bench.py", "orchestrator.main"),
        ("orchestrator/routers/job_controls.py", "orchestrator.main"),
        ("orchestrator/routers/thread_lifecycle.py", "orchestrator.main"),
        ("orchestrator/services/job_controls.py", "orchestrator.main"),
        (
            "orchestrator/services/manifest_execution_retirement.py",
            "orchestrator.main",
        ),
        ("orchestrator/services/pinned_retirement.py", "orchestrator.main"),
        ("orchestrator/routers/thread_session.py", "orchestrator.main"),
        ("orchestrator/routers/thread_history.py", "orchestrator.main"),
        ("orchestrator/routers/thread_transport.py", "orchestrator.main"),
        ("orchestrator/routers/thread_permissions.py", "orchestrator.main"),
        ("orchestrator/schemas/thread_session.py", "orchestrator.main"),
        ("orchestrator/schemas/thread_transport.py", "orchestrator.main"),
        ("orchestrator/services/session_attention.py", "orchestrator.main"),
        ("orchestrator/services/pinned_forwarding.py", "orchestrator.main"),
        ("orchestrator/services/thread_event_stream.py", "orchestrator.main"),
        ("orchestrator/services/stateless_input_admission.py", "orchestrator.main"),
        ("orchestrator/services/session_tool_view.py", "orchestrator.main"),
        ("orchestrator/services/thread_permissions.py", "orchestrator.main"),
        ("orchestrator/services/thread_projection.py", "orchestrator.main"),
        ("orchestrator/services/magic_link_pages.py", "orchestrator.main"),
        ("orchestrator/services/thread_turn_locks.py", "orchestrator.main"),
        ("orchestrator/services/preference_defaults.py", "orchestrator.main"),
        ("orchestrator/services/session_workspace_policy.py", "orchestrator.main"),
        ("orchestrator/schemas/job_create.py", "orchestrator.main"),
        ("orchestrator/services/job_create_ingress.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_scope.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_config.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_officer.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_workspace.py", "orchestrator.main"),
        ("orchestrator/services/job_admission.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_datasources.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_delivery.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_creation.py", "orchestrator.main"),
        ("orchestrator/services/job_admission_creator.py", "orchestrator.main"),
        ("orchestrator/services/datasource_policy_errors.py", "orchestrator.main"),
        ("orchestrator/services/officer_metadata.py", "orchestrator.main"),
        ("orchestrator/services/manifest_resources.py", "orchestrator.main"),
        ("orchestrator/services/manifest_execution.py", "orchestrator.main"),
        ("orchestrator/services/generic_harness_runtime.py", "shared.runtime.provider"),
        ("orchestrator/services/manifest_workspace_runtime.py", "orchestrator.main"),
        (
            "orchestrator/services/infrastructure_activation_policy.py",
            "orchestrator.main",
        ),
        ("orchestrator/services/usage_reporting.py", "orchestrator.main"),
        ("orchestrator/services/infrastructure_admin.py", "orchestrator.main"),
        ("orchestrator/services/identity.py", "orchestrator.main"),
        ("orchestrator/services/access_tokens.py", "orchestrator.main"),
        ("orchestrator/services/ssh_access.py", "orchestrator.main"),
        ("orchestrator/services/provider_credentials.py", "orchestrator.main"),
        ("orchestrator/services/subscription_management.py", "orchestrator.main"),
        ("orchestrator/services/voice.py", "orchestrator.main"),
        ("orchestrator/services/system_settings.py", "orchestrator.main"),
        ("orchestrator/services/user_administration.py", "orchestrator.main"),
        ("orchestrator/services/job_diagnostics.py", "orchestrator.main"),
        ("orchestrator/routers/usage_reporting.py", "orchestrator.main"),
        ("orchestrator/routers/infrastructure_admin.py", "orchestrator.main"),
        ("orchestrator/routers/identity.py", "orchestrator.main"),
        ("orchestrator/routers/access_tokens.py", "orchestrator.main"),
        ("orchestrator/routers/ssh_access.py", "orchestrator.main"),
        ("orchestrator/routers/provider_credentials.py", "orchestrator.main"),
        ("orchestrator/routers/subscription_management.py", "orchestrator.main"),
        ("orchestrator/routers/voice.py", "orchestrator.main"),
        ("orchestrator/routers/system_settings.py", "orchestrator.main"),
        ("orchestrator/routers/user_administration.py", "orchestrator.main"),
        ("orchestrator/routers/job_diagnostics.py", "orchestrator.main"),
        ("orchestrator/services/datasource_config.py", "orchestrator.main"),
        ("orchestrator/services/datasources.py", "orchestrator.main"),
        ("orchestrator/services/kb_task_registry.py", "orchestrator.main"),
        ("orchestrator/services/knowledge_index.py", "orchestrator.main"),
        ("orchestrator/services/knowledge_projection.py", "orchestrator.main"),
        ("orchestrator/services/knowledge_operations.py", "orchestrator.main"),
        ("orchestrator/services/citations.py", "orchestrator.main"),
        ("orchestrator/services/projects.py", "orchestrator.main"),
        ("orchestrator/services/project_provisioning.py", "orchestrator.main"),
        ("orchestrator/routers/datasources.py", "orchestrator.main"),
        ("orchestrator/routers/knowledge.py", "orchestrator.main"),
        ("orchestrator/routers/citations.py", "orchestrator.main"),
        ("orchestrator/routers/media.py", "orchestrator.main"),
        ("orchestrator/routers/projects.py", "orchestrator.main"),
        ("orchestrator/schemas/media.py", "orchestrator.main"),
        ("vm_controller/app.py", "headscale_client"),
    ],
)
def test_forbidden_dependency_fails_the_gate(boundary_tree, source, target):
    (boundary_tree / "src" / source).write_text(f"import {target}\n")
    result = lint_boundaries(boundary_tree)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "BROKEN" in result.stdout


@pytest.mark.parametrize(
    "source",
    [
        "orchestrator/schemas/job_create.py",
        "orchestrator/services/job_create_ingress.py",
        "orchestrator/services/job_admission_scope.py",
        "orchestrator/services/job_admission_config.py",
        "orchestrator/services/job_admission_officer.py",
        "orchestrator/services/job_admission_workspace.py",
        "orchestrator/services/job_admission.py",
        "orchestrator/services/job_admission_datasources.py",
        "orchestrator/services/job_admission_delivery.py",
        "orchestrator/services/job_admission_creation.py",
        "orchestrator/services/job_admission_creator.py",
        "orchestrator/services/datasource_policy_errors.py",
        "orchestrator/services/officer_metadata.py",
    ],
)
def test_job_create_boundary_rejects_indirect_startup_dependencies(
    boundary_tree, source
):
    (boundary_tree / "src/orchestrator/services/bridge.py").write_text(
        "from orchestrator.main import VALUE\n"
    )
    (boundary_tree / "src" / source).write_text(
        "from orchestrator.services.bridge import VALUE\n"
    )
    result = lint_boundaries(boundary_tree)
    assert result.returncode != 0, result.stdout + result.stderr
    assert (
        "Job creation schemas and ingress policy do not import application startup BROKEN"
        in result.stdout
    )


@pytest.mark.parametrize(
    "source",
    [
        "orchestrator/services/job_queries.py",
        "orchestrator/services/job_projection.py",
        "orchestrator/services/job_reads.py",
    ],
)
def test_job_read_services_reject_indirect_startup_dependencies(boundary_tree, source):
    (boundary_tree / "src/orchestrator/services/bridge.py").write_text(
        "from orchestrator.main import VALUE\n"
    )
    (boundary_tree / "src" / source).write_text(
        "from orchestrator.services.bridge import VALUE\n"
    )
    result = lint_boundaries(boundary_tree)
    assert result.returncode != 0, result.stdout + result.stderr
    assert "Job read services do not import application startup BROKEN" in result.stdout
