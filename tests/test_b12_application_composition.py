"""R1.B12: the application is built by ``create_app()`` and owns its resources.

These tests pin the composition properties B12 introduced:

* two applications built in one process own distinct resources, and every
  router dependency factory binds its own application's stores and registries;
* the resource container never reaches a domain dependency object;
* importing the composition package has no side effects, while ``create_app``
  performs exactly the former import-time construction;
* the deployment gates keep their environment variables and defaults.
"""

from __future__ import annotations

import dataclasses
import os
import subprocess
import sys
import textwrap

import pytest

os.environ.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")

from orchestrator.application import (  # noqa: E402
    ApplicationResources,
    DeploymentSettings,
    build_application_resources,
    create_app,
)

_PER_APPLICATION = (
    "postgres_db",
    "vector_db",
    "audit_store",
    "gitea_client",
    "keycloak_groups",
    "main_cloud_router",
    "session_router",
    "knowledge_graph",
    "ssh_gateway_host_key_cache",
    "job_dispatch_state",
    "kb_datasource_tasks",
    "cloud_task_registry",
    "project_repair_state",
    "stateless_workspace_ensure_registry",
    "attach_abort_successor_tasks",
    "threads_suspending",
    "pending_actions_cache",
    "late_cloud_setup_tasks",
    "thread_turn_locks",
    "expert_catalog_state",
    "catalogue_resources",
    "completion_alerts",
    "completion_runtime",
    "completion_control_boundary",
    "session_memory_runtime",
    "settings",
)


def _factories(app):
    state = app.state._state
    return {
        name: value
        for name, value in sorted(state.items())
        if name.endswith("_factory") and callable(value)
    }


def _build(factory):
    try:
        return factory()
    except TypeError as exc:  # request-scoped factories take the request
        if "required positional argument" in str(exc):
            return None
        raise


def test_two_applications_own_distinct_resources():
    first, second = create_app(), create_app()
    a, b = first.state.resources, second.state.resources
    assert isinstance(a, ApplicationResources) and isinstance(b, ApplicationResources)
    for name in _PER_APPLICATION:
        assert getattr(a, name) is not getattr(b, name), name
    # the completion boundary is built around its own application's runtime
    assert a.completion_control_boundary is not b.completion_control_boundary
    assert first.state.store is a.postgres_db
    assert second.state.store is b.postgres_db
    assert first.state.catalogue_resources is a.catalogue_resources
    assert first.state.expert_catalog_state is a.expert_catalog_state


def test_every_router_factory_binds_its_own_application():
    first, second = create_app(), create_app()
    owned = {
        "postgres_db",
        "vector_db",
        "audit_reader",
        "gitea_client",
        "main_cloud_router",
        "turn_locks",
    }
    checked = 0
    for app, other in ((first, second), (second, first)):
        mine, theirs = app.state.resources, other.state.resources
        own_objects = {
            id(getattr(mine, n)) for n in _PER_APPLICATION if hasattr(mine, n)
        }
        foreign = {
            id(getattr(theirs, n)): n for n in _PER_APPLICATION if hasattr(theirs, n)
        }
        for name, factory in _factories(app).items():
            dependencies = _build(factory)
            if dependencies is None or not dataclasses.is_dataclass(dependencies):
                continue
            for field in dataclasses.fields(dependencies):
                value = getattr(dependencies, field.name)
                assert id(value) not in foreign, (
                    name,
                    field.name,
                    foreign.get(id(value)),
                )
                if id(value) in own_objects:
                    checked += 1
        assert owned  # documented intent; the identity walk above is the check
    assert checked > 50


def test_resources_never_reach_a_dependency_object():
    app = create_app()
    for name, factory in _factories(app).items():
        dependencies = _build(factory)
        if dependencies is None or not dataclasses.is_dataclass(dependencies):
            continue
        for field in dataclasses.fields(dependencies):
            value = getattr(dependencies, field.name)
            assert not isinstance(value, ApplicationResources), (name, field.name)
            if dataclasses.is_dataclass(value):
                for inner in dataclasses.fields(value):
                    assert not isinstance(
                        getattr(value, inner.name), ApplicationResources
                    ), (name, field.name, inner.name)


def test_build_refuses_missing_vector_credentials(monkeypatch):
    for key in (
        "VECTOR_DB_URL",
        "VECTOR_POSTGRES_USER",
        "VECTOR_POSTGRES_PASSWORD",
        "VECTOR_POSTGRES_HOST",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(RuntimeError, match="Vector DB credentials missing"):
        build_application_resources()


def test_importing_the_package_has_no_side_effects():
    code = textwrap.dedent(
        """
        import asyncio, gc, sys
        import orchestrator.application as application
        from orchestrator.database import PostgresDB
        assert "orchestrator.main" not in sys.modules
        assert not [o for o in gc.get_objects() if isinstance(o, PostgresDB)]
        assert not [o for o in gc.get_objects() if isinstance(o, application.ApplicationResources)]
        print("ok")
        """
    )
    env = dict(os.environ)
    env.setdefault("VECTOR_DB_URL", "postgresql://test@localhost/test")
    env["KUBECONFIG"] = "/dev/null"
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=120,
        env=env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().endswith("ok")


@pytest.mark.parametrize(
    ("env", "field", "expected"),
    [
        ({}, "auto_assign_enabled", True),
        ({"AUTO_ASSIGN_ENABLED": "false"}, "auto_assign_enabled", False),
        ({"AUTO_ASSIGN_ENABLED": "YES"}, "auto_assign_enabled", True),
        ({}, "stateless_session_enabled", False),
        ({"STATELESS_SESSION_ENABLED": "1"}, "stateless_session_enabled", True),
        ({"STATELESS_WORKER_ENABLED": "true"}, "stateless_worker_enabled", True),
        (
            {"STATELESS_WORKER_DEFAULT_ENABLED": "on"},
            "stateless_worker_default_enabled",
            False,
        ),
        ({"COMPLETION_COMMANDS_ENABLED": "True"}, "completion_commands_enabled", True),
        (
            {"COMPLETION_STATUS_REORDER_ENABLED": "yes"},
            "completion_status_reorder_enabled",
            True,
        ),
        (
            {"PERSISTENT_AGENT_RECONCILIATION_ENABLED": "true"},
            "persistent_agent_reconciliation_enabled",
            True,
        ),
        (
            {"OFFICER_RUNTIME_VERIFICATION_ENABLED": "true"},
            "officer_runtime_verification_enabled",
            True,
        ),
        (
            {"OFFICER_AUTO_PULL_RELEASE_ENABLED": "true"},
            "officer_auto_pull_release_enabled",
            True,
        ),
        (
            {"COMPLETION_FINALIZER_INLINE_DELAY_SECONDS": "-3"},
            "completion_finalizer_inline_delay_seconds",
            0.0,
        ),
        (
            {"COMPLETION_FINALIZER_INLINE_DELAY_SECONDS": "2.5"},
            "completion_finalizer_inline_delay_seconds",
            2.5,
        ),
    ],
)
def test_deployment_settings_keep_their_environment_contract(
    monkeypatch, env, field, expected
):
    for key in (
        "AUTO_ASSIGN_ENABLED",
        "STATELESS_SESSION_ENABLED",
        "STATELESS_WORKER_ENABLED",
        "STATELESS_WORKER_DEFAULT_ENABLED",
        "COMPLETION_COMMANDS_ENABLED",
        "COMPLETION_STATUS_REORDER_ENABLED",
        "PERSISTENT_AGENT_RECONCILIATION_ENABLED",
        "OFFICER_RUNTIME_VERIFICATION_ENABLED",
        "OFFICER_AUTO_PULL_RELEASE_ENABLED",
        "COMPLETION_FINALIZER_INLINE_DELAY_SECONDS",
    ):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    assert getattr(DeploymentSettings.from_environment(), field) == expected


def test_create_app_uses_explicit_settings():
    settings = dataclasses.replace(
        DeploymentSettings.from_environment(), completion_commands_enabled=True
    )
    app = create_app(settings=settings)
    assert app.state.resources.settings is settings


def test_config_dir_resolves_to_the_repository_config_without_override(monkeypatch):
    """The source-tree candidate is anchored at the package, not at the module
    that asks: the composition modules sit one level deeper than the former
    ``orchestrator/main.py`` caller (R1.B12 regression)."""
    from pathlib import Path

    import orchestrator
    from orchestrator.application import catalogue

    monkeypatch.delenv("CONFIG_DIR", raising=False)
    expected = Path(orchestrator.__file__).resolve().parents[2] / "config"
    assert expected.is_dir()
    assert catalogue.get_config_dir() == expected
    assert (catalogue.get_config_dir() / "experts").is_dir()


@pytest.mark.asyncio
async def test_shared_browser_kick_schedules_the_owner_workspace_reconcile(monkeypatch):
    """R1.B12 caller closure: the router's one collaborator is composed here and
    reaches the owners the former ``orchestrator.main`` import named."""
    import asyncio

    from orchestrator.application import workspace as workspace_composition
    from orchestrator.services import container_provisioner as container_owner
    from orchestrator.services import session_provisioner
    from orchestrator.services import workspace_suspension as suspension_owner

    calls = []

    async def ensure(thread_id, *, db, provisioner, suspension):
        calls.append((thread_id, db, provisioner, suspension))

    provisioner, suspension, db = object(), object(), object()
    monkeypatch.setattr(session_provisioner, "ensure_session_workspace", ensure)
    monkeypatch.setattr(container_owner, "container_provisioner", provisioner)
    monkeypatch.setattr(suspension_owner, "workspace_suspension_service", suspension)
    resources = build_application_resources()
    dependencies = workspace_composition.shared_browser_dependencies(resources)
    assert dependencies.kick_workspace_provisioning("thread-1", db) is None
    await asyncio.sleep(0)
    assert calls == [("thread-1", db, provisioner, suspension)]
