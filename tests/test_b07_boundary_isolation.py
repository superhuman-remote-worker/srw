"""B07's modules and its five assigned caller files stand on their own.

R1.B07 moved message routing, the notification feed and its action registry,
the Officer Post and the project-loop engine out of ``orchestrator.main``, and
closed the five caller files the ledger assigned it: ``routers/automations.py``,
``routers/project_loops.py``, ``services/cron_dispatcher.py``,
``services/sitrep.py`` and ``services/project_backlog.py``.

The point of that move is not tidiness. While the two routers resolved module
singletons, two FastAPI applications in one process shared one store no matter
which was answering; while the sitrep and the backlog imported the application
module, neither could be exercised — nor reasoned about — without the whole
startup chain; and the cron loop reached for the application's forge client at
the moment a job happened to be created, long after the request that started it
was gone.

These cases hold that closure. A regression to any process-wide lookup fails
here rather than in production.
"""

from __future__ import annotations

import ast
import inspect
import pathlib
from types import SimpleNamespace
from typing import Any

import pytest
from fastapi.testclient import TestClient

from orchestrator.routers import (
    actions,
    agent_officer,
    automations,
    loop_plan,
    messaging,
    notifications,
    officers,
    project_loops,
)
from orchestrator.services import (
    agent_messaging,
    cron_dispatcher,
    curation_final_pass,
    inbound_reply,
    job_freeze_notifications,
    job_guidance,
    loop_plan_filing,
    message_thread_reads,
    notification_actions,
    notification_api,
    officer_conference,
    officer_message_actions,
    officer_notices,
    officer_paging,
    officer_post_lifecycle,
    officer_post_policy,
    officer_post_views,
    officer_watchdog,
    pending_actions,
    project_backlog,
    project_loop_advance,
    project_loop_spawn,
    sitrep,
)

from ._mounted_router import mount_router
from orchestrator.application import workflows as workflows_composition

ROOT = pathlib.Path(__file__).resolve().parents[1]

B07_SERVICES = [
    agent_messaging,
    curation_final_pass,
    inbound_reply,
    job_freeze_notifications,
    job_guidance,
    loop_plan_filing,
    message_thread_reads,
    notification_actions,
    notification_api,
    officer_conference,
    officer_message_actions,
    officer_notices,
    officer_paging,
    officer_post_lifecycle,
    officer_post_policy,
    officer_post_views,
    officer_watchdog,
    pending_actions,
    project_loop_advance,
    project_loop_spawn,
]
B07_ROUTERS = [
    actions,
    agent_officer,
    loop_plan,
    messaging,
    notifications,
    officers,
]
B07_CLOSED_CALLERS = [
    automations,
    project_loops,
    cron_dispatcher,
    sitrep,
    project_backlog,
]


# --------------------------------------------------------------------------- #
# Nothing this batch owns reaches the application module at all
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    "module",
    B07_SERVICES + B07_ROUTERS + B07_CLOSED_CALLERS,
    ids=lambda m: m.__name__.rsplit(".", 1)[-1],
)
def test_b07_module_never_imports_the_application_module(module) -> None:
    """Neither at import time nor from inside any function body."""

    tree = ast.parse(inspect.getsource(module))
    offenders = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module == "orchestrator.main"
    ] + [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        and any(alias.name == "orchestrator.main" for alias in node.names)
    ]
    assert offenders == [], offenders


# --------------------------------------------------------------------------- #
# Each router resolves its collaborators from the requesting application
# --------------------------------------------------------------------------- #


@pytest.mark.parametrize(
    ("resolver", "attribute"),
    [
        (
            messaging.get_agent_messaging_dependencies,
            "agent_messaging_dependencies_factory",
        ),
        (
            messaging.get_inbound_reply_dependencies,
            "inbound_reply_dependencies_factory",
        ),
        (
            messaging.get_officer_message_action_dependencies,
            "officer_message_action_dependencies_factory",
        ),
        (messaging.get_job_guidance_dependencies, "job_guidance_dependencies_factory"),
        (
            messaging.get_message_thread_read_dependencies,
            "message_thread_read_dependencies_factory",
        ),
        (
            actions.get_pending_actions_dependencies,
            "pending_actions_dependencies_factory",
        ),
        (
            notifications.get_notification_api_dependencies,
            "notification_api_dependencies_factory",
        ),
        (
            officers.get_officer_post_view_dependencies,
            "officer_post_view_dependencies_factory",
        ),
        (
            officers.get_officer_post_lifecycle_dependencies,
            "officer_post_lifecycle_dependencies_factory",
        ),
        (
            agent_officer.get_officer_paging_dependencies,
            "officer_paging_dependencies_factory",
        ),
        (
            loop_plan.get_loop_plan_filing_dependencies,
            "loop_plan_filing_dependencies_factory",
        ),
        (automations.get_automations_dependencies, "automations_dependencies_factory"),
        (
            project_loops.get_project_loops_dependencies,
            "project_loops_dependencies_factory",
        ),
    ],
    ids=lambda value: value if isinstance(value, str) else "",
)
def test_router_resolver_reads_only_the_requesting_application(
    resolver, attribute
) -> None:
    helper = ast.parse(inspect.getsource(resolver)).body[0]
    assert isinstance(helper, ast.FunctionDef)
    assert [argument.arg for argument in helper.args.args] == ["request"]
    assert not [
        node
        for node in ast.walk(helper)
        if isinstance(node, (ast.Import, ast.ImportFrom))
    ]
    assert ast.unparse(helper.body[-1]) == f"return request.app.state.{attribute}()"


# --------------------------------------------------------------------------- #
# Two applications, two stores — proved through the real routes
# --------------------------------------------------------------------------- #


class _EmptyAutomationStore:
    """Answers the ACL read with "no such row", which is a clean 404."""

    def __init__(self) -> None:
        self.seen = 0

    async def get_automation(self, automation_id: str) -> None:
        del automation_id
        self.seen += 1
        return None


def _automations_app(store: Any):
    app = mount_router(automations.router)
    app.state.automations_dependencies_factory = (
        lambda: automations.AutomationsDependencies(
            store=store,
            gitea_client=object(),
            main_cloud_router=object(),
            trigger_dispatch=lambda: None,
        )
    )
    return app


def test_automations_route_uses_the_store_of_its_own_application(monkeypatch) -> None:
    async def _approved(request, db):
        del request, db
        return {"id": "u1", "is_admin": True, "is_approved": True}

    monkeypatch.setattr(automations, "require_approved_user", _approved)

    store_a, store_b = _EmptyAutomationStore(), _EmptyAutomationStore()
    app_a, app_b = _automations_app(store_a), _automations_app(store_b)

    for app in (app_a, app_b, app_a):
        response = TestClient(app).get("/api/automations/a1")
        assert response.status_code == 404

    assert (store_a.seen, store_b.seen) == (2, 1)


class _NoLoopStore:
    def __init__(self) -> None:
        self.seen = 0

    async def get_active_project_loop(self, project_id: str) -> None:
        del project_id
        self.seen += 1
        return None

    async def list_project_loops(self, project_id: str) -> list:
        del project_id
        return []


def _project_loops_app(store: Any):
    app = mount_router(project_loops.router)
    app.state.project_loops_dependencies_factory = (
        lambda: project_loops.ProjectLoopsDependencies(
            store=store,
            vector_store=object(),
            spawn_loop_stage=None,
            writeback_loop_stage=None,
            resume_project_loop=None,
            check_vm_permission=None,
        )
    )
    return app


def test_project_loops_route_uses_the_store_of_its_own_application(monkeypatch) -> None:
    async def _approved(request, db):
        del request, db
        return {"id": "u1", "is_approved": True}

    async def _member(request, db, project_id, min_role="viewer", allow_archived=True):
        del request, db, project_id, min_role, allow_archived
        return {"id": "u1"}, {"id": "p1"}

    monkeypatch.setattr(project_loops, "require_approved_user", _approved)
    monkeypatch.setattr(project_loops, "require_project_member", _member)

    store_a, store_b = _NoLoopStore(), _NoLoopStore()
    app_a, app_b = _project_loops_app(store_a), _project_loops_app(store_b)

    for app in (app_a, app_b, app_a):
        response = TestClient(app).get("/api/projects/p1/loop")
        assert response.status_code == 404

    assert (store_a.seen, store_b.seen) == (2, 1)


# --------------------------------------------------------------------------- #
# The two background paths carry their collaborators explicitly
# --------------------------------------------------------------------------- #


def test_cron_dispatcher_takes_its_provisioning_adapter_as_an_argument() -> None:
    """The loop outlives every request, so the adapter is a parameter."""

    signature = inspect.signature(cron_dispatcher.cron_dispatcher_loop)
    assert "provision_repo" in signature.parameters
    assert signature.parameters["provision_repo"].default is None
    tick = inspect.signature(cron_dispatcher._process_one_due_automation)
    assert "provision_repo" in tick.parameters


def test_cron_provisioning_adapter_forwards_the_applications_clients() -> None:
    """The six-line adapter `main` hands the dispatcher is covered here.

    `tests/test_cron_dispatcher.py` covers the dispatcher's *decision* to call
    an adapter; nothing else covers the mapping from the loop's ``(job_row, db)``
    to ``provision_job_repo``'s keywords, and getting that wrong would leave
    every cron-fired job repo-less in exactly the way the parity fix removed.
    """
    import asyncio

    import orchestrator.main as main
    from orchestrator.services import job_provisioning

    seen: dict[str, object] = {}

    async def _provision(**kwargs):
        seen.update(kwargs)

    original = job_provisioning.provision_job_repo
    job_provisioning.provision_job_repo = _provision
    try:
        job_row = {"id": "j1"}
        store = object()
        asyncio.run(
            workflows_composition.provision_cron_job_repo(
                main.app.state.resources, job_row, store
            )
        )
    finally:
        job_provisioning.provision_job_repo = original

    assert seen["job_row"] is job_row
    # The dispatcher's own handle, not the application's — the fired job is
    # written through the transaction the loop already holds.
    assert seen["postgres_db"] is store
    assert seen["gitea_client"] is main.app.state.resources.gitea_client
    assert seen["main_cloud_router"] is main.app.state.resources.main_cloud_router


def test_officer_watchdog_re_reads_its_recycler_through_a_callable() -> None:
    """Startup assigns the recycler after the provisioners; the tick re-reads."""

    fields = {
        field.name: field.type
        for field in __import__("dataclasses").fields(
            officer_watchdog.OfficerWatchdogDependencies
        )
    }
    assert "persistent_thread_recycler" in fields
    source = inspect.getsource(officer_watchdog.officer_watchdog_check_one)
    assert "dependencies.persistent_thread_recycler()" in source


def test_sitrep_takes_its_reporting_handles_from_an_explicit_bind() -> None:
    """No application import; an all-``None`` default is the degraded path."""

    # The default is an all-``None`` instance, but another case in the same
    # worker may have bound handles first, so this restores whatever it found
    # rather than asserting on it.
    before = sitrep.reporting_handles()
    marker = SimpleNamespace(name="audit")
    sitrep.bind_reporting_handles(
        sitrep.ReportingHandles(audit_reader=marker, usage_ledger=None, vector_db=None)
    )
    try:
        assert sitrep.reporting_handles().audit_reader is marker
        assert sitrep._resolve_handles() == (marker, None)
        assert sitrep._resolve_vector_db() is None
    finally:
        sitrep.bind_reporting_handles(before)


def test_backlog_ticket_mirror_requires_the_store_from_its_caller() -> None:
    """The ``main`` fallback is gone; a caller that passes none gets no repo."""

    tree = ast.parse(inspect.getsource(project_backlog._resolve_note_repo))
    assert not [
        node
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        and "orchestrator.main" in ast.unparse(node)
    ]
    signature = inspect.signature(project_backlog.close_backlog_ticket)
    assert "postgres_db" in signature.parameters
