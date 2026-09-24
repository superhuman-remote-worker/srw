"""Pytest configuration for tests."""

import os
import sys
import tempfile
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

# orchestrator/main.py SystemExits at import time unless the license gate is
# accepted. Tests only exercise its utility functions — auto-accept here so
# CI and local runs don't need extra setup.
os.environ.setdefault("LICENSE_TERMS_ACCEPTED", "true")

# Set WORKSPACE_PATH to a temp directory for tests so that workspace files
# (logs, checkpoints, uploads) don't get created inside the repository.
if "WORKSPACE_PATH" not in os.environ:
    _test_workspace = tempfile.mkdtemp(prefix="srw_test_workspace_")
    os.environ["WORKSPACE_PATH"] = _test_workspace

# Provide a deterministic encryption key so any code path that touches the
# orchestrator's canonical ``security.crypto`` module during tests has a working
# cipher. Real credentials never run through this key.
os.environ.setdefault(
    "APP_ENCRYPTION_KEY", "eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHh4eHg="
)

from orchestrator.security import crypto as _encryption_crypto  # noqa: E402


@pytest.fixture(autouse=True)
def _isolate_encryption_cipher_cache():
    """Prevent a test-specific encryption key from leaking between tests."""
    _encryption_crypto.reset_cipher_cache()
    yield
    _encryption_crypto.reset_cipher_cache()


from orchestrator.services import notification_catalog as _notification_catalog  # noqa: E402

_NOTIFICATION_REGISTRIES = (
    _notification_catalog._ACTION_HANDLERS,
    _notification_catalog._SOURCE_LOADERS,
    _notification_catalog._SOURCE_PROBES,
)


@pytest.fixture(autouse=True)
def _isolate_notification_registries():
    """Keep the notification catalog's process-global registries per-test.

    ``notification_actions.register_notification_actions()`` installs the live
    action handlers, source loaders and source probes into module-level dicts. A
    test that calls it leaves them installed for every later test in the
    same xdist worker, and under ``--dist loadfile`` which files share a
    worker is a scheduling detail — so the damage lands as an intermittent,
    order-dependent CI failure far from the test that caused it. The live
    ``job`` probe, for one, reads a non-existent job as *resolved* ("deleted:
    nothing left to decide"), which silently cancels the deferred steps a
    sweeper test is asserting on.

    Restoring the snapshot keeps the registries the caller's own business,
    the way ``tests/test_notification_record.py``'s ``handlers`` fixture
    already does for its own registrations.
    """
    saved = [dict(registry) for registry in _NOTIFICATION_REGISTRIES]
    yield
    for registry, snapshot in zip(_NOTIFICATION_REGISTRIES, saved):
        registry.clear()
        registry.update(snapshot)


# orchestrator/main.py also guards on vector-DB credentials at import time.
# Tests only exercise its utility functions / pure models, never the vector
# store, so provide a dummy URL. setdefault never overrides a real CI/prod value.
os.environ.setdefault("VECTOR_DB_URL", "postgresql://test:test@localhost:5432/test")

# =============================================================================
# Hermetic build-provenance environment
# =============================================================================
#
# Both workflows declare SRW_SOURCE_URL, SRW_DOCUMENTATION_URL and
# SRW_RELEASE_VERSION in their top-level ``env:`` block, so GitHub injects them
# into every step of every job — including ``pytest tests/``. Those are exactly
# the names src/core/runtime_provenance.py reads, so a test asserting that a
# provenance field is absent passes locally (unset) and fails in CI (set), which
# no local run can reproduce. Strip the whole surface before each test; a test
# that wants a value still sets it via monkeypatch, which runs after this.

from shared.runtime.core.product_capabilities import ProductComponent  # noqa: E402

_PROVENANCE_FIELDS = (
    "SOURCE_REVISION",
    "SOURCE_URL",
    "ARTIFACT_DIGEST",
    "RELEASE_VERSION",
    "DOCUMENTATION_URL",
)
_PROVENANCE_ENV_VARS = (
    "SRW_COMPONENT",
    "SRW_DEPLOYMENT_PROVENANCE_JSON",
    # Declared alongside the fields above in the register payload.
    "BUILD_SHA",
    *(f"SRW_{field}" for field in _PROVENANCE_FIELDS),
    *(
        f"SRW_{component.value.upper()}_{field}"
        for component in ProductComponent
        for field in _PROVENANCE_FIELDS
    ),
)


@pytest.fixture(autouse=True)
def _isolate_declared_provenance_env(monkeypatch):
    """Keep ambient build metadata out of every test's environment."""
    for name in _PROVENANCE_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


# =============================================================================
# Orchestrator application-state hygiene
# =============================================================================
#
# The entrypoint's application (``orchestrator.main.app``) owns its resources
# (``app.state.resources``: stores, clients, registries, deployment settings),
# and the process-wide service singletons live in their owning modules. A test
# that swaps one in with a bare assignment rather than monkeypatch leaks it into
# every test that runs after it in the same process, and the failure surfaces
# hundreds of tests later in a file that never touched it — e.g. a leaked
# ``MagicMock`` store turning an unrelated ``await store.<method>()`` into
# "MagicMock can't be used in 'await'". Snapshot the identities around every
# test and put them back, so a missing restore stays local to the test that
# made it.

_OWNER_SINGLETONS = (
    ("orchestrator.services.agent_provisioner", "agent_provisioner"),
    ("orchestrator.services.container_provisioner", "container_provisioner"),
    ("orchestrator.services.docker_provisioner", "docker_provisioner"),
    ("orchestrator.services.email", "email_service"),
    ("orchestrator.services.ide_proxy", "ide_proxy_service"),
    ("orchestrator.services.ide_session", "ide_session_service"),
    ("orchestrator.services.imap_poller", "imap_poller"),
    ("orchestrator.services.nats_bridge", "nats_bridge"),
    ("orchestrator.services.notification_service", "notification_service"),
    ("orchestrator.services.persistent_provisioner", "persistent_provisioner"),
    ("orchestrator.services.snapshot_service", "snapshot_service"),
    ("orchestrator.services.sudo_gate", "sudo_gate"),
    ("orchestrator.services.vm_provisioner", "vm_provisioner"),
    ("orchestrator.services.workspace", "workspace_service"),
    ("orchestrator.services.workspace_suspension", "workspace_suspension_service"),
    # Process-wide seams ``create_app()`` binds to the most recently built
    # application; a test that builds its own application must not leave them
    # pointing at it.
    ("orchestrator.security.auth", "_cloud_router_provider"),
    ("orchestrator.security.auth", "_forge_provider"),
    ("orchestrator.routers.vm_workspace_cleanup_authority", "_store_factory"),
    ("orchestrator.routers.vm_creation_retry_authority", "_store_factory"),
    ("orchestrator.routers.vm_resource_inventory", "_configuration"),
)


@pytest.fixture(autouse=True)
def _restore_orchestrator_singletons():
    import dataclasses

    snapshots = []
    main = sys.modules.get("orchestrator.main")
    resources = getattr(
        getattr(getattr(main, "app", None), "state", None), "_state", {}
    )
    resources = resources.get("resources") if isinstance(resources, dict) else None
    if resources is not None:
        fields = [f.name for f in dataclasses.fields(resources)]
        snapshots.append(
            (
                resources,
                {n: getattr(resources, n) for n in fields if hasattr(resources, n)},
            )
        )
        settings = resources.settings
        snapshots.append((settings, dataclasses.asdict(settings)))
    for module_name, attribute in _OWNER_SINGLETONS:
        module = sys.modules.get(module_name)
        if module is not None and hasattr(module, attribute):
            snapshots.append((module, {attribute: getattr(module, attribute)}))
    try:
        yield
    finally:
        for owner, snapshot in snapshots:
            for name, value in snapshot.items():
                if getattr(owner, name, None) is not value:
                    setattr(owner, name, value)


# =============================================================================
# F1 multi-tenancy fixture — three users, two projects, jobs/threads/sessions
# =============================================================================
#
# This is the canonical fixture for any test that needs to exercise the
# visibility model in orchestrator/security/access.py. It is mock-only — no
# real Postgres — so tests stay fast. The shapes mirror what the real DB
# layer returns; if you find yourself reaching for a field that isn't here,
# extend the fixture rather than re-inventing it per-file.
#
# Layout:
#   user_a  ──owner──▶ project_a ──contains──▶ job_a
#                                    │
#                                    └─thread_a
#   user_b  ──owner──▶ project_b ──contains──▶ job_b
#                                    │
#                                    └─thread_b
#   user_admin (is_admin=True, no project membership; admin role bypasses)
#
# user_a has no membership in project_b (and vice versa). user_admin has no
# membership rows but should pass every gate via the is_admin bypass.


_UID_A = UUID("11111111-1111-1111-1111-111111111111")
_UID_B = UUID("22222222-2222-2222-2222-222222222222")
_UID_ADMIN = UUID("33333333-3333-3333-3333-333333333333")
_PID_A = UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")
_PID_B = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
_JID_A = UUID("a1111111-1111-1111-1111-111111111111")
_JID_B = UUID("b2222222-2222-2222-2222-222222222222")
_TID_A = UUID("a3333333-3333-3333-3333-333333333333")
_TID_B = UUID("b4444444-4444-4444-4444-444444444444")
_SID_A = UUID("a5555555-5555-5555-5555-555555555555")
_SID_B = UUID("b6666666-6666-6666-6666-666666666666")
_DSID_A = UUID("a7777777-7777-7777-7777-777777777777")
_DSID_B = UUID("b8888888-8888-8888-8888-888888888888")
_DSID_GLOBAL = UUID("c9999999-9999-9999-9999-999999999999")


def _make_user(uid: UUID, name: str, is_admin: bool = False) -> dict:
    # ``real_is_admin`` mirrors the contract of
    # ``security.auth.require_approved_user`` — it always sets the flag so
    # downstream gates (``_require_admin``) can read it. Tests use the
    # already-resolved user dict, so the flag matches ``is_admin`` here.
    # The "view as user" shadow (X-Admin-View-As: user) is tested in
    # tests/test_view_as_user.py against the auth resolver itself.
    return {
        "id": uid,
        "display_name": name,
        "email": f"{name}@example.test",
        "is_admin": is_admin,
        "real_is_admin": is_admin,
        "is_approved": True,
        "auth_method": "cookie",
        "scopes": [],
    }


@pytest.fixture
def user_a() -> dict:
    return _make_user(_UID_A, "user_a")


@pytest.fixture
def user_b() -> dict:
    return _make_user(_UID_B, "user_b")


@pytest.fixture
def user_admin() -> dict:
    return _make_user(_UID_ADMIN, "admin", is_admin=True)


@pytest.fixture
def project_a() -> dict:
    return {"id": _PID_A, "name": "Project A", "user_id": _UID_A}


@pytest.fixture
def project_b() -> dict:
    return {"id": _PID_B, "name": "Project B", "user_id": _UID_B}


@pytest.fixture
def job_a() -> dict:
    return {"id": _JID_A, "user_id": _UID_A, "project_id": _PID_A, "status": "created"}


@pytest.fixture
def job_b() -> dict:
    return {"id": _JID_B, "user_id": _UID_B, "project_id": _PID_B, "status": "created"}


# ``execution_lane`` mirrors the column's own contract: threads.execution_lane
# is NOT NULL DEFAULT 'pinned' (migration 0115a), so a real row can never carry
# None. Omitting it here made these fixtures the only place in the system where
# the lane is absent, which routes callers into the deliberate fail-closed
# "unknown future lanes" branch (resume_thread) rather than the pinned path
# they are exercising.
@pytest.fixture
def thread_a() -> dict:
    return {
        "id": _TID_A,
        "user_id": _UID_A,
        "title": "thread A",
        "execution_lane": "pinned",
    }


@pytest.fixture
def thread_b() -> dict:
    return {
        "id": _TID_B,
        "user_id": _UID_B,
        "title": "thread B",
        "execution_lane": "pinned",
    }


@pytest.fixture
def datasource_a() -> dict:
    """Created by user_a, linked to project_a."""
    return {
        "id": _DSID_A,
        "name": "ds_a",
        "type": "postgresql",
        "created_by": _UID_A,
        "credentials": {"username": "pg_a", "password": "secret_a"},
        "connection_url": "postgresql://host/db_a",
        "is_global": False,
        "job_id": None,
    }


@pytest.fixture
def datasource_b() -> dict:
    """Created by user_b, linked to project_b."""
    return {
        "id": _DSID_B,
        "name": "ds_b",
        "type": "postgresql",
        "created_by": _UID_B,
        "credentials": {"username": "pg_b", "password": "secret_b"},
        "connection_url": "postgresql://host/db_b",
        "is_global": False,
        "job_id": None,
    }


@pytest.fixture
def datasource_global() -> dict:
    """Created by admin, no project link — admin-only by visibility."""
    return {
        "id": _DSID_GLOBAL,
        "name": "ds_global",
        "type": "postgresql",
        "created_by": _UID_ADMIN,
        "credentials": {"username": "pg_root", "password": "rootsecret"},
        "connection_url": "postgresql://host/db_global",
        "is_global": True,
        "job_id": None,
    }


@pytest.fixture
def fake_db(
    project_a,
    project_b,
    job_a,
    job_b,
    thread_a,
    thread_b,
    datasource_a,
    datasource_b,
    datasource_global,
):
    """AsyncMock postgres_db prewired with the 3-user / 2-project graph.

    Lookups are dispatched on UUID — both ``UUID`` objects and the string
    form are accepted, matching how the real layer normalises. Methods
    that return None for missing rows do so here as well.
    """
    projects = {_PID_A: project_a, _PID_B: project_b}
    jobs = {_JID_A: job_a, _JID_B: job_b}
    threads = {_TID_A: thread_a, _TID_B: thread_b}
    datasources = {
        _DSID_A: datasource_a,
        _DSID_B: datasource_b,
        _DSID_GLOBAL: datasource_global,
    }
    # (project_id, user_id) → role. Each project owner is its sole member.
    memberships: dict[tuple[UUID, UUID], str] = {
        (_PID_A, _UID_A): "owner",
        (_PID_B, _UID_B): "owner",
    }
    # datasource_id → [project_id, ...] linkages via project_datasources.
    datasource_projects: dict[UUID, list[UUID]] = {
        _DSID_A: [_PID_A],
        _DSID_B: [_PID_B],
        _DSID_GLOBAL: [],
    }

    def _to_uuid(value):
        if isinstance(value, UUID):
            return value
        try:
            return UUID(str(value))
        except (ValueError, TypeError):
            return None

    async def get_project(pid):
        return projects.get(_to_uuid(pid))

    async def get_job(jid):
        return jobs.get(_to_uuid(jid))

    async def get_thread(tid):
        return threads.get(_to_uuid(tid))

    async def get_user_role_in_project(pid, uid):
        return memberships.get((_to_uuid(pid), _to_uuid(uid)))

    async def get_projects_for_user(uid, limit=100, statuses=None):
        u = _to_uuid(uid)
        rows = [
            projects[pid]
            for (pid, member_uid), _role in memberships.items()
            if member_uid == u
        ]
        if statuses is None:
            return rows
        # Mirror project_status_filter_sql: requested statuses pass, and so
        # does anything outside the known vocabulary (NULL included), so a row
        # nobody can classify is never silently swallowed.
        wanted = {str(s).lower() for s in statuses}
        return [
            row
            for row in rows
            if (str(row.get("status") or "active").lower() in wanted)
            or (
                str(row.get("status") or "active").lower() not in {"active", "archived"}
            )
        ]

    async def get_datasource(dsid):
        return datasources.get(_to_uuid(dsid))

    async def list_datasource_projects(dsid):
        return [str(pid) for pid in datasource_projects.get(_to_uuid(dsid), [])]

    async def list_datasource_projects_bulk(dsids):
        # Batched form: {datasource_id: [project_id, ...]}, entries only for
        # linked datasources. Keys mirror the caller's str(ds["id"]) inputs.
        out: dict[str, list[str]] = {}
        for dsid in dsids:
            pids = datasource_projects.get(_to_uuid(dsid), [])
            if pids:
                out[str(dsid)] = [str(pid) for pid in pids]
        return out

    async def list_datasources(job_id=None, ds_type=None, limit=100):
        return list(datasources.values())

    db = AsyncMock()
    db.get_project = AsyncMock(side_effect=get_project)
    db.get_job = AsyncMock(side_effect=get_job)
    db.get_thread = AsyncMock(side_effect=get_thread)
    db.get_user_role_in_project = AsyncMock(side_effect=get_user_role_in_project)
    db.get_projects_for_user = AsyncMock(side_effect=get_projects_for_user)
    db.get_datasource = AsyncMock(side_effect=get_datasource)
    db.list_datasource_projects = AsyncMock(side_effect=list_datasource_projects)
    db.list_datasource_projects_bulk = AsyncMock(
        side_effect=list_datasource_projects_bulk
    )
    db.list_datasources = AsyncMock(side_effect=list_datasources)

    # PostgresDB.acquire() is an @asynccontextmanager, not a coroutine: the
    # owner reads (read_vm_idle_states on job/thread detail and list, and
    # VMIdleLifecycleStore on pinned Resume) open it with ``async with``. This
    # graph holds no VM contexts and no vm_idle_operations rows, so every raw
    # read answers empty, as the real query would. Only reads are modelled; a
    # raw write through this double still fails loudly. Tests needing SQL
    # rows keep overriding ``fake_db.acquire`` themselves.
    empty_reads = SimpleNamespace(
        fetch=AsyncMock(return_value=[]),
        fetchrow=AsyncMock(return_value=None),
        fetchval=AsyncMock(return_value=None),
    )

    @asynccontextmanager
    async def acquire():
        yield empty_reads

    db.acquire = MagicMock(side_effect=acquire)
    return db


@pytest.fixture
def fake_request():
    """Minimal stand-in for fastapi.Request.

    The access.py helpers only call ``require_approved_user(request, db)``
    on it; that path doesn't touch the request when callers patch the
    auth resolver in tests. A bare MagicMock with no spec is enough.
    """
    return MagicMock()
