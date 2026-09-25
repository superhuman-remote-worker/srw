"""Control-plane contract for OKF Knowledge Base datasources (Slice 4)."""

from tests import _b09_control_seams as control_seams

import asyncio
from datetime import datetime, timezone
from contextlib import asynccontextmanager
from uuid import UUID
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException

from orchestrator.schemas.thread_admission import ThreadCreateRequest
import orchestrator.main
from orchestrator.services.thread_project_authorization import (
    thread_creation_project_ids as _thread_creation_project_ids,
)
from orchestrator.routers.datasources import (
    DatasourcesDependencies,
    create_datasource,
    delete_datasource,
    get_datasource_index_status,
    reindex_datasource_knowledge,
    update_datasource,
)
from orchestrator.schemas.datasources import DatasourceCreate, DatasourceUpdate
from orchestrator.services.config_drift import DriftItem
from orchestrator.services.datasource_config import (
    normalize_kb_config,
    validate_kb_repository_url,
)
from orchestrator.services.datasources import DatasourceDependencies
from orchestrator.services.kb_datasources import (
    index_status_payload,
    reindex_kb_datasource,
    test_kb_datasource as probe_kb_datasource,
)
from orchestrator.services.kb_task_registry import KbDatasourceTaskRegistry
from orchestrator.services.knowledge_index import KnowledgeIndexDependencies
from shared.runtime.services.knowledge_store import KbWatermark
from orchestrator.application import preparation as preparation_composition
from orchestrator.application import sessions as sessions_composition
from orchestrator.services import (
    agent_datasource_payload as agent_datasource_payload_module,
)
from orchestrator.services import datasource_config as datasource_config_module
from orchestrator.services import deployment_gates as deployment_gates_module
from orchestrator.services import thread_admission as thread_admission_module
from orchestrator.services import (
    thread_project_authorization as thread_project_authorization_module,
)


def _ds_deps(store=None, **gates) -> DatasourcesDependencies:
    """Connector router dependencies, composed the way main's factory does.

    Gates named in ``gates`` replace what these tests used to patch onto
    ``orchestrator.main``; every gate left unnamed keeps the real
    ``orchestrator.security.access`` implementation the dataclass defaults to,
    which is exactly what an unpatched ``main`` global used to resolve to. The
    MCP predicates come from ``main`` itself so their behavior is unchanged.
    """

    db = MagicMock() if store is None else store
    return DatasourcesDependencies(
        store=db,
        operations=DatasourceDependencies(
            store=db,
            vector_db=MagicMock(),
            knowledge_index=KnowledgeIndexDependencies(
                store=db,
                vector_db=MagicMock(),
                gitea_client=MagicMock(),
                logger=MagicMock(),
                tasks=KbDatasourceTaskRegistry(),
                inject_system_kb_embedding_profile=AsyncMock(return_value=None),
            ),
            mcp_datasources_enabled=deployment_gates_module.mcp_datasources_enabled,
            validate_mcp_datasource=datasource_config_module.validate_mcp_datasource,
        ),
        **gates,
    )


@pytest.fixture(autouse=True)
def _trust_test_git_hosts(monkeypatch):
    monkeypatch.setenv(
        "KB_GIT_ALLOWED_HOSTS",
        "example.test,git.example.test,host",
    )


class TestNormalizeKbConfig:
    def test_defaults_to_repository_root(self):
        assert normalize_kb_config(None) == {"root_path": ""}
        assert normalize_kb_config({}) == {"root_path": ""}

    def test_normalizes_relative_posix_path(self):
        assert normalize_kb_config({"root_path": r"./docs\\knowledge//notes"}) == {
            "root_path": "docs/knowledge/notes"
        }

    @pytest.mark.parametrize(
        "root",
        ["/absolute", "../escape", "docs/../escape", "https://host/vault", "a\x00b"],
    )
    def test_rejects_unsafe_root(self, root):
        with pytest.raises(HTTPException) as exc:
            normalize_kb_config({"root_path": root})
        assert exc.value.status_code == 400

    def test_rejects_non_string_and_unknown_keys(self):
        with pytest.raises(HTTPException):
            normalize_kb_config({"root_path": 42})
        with pytest.raises(HTTPException) as exc:
            normalize_kb_config({"path_prefix": "knowledge"})
        assert "path_prefix" in str(exc.value.detail)


class TestKbRepositoryUrl:
    @pytest.mark.parametrize(
        "url",
        [
            "https://github.com/acme/wiki.git",
            "http://git.internal/acme/wiki.git",
            "ssh://git@github.com/acme/wiki.git",
            "git@github.com:acme/wiki.git",
        ],
    )
    def test_accepts_normal_git_urls(self, url):
        assert validate_kb_repository_url(url) == url

    @pytest.mark.parametrize(
        "url",
        [
            "",
            None,
            "https://token@github.com/acme/wiki.git",
            "https://u:p@host/repo",
            "https://host/repo.git?token=secret",
            "https://host/repo.git#branch",
            "user:password@host:repo.git",
            "git://host/repo.git",
            "file:///tmp/repo.git",
            "/tmp/repo.git",
            "ext::sh -c id",
            "--upload-pack=evil",
        ],
    )
    def test_rejects_missing_or_embedded_credentials(self, url):
        with pytest.raises(HTTPException) as exc:
            validate_kb_repository_url(url)
        assert exc.value.status_code == 400


def test_datasource_models_carry_non_secret_config():
    create = DatasourceCreate(
        name="Engineering Knowledge",
        type="kb",
        connection_url="https://github.com/acme/wiki.git",
        config={"root_path": "knowledge"},
    )
    assert create.config == {"root_path": "knowledge"}
    assert DatasourceUpdate(config={"root_path": "docs"}).config == {
        "root_path": "docs"
    }


def test_kb_dispatch_payload_is_source_qualified_and_credential_free():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    sentinel_token = "TOKEN_MUST_NOT_REACH_AGENT"
    sentinel_key = "KEY_MUST_NOT_REACH_AGENT"
    result = agent_datasource_payload_module.build_datasources_payload(
        [
            {
                "id": datasource_id,
                "type": "kb",
                "name": "Engineering Knowledge",
                "description": "Shared architecture notes",
                "connection_url": "https://github.com/acme/wiki.git",
                "credentials": {
                    "auth_method": "token",
                    "token": sentinel_token,
                    "ssh_key": sentinel_key,
                },
                "default_branch": "main",
                "config": {"root_path": "docs/knowledge"},
                "project_read_only": False,
            }
        ],
        dependencies=preparation_composition.datasource_payload_dependencies(
            orchestrator.main.app.state.resources
        ),
    )

    assert result == [
        {
            "type": "kb",
            "name": "Engineering Knowledge",
            "description": "Shared architecture notes",
            "connection_url": None,
            "credentials": {},
            "project_read_only": True,
            "datasource_id": str(datasource_id),
            "config": {"root_path": "docs/knowledge"},
            "default_branch": "main",
        }
    ]
    assert sentinel_token not in repr(result)
    assert sentinel_key not in repr(result)


def test_index_status_payload_is_credential_free_and_serializable():
    now = datetime(2026, 7, 11, tzinfo=timezone.utc)
    watermark = KbWatermark(
        kb_id=UUID("11111111-2222-3333-4444-555555555555"),
        repo_name="datasource:11111111-2222-3333-4444-555555555555",
        branch="main",
        indexed_commit="a" * 40,
        source_head="b" * 40,
        pipeline_version="embed:parser:root",
        status="partial",
        last_attempt_at=now,
        last_success_at=now,
        last_error="one note failed",
        notes_done=3,
        notes_total=10,
    )

    result = index_status_payload(str(watermark.kb_id), watermark)

    assert result["status"] == "partial"
    assert result["indexed_commit"] == "a" * 40
    assert result["source_head"] == "b" * 40
    assert result["last_success_at"] == now.isoformat()
    assert result["notes_done"] == 3
    assert result["notes_total"] == 10
    assert "repo_name" not in result
    assert "credentials" not in result


@pytest.mark.asyncio
async def test_status_endpoint_uses_normal_visibility_gate():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    request = object()
    gate = AsyncMock(return_value=({}, {"id": datasource_id, "type": "kb"}))
    get_watermark = AsyncMock(return_value=None)

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore.get_watermark",
        get_watermark,
    ):
        result = await get_datasource_index_status(
            request,
            datasource_id,
            dependencies=_ds_deps(require_datasource_access=gate),
        )

    assert result["status"] == "pending"
    assert result["notes_done"] is None
    assert result["notes_total"] is None
    gate.assert_awaited_once()
    get_watermark.assert_awaited_once_with(UUID(datasource_id))


@pytest.mark.asyncio
async def test_status_endpoint_resolves_native_connector_to_project_watermark():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    project_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    request = object()
    gate = AsyncMock(
        return_value=(
            {},
            {
                "id": datasource_id,
                "type": "kb",
                "config": {"native_project_id": project_id},
            },
        )
    )
    watermark = KbWatermark(
        kb_id=UUID(project_id),
        indexed_commit="a" * 40,
        status="ready",
    )
    get_watermark = AsyncMock(return_value=watermark)

    with patch(
        "shared.runtime.services.knowledge_store.KnowledgeStore.get_watermark",
        get_watermark,
    ):
        result = await get_datasource_index_status(
            request,
            datasource_id,
            dependencies=_ds_deps(require_datasource_access=gate),
        )

    assert result["datasource_id"] == datasource_id
    assert result["status"] == "ready"
    assert result["indexed_commit"] == "a" * 40
    gate.assert_awaited_once()
    get_watermark.assert_awaited_once_with(UUID(project_id))


@pytest.mark.asyncio
async def test_manual_reindex_is_owner_gated_and_uses_stored_datasource():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    datasource = {"id": datasource_id, "type": "kb"}
    gate = AsyncMock(return_value=({}, datasource))
    run = AsyncMock(return_value={"status": "completed", "upserted": 2})

    deps = _ds_deps(require_datasource_owner=gate)
    with patch("orchestrator.services.knowledge_index.reindex_kb_datasource_now", run):
        result = await reindex_datasource_knowledge(
            object(), datasource_id, full=True, dependencies=deps
        )

    assert result["status"] == "completed"
    run.assert_awaited_once_with(
        datasource,
        force_full=True,
        dependencies=deps.operations.knowledge_index,
    )


@pytest.mark.asyncio
async def test_create_marks_pending_and_schedules_initial_full_index():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    db = MagicMock()
    db.create_datasource = AsyncMock(
        return_value={
            "id": datasource_id,
            "name": "Engineering Knowledge",
            "type": "kb",
            "credentials": {"token": "secret"},
            "config": {"root_path": "vault"},
        }
    )
    pending = AsyncMock()
    schedule = MagicMock()
    body = DatasourceCreate(
        name="Engineering Knowledge",
        type="kb",
        connection_url="https://example.test/knowledge.git",
        credentials={"token": "secret"},
        config={"root_path": "vault"},
    )

    deps = _ds_deps(
        db, require_approved_user=AsyncMock(return_value={"id": UUID(int=1)})
    )
    with (
        patch(
            "orchestrator.services.knowledge_index.mark_kb_datasource_pending", pending
        ),
        patch(
            "orchestrator.services.knowledge_index.schedule_kb_datasource_reindex",
            schedule,
        ),
    ):
        result = await create_datasource(body, object(), dependencies=deps)

    assert "credentials" not in result
    index_deps = deps.operations.knowledge_index
    pending.assert_awaited_once_with(str(datasource_id), dependencies=index_deps)
    schedule.assert_called_once_with(
        str(datasource_id), force_full=True, dependencies=index_deps
    )


@pytest.mark.asyncio
async def test_kb_create_rejects_legacy_job_id_auto_attachment():
    victim_job_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    db = MagicMock()
    db.create_datasource = AsyncMock()
    body = DatasourceCreate(
        name="Malicious Knowledge",
        type="kb",
        connection_url="https://example.test/knowledge.git",
        job_id=victim_job_id,
    )

    deps = _ds_deps(
        db,
        require_approved_user=AsyncMock(
            side_effect=AssertionError("auth ran past shape validation")
        ),
    )
    with pytest.raises(HTTPException) as exc:
        await create_datasource(body, object(), dependencies=deps)

    assert exc.value.status_code == 400
    assert "explicit connector selection" in str(exc.value.detail)
    assert victim_job_id not in str(exc.value.detail)
    db.create_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_kb_create_rejects_non_secret_config_surface():
    """V1 exposes only the typed KB root config; arbitrary datasource config
    must not become an unredacted second credentials bag."""
    db = MagicMock()
    db.create_datasource = AsyncMock()
    body = DatasourceCreate(
        name="Generic",
        type="generic",
        config={"password": "must-not-be-persisted"},
    )

    deps = _ds_deps(
        db, require_approved_user=AsyncMock(return_value={"id": UUID(int=1)})
    )
    with pytest.raises(HTTPException) as exc:
        await create_datasource(body, object(), dependencies=deps)

    assert exc.value.status_code == 400
    assert "only supported" in str(exc.value.detail)
    db.create_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_rejects_token_over_plain_http_before_persistence():
    db = MagicMock()
    db.create_datasource = AsyncMock()
    body = DatasourceCreate(
        name="Unsafe Knowledge",
        type="kb",
        connection_url="http://git.example.test/knowledge.git",
        credentials={"auth_method": "token", "token": "secret"},
    )

    deps = _ds_deps(
        db, require_approved_user=AsyncMock(return_value={"id": UUID(int=1)})
    )
    with pytest.raises(HTTPException) as exc:
        await create_datasource(body, object(), dependencies=deps)

    assert exc.value.status_code == 400
    assert "HTTPS" in str(exc.value.detail)
    db.create_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_create_rejects_untrusted_git_host_before_persistence():
    db = MagicMock()
    db.create_datasource = AsyncMock()
    body = DatasourceCreate(
        name="Untrusted Knowledge",
        type="kb",
        connection_url="https://arbitrary.example/knowledge.git",
    )

    deps = _ds_deps(
        db, require_approved_user=AsyncMock(return_value={"id": UUID(int=1)})
    )
    with pytest.raises(HTTPException) as exc:
        await create_datasource(body, object(), dependencies=deps)

    assert exc.value.status_code == 400
    assert "not trusted" in str(exc.value.detail)
    db.create_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_non_kb_update_rejects_non_secret_config_surface():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {"id": datasource_id, "type": "generic", "credentials": {}}
    db = MagicMock()
    db.update_datasource = AsyncMock()

    deps = _ds_deps(db, require_datasource_owner=AsyncMock(return_value=({}, existing)))
    with pytest.raises(HTTPException) as exc:
        await update_datasource(
            object(),
            datasource_id,
            DatasourceUpdate(config={"token": "secret"}),
            dependencies=deps,
        )

    assert exc.value.status_code == 400
    assert "only supported" in str(exc.value.detail)
    db.update_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_validates_preserved_token_against_changed_transport():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {
        "id": UUID(datasource_id),
        "type": "kb",
        "name": "Knowledge",
        "connection_url": "https://git.example.test/knowledge.git",
        "credentials": {"auth_method": "token", "token": "secret"},
        "config": {"root_path": ""},
    }
    db = MagicMock()
    db.update_datasource = AsyncMock()

    deps = _ds_deps(db, require_datasource_owner=AsyncMock(return_value=({}, existing)))
    with pytest.raises(HTTPException) as exc:
        await update_datasource(
            object(),
            datasource_id,
            DatasourceUpdate(connection_url="http://git.example.test/knowledge.git"),
            dependencies=deps,
        )

    assert exc.value.status_code == 400
    assert "HTTPS" in str(exc.value.detail)
    db.update_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_update_validates_new_token_against_preserved_plain_http_url():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {
        "id": UUID(datasource_id),
        "type": "kb",
        "name": "Knowledge",
        "connection_url": "http://git.example.test/knowledge.git",
        "credentials": {},
        "config": {"root_path": ""},
    }
    db = MagicMock()
    db.update_datasource = AsyncMock()

    deps = _ds_deps(db, require_datasource_owner=AsyncMock(return_value=({}, existing)))
    with pytest.raises(HTTPException) as exc:
        await update_datasource(
            object(),
            datasource_id,
            DatasourceUpdate(credentials={"auth_method": "token", "token": "secret"}),
            dependencies=deps,
        )

    assert exc.value.status_code == 400
    assert "HTTPS" in str(exc.value.detail)
    db.update_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_delete_uses_coordinated_kb_index_and_app_row_cleanup():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    actor_id = "99999999-8888-7777-6666-555555555555"
    db = MagicMock()
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.delete_datasource = AsyncMock(return_value=True)
    cleanup = AsyncMock(return_value=True)
    gate = AsyncMock(
        return_value=({"id": actor_id}, {"id": datasource_id, "type": "kb"})
    )

    deps = _ds_deps(db, require_datasource_owner=gate)
    with patch(
        "orchestrator.services.knowledge_index.delete_kb_datasource_with_index",
        cleanup,
    ):
        result = await delete_datasource(object(), datasource_id, dependencies=deps)

    assert result == {"status": "deleted"}
    # deleted_by is the SAME authenticated caller the non-kb branch already
    # attributes tombstones to (Task 12 item C) — the kb branch must not be
    # a silent NULL-forever exception to that.
    cleanup.assert_awaited_once_with(
        datasource_id,
        authority_project_scope_id=None,
        deleted_by=actor_id,
        dependencies=deps.operations.knowledge_index,
    )
    db.delete_datasource.assert_not_awaited()


@pytest.mark.asyncio
async def test_connectivity_probe_counts_only_indexable_markdown_notes():
    snapshot = MagicMock()
    snapshot.list_tree = AsyncMock(
        return_value=[
            {"path": "vault/index.md", "type": "blob", "sha": "a"},
            {"path": "vault/real.md", "type": "blob", "sha": "b"},
        ]
    )
    source = MagicMock()
    source.get_head = AsyncMock(return_value="c" * 40)

    @asynccontextmanager
    async def open_snapshot(_ref):
        yield snapshot

    source.snapshot = open_snapshot
    datasource = {
        "id": UUID("11111111-2222-3333-4444-555555555555"),
        "type": "kb",
        "connection_url": "https://example.test/knowledge.git",
        "config": {"root_path": "vault"},
    }

    with patch(
        "orchestrator.services.kb_datasources.kb_source_from_datasource",
        return_value=source,
    ):
        result = await probe_kb_datasource(datasource)

    assert result["status"] == "ok"
    assert result["note_count"] == 1


@pytest.mark.asyncio
async def test_external_reindex_concurrency_is_bounded_across_distinct_kbs(monkeypatch):
    import orchestrator.services.kb_datasources as service

    first_started = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    active = 0
    max_active = 0

    async def fake_reindex(**_kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        if not first_started.is_set():
            first_started.set()
            await release_first.wait()
        else:
            second_started.set()
        active -= 1
        return {"status": "completed"}

    datasource = {
        "type": "kb",
        "connection_url": "https://github.com/acme/wiki.git",
        "config": {},
    }
    monkeypatch.setattr(service, "_external_reindex_semaphore", asyncio.Semaphore(1))
    with patch.object(service, "kb_source_from_datasource", return_value=MagicMock()):
        first = asyncio.create_task(
            reindex_kb_datasource(
                {**datasource, "id": UUID(int=1)},
                store=object(),
                embedding_service=object(),
                is_active=AsyncMock(return_value=True),
                reindex_fn=fake_reindex,
            )
        )
        await first_started.wait()
        second = asyncio.create_task(
            reindex_kb_datasource(
                {**datasource, "id": UUID(int=2)},
                store=object(),
                embedding_service=object(),
                is_active=AsyncMock(return_value=True),
                reindex_fn=fake_reindex,
            )
        )
        await asyncio.sleep(0)
        assert not second_started.is_set()
        release_first.set()
        await asyncio.gather(first, second)

    assert second_started.is_set()
    assert max_active == 1


@pytest.mark.asyncio
async def test_source_construction_failure_is_recorded_on_watermark():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    datasource = {
        "id": datasource_id,
        "type": "kb",
        "connection_url": "http://git.example.test/knowledge.git",
        "credentials": {"auth_method": "token", "token": "secret"},
        "default_branch": "main",
        "config": {},
    }
    watermark = KbWatermark(kb_id=datasource_id, indexed_commit="last-good")
    store = AsyncMock()
    store.get_watermark.return_value = watermark
    runner = AsyncMock()

    with patch(
        "orchestrator.services.kb_datasources.kb_source_from_datasource",
        side_effect=ValueError("Token/password authentication requires an HTTPS URL"),
    ):
        result = await reindex_kb_datasource(
            datasource,
            store=store,
            embedding_service=object(),
            is_active=AsyncMock(return_value=True),
            reindex_fn=runner,
        )

    assert result == {
        "status": "source-failed",
        "indexed_commit": "last-good",
        "full": False,
        "upserted": 0,
        "deleted": 0,
        "skipped": 0,
        "skipped_duplicates": 0,
        "errors": 1,
    }
    runner.assert_not_awaited()
    store.set_watermark_status.assert_awaited_once_with(
        datasource_id,
        "failed",
        source_head=None,
        last_error="Token/password authentication requires an HTTPS URL",
        repo_name=f"datasource:{datasource_id}",
        branch="main",
        error_fingerprint=None,
    )


@pytest.mark.asyncio
async def test_source_construction_failure_status_write_is_best_effort():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    datasource = {
        "id": datasource_id,
        "type": "kb",
        "connection_url": "https://example.test/knowledge.git",
        "credentials": {},
        "config": {},
    }
    store = AsyncMock()
    store.get_watermark.return_value = None
    store.set_watermark_status.side_effect = RuntimeError("vector db unavailable")

    with patch(
        "orchestrator.services.kb_datasources.kb_source_from_datasource",
        side_effect=ValueError("invalid source configuration"),
    ):
        result = await reindex_kb_datasource(
            datasource,
            store=store,
            embedding_service=object(),
            is_active=AsyncMock(return_value=True),
            reindex_fn=AsyncMock(),
        )

    assert result["status"] == "source-failed"
    assert result["errors"] == 1


@pytest.mark.asyncio
async def test_source_construction_failure_does_not_resurrect_deleted_source():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    datasource = {
        "id": datasource_id,
        "type": "kb",
        "connection_url": "https://example.test/knowledge.git",
        "credentials": {},
        "config": {},
    }
    store = AsyncMock()
    is_active = AsyncMock(return_value=False)

    with patch(
        "orchestrator.services.kb_datasources.kb_source_from_datasource",
        side_effect=ValueError("invalid source configuration"),
    ):
        result = await reindex_kb_datasource(
            datasource,
            store=store,
            embedding_service=object(),
            is_active=is_active,
            reindex_fn=AsyncMock(),
        )

    assert result["status"] == "source-deleted"
    is_active.assert_awaited_once()
    store.get_watermark.assert_not_awaited()
    store.set_watermark_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_attachment_rejects_an_inaccessible_private_kb():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    owner_id = UUID(int=1)
    db = MagicMock()
    db.get_user = AsyncMock(
        return_value={"id": owner_id, "is_admin": False, "is_approved": True}
    )
    db.get_datasource_policy_rows = AsyncMock(
        return_value=[
            {
                "id": datasource_id,
                "type": "kb",
                "is_global": False,
                "created_by": UUID(int=2),
                "scope_mode": "all",
                "policy_revision": 1,
                "project_ids": [],
            }
        ]
    )

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.authorize_thread_datasource_ids(
            {"id": owner_id},
            [str(datasource_id)],
            workspace_backend="virtual",
        )

    assert exc.value.status_code == 403


@pytest.mark.asyncio
async def test_thread_attachment_allows_kb_but_not_clone_repo_on_lite_tier():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    owner_id = UUID(int=1)
    policy_row = {
        "id": datasource_id,
        "type": "kb",
        "is_global": False,
        "created_by": owner_id,
        "scope_mode": "all",
        "policy_revision": 1,
        "project_ids": [],
    }
    db = MagicMock()
    db.get_user = AsyncMock(
        return_value={"id": owner_id, "is_admin": False, "is_approved": True}
    )
    db.get_datasource_policy_rows = AsyncMock(side_effect=lambda _ids: [policy_row])

    with patch("orchestrator.main.app.state.resources.postgres_db", db):
        selected = await control_seams.authorize_thread_datasource_ids(
            {"id": owner_id},
            [str(datasource_id), str(datasource_id)],
            workspace_backend="virtual",
        )
        policy_row["type"] = "repository"
        with pytest.raises(HTTPException) as exc:
            await control_seams.authorize_thread_datasource_ids(
                {"id": owner_id},
                [str(datasource_id)],
                workspace_backend="virtual",
            )

    assert selected == [str(datasource_id)]
    assert exc.value.status_code == 400


@pytest.mark.asyncio
async def test_persisted_thread_datasource_is_denied_after_access_revocation():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    owner_id = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    db = MagicMock()
    db.get_user = AsyncMock(
        return_value={"id": owner_id, "is_admin": False, "is_approved": True}
    )
    db.get_datasource_policy_rows = AsyncMock(
        return_value=[
            {
                "id": datasource_id,
                "type": "kb",
                "is_global": False,
                "created_by": UUID(int=2),
                "scope_mode": "all",
                "policy_revision": 1,
                "project_ids": [],
            }
        ]
    )

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.revalidate_thread_datasource_selection(
            {"id": "thread-1", "user_id": owner_id},
            [str(datasource_id)],
        )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more selected connectors are unavailable"
    assert str(datasource_id) not in exc.value.detail


@pytest.mark.asyncio
async def test_persisted_thread_revalidation_preserves_global_and_system_semantics():
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    owner_id = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    db = MagicMock()
    db.get_user = AsyncMock(
        return_value={"id": owner_id, "is_admin": False, "is_approved": True}
    )
    db.get_datasource_policy_rows = AsyncMock(
        return_value=[
            {
                "id": datasource_id,
                "type": "kb",
                "is_global": True,
                "scope_mode": "all",
                "policy_revision": 1,
                "project_ids": [],
            }
        ]
    )

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_mount_rows.thread_project_ids",
            AsyncMock(return_value=[]),
        ),
    ):
        (
            global_selection,
            global_revisions,
        ) = await control_seams.revalidate_thread_datasource_selection(
            {"id": "thread-user", "user_id": owner_id},
            [str(datasource_id)],
        )
        (
            system_selection,
            system_revisions,
        ) = await control_seams.revalidate_thread_datasource_selection(
            {"id": "thread-system", "user_id": None},
            [str(datasource_id), str(datasource_id)],
        )

    assert global_selection == [str(datasource_id)]
    assert system_selection == [str(datasource_id)]
    assert global_revisions == {str(datasource_id): 1}
    assert system_revisions == {str(datasource_id): 1}
    assert db.get_user.await_count == 2
    db.get_user.assert_awaited_with(str(owner_id))


@pytest.mark.asyncio
async def test_persisted_thread_project_scope_is_denied_after_membership_revocation():
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    owner_id = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    db = MagicMock()
    db.get_user = AsyncMock(return_value={"id": owner_id, "is_admin": False})
    db.get_project = AsyncMock(return_value={"id": project_id})
    db.get_user_role_in_project = AsyncMock(return_value=None)

    with (
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        pytest.raises(HTTPException) as exc,
    ):
        await thread_project_authorization_module.revalidate_thread_project_ids(
            {"id": "thread-1", "user_id": owner_id},
            [str(project_id)],
            dependencies=sessions_composition.thread_project_authorization_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more attached projects are unavailable"
    assert str(project_id) not in exc.value.detail


@pytest.mark.asyncio
async def test_thread_creation_rejects_unavailable_project_without_enumeration():
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    owner_id = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
    user = {"id": owner_id, "is_admin": False, "settings": {}}
    db = MagicMock()
    db.get_project = AsyncMock(return_value={"id": project_id})
    db.get_user_role_in_project = AsyncMock(return_value=None)
    db.get_user_settings = AsyncMock(return_value={})
    db.create_thread = AsyncMock()

    with (
        patch(
            "orchestrator.security.auth.require_approved_user",
            AsyncMock(return_value=user),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.application.access.enforce_readiness_gate",
            AsyncMock(return_value=None),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await thread_admission_module.create_thread(
            ThreadCreateRequest(project_ids=[str(project_id)]),
            object(),
            dependencies=sessions_composition.thread_admission_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert exc.value.status_code == 403
    assert exc.value.detail == "One or more attached projects are unavailable"
    assert str(project_id) not in exc.value.detail
    db.create_thread.assert_not_awaited()


def test_project_scoped_mcp_thread_omission_binds_to_token_project():
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    user = {"scopes": [f"project:{project_id}"]}

    assert _thread_creation_project_ids(ThreadCreateRequest(), user) == [
        str(project_id)
    ]


@pytest.mark.parametrize(
    "request_body",
    [
        ThreadCreateRequest(project_ids=["88888888-2222-3333-4444-555555555555"]),
        ThreadCreateRequest(
            project_ids=[
                "99999999-2222-3333-4444-555555555555",
                "88888888-2222-3333-4444-555555555555",
            ]
        ),
        ThreadCreateRequest(
            project_id="88888888-2222-3333-4444-555555555555",
            project_ids=["99999999-2222-3333-4444-555555555555"],
        ),
    ],
)
def test_project_scoped_mcp_thread_rejects_other_or_multi_project_targets(
    request_body,
):
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    user = {"scopes": [f"project:{project_id}"]}

    with pytest.raises(HTTPException) as exc:
        _thread_creation_project_ids(request_body, user)

    assert exc.value.status_code == 403
    assert exc.value.detail == "Access denied by MCP token scope"


def test_project_scoped_mcp_thread_accepts_its_single_target():
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    user = {"scopes": [f"project:{project_id}"]}

    assert _thread_creation_project_ids(
        ThreadCreateRequest(project_ids=[str(project_id)]),
        user,
    ) == [str(project_id)]


@pytest.mark.asyncio
async def test_thread_project_authorization_preserves_admin_access():
    project_id = UUID("99999999-2222-3333-4444-555555555555")
    db = MagicMock()
    db.get_project = AsyncMock(return_value={"id": project_id})
    db.get_user_role_in_project = AsyncMock()

    with patch("orchestrator.main.app.state.resources.postgres_db", db):
        selected = await thread_project_authorization_module.authorize_thread_project_ids(
            {"id": UUID(int=1), "is_admin": True},
            [str(project_id), str(project_id)],
            dependencies=sessions_composition.thread_project_authorization_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    assert selected == [str(project_id)]
    db.get_user_role_in_project.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_revalidates_datasources_before_mutating_thread_status():
    """resume_thread must not flip a thread's status before its config drift
    is resolved.

    Round-1 diagnosis (this test regressed under Task 6, commit a6e073e3):
    this pinned a synchronous 403 raised by the old
    ``_revalidate_thread_datasource_ids`` helper — one ``resume_thread`` no
    longer called, and which Task 13 later deleted outright once its only
    remaining callers (direct tests) were redirected to the selection
    function it had wrapped, ``_revalidate_thread_datasource_selection``.
    Task 6 (knowledge-history/done/session_config_drift_resume.md) deliberately
    replaced that dead-end 403 with an acknowledgeable 428 listing every
    drifted item (a revoked/deleted datasource no longer permanently
    strands the session) — an intentional, already-shipped contract
    change, not a bug. This is a
    genuine (ii): the old mock (`user={}`, the old helper patched) doesn't
    match resume_thread's real current collaborator (`_thread_config_drift`),
    which is why it crashed with a raw ``KeyError`` rather than failing its
    assertion. Classification correctness (deleted/revoked/out_of_scope) is
    unit tested on its own in tests/test_config_drift.py and
    tests/test_resume_config_drift.py; this test only pins resume_thread's
    ordering guarantee, unchanged: no status mutation before drift resolves.
    """
    datasource_id = UUID("11111111-2222-3333-4444-555555555555")
    thread = {
        "id": "thread-1",
        "execution_lane": "pinned",
        "user_id": UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"),
        "status": "ended",
        "metadata": {"datasource_ids": [str(datasource_id)]},
    }
    user = {"id": str(thread["user_id"])}
    db = MagicMock()
    db.resume_thread = AsyncMock()
    db.record_thread_config_drift_ack = AsyncMock()
    # resume_thread resolves drift as the THREAD OWNER, not the caller (a
    # caller can be an admin acting on someone else's thread) — it reads the
    # owner row via get_user(thread["user_id"]) before calling
    # _thread_config_drift. Here the caller already stands in as the owner
    # (same id), so the same dict is the right stand-in for that row.
    db.get_user = AsyncMock(return_value=user)
    drift = [DriftItem(f"connector:{datasource_id}", "connector", "deleted", "gone")]

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=drift),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.resume_thread("thread-1", object())

    assert exc.value.status_code == 428
    assert exc.value.detail["drift"][0]["id"] == f"connector:{datasource_id}"
    db.resume_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_resume_blocks_revoked_native_project_scope_before_status_mutation():
    """Same ordering guarantee as the datasource test above, for a revoked
    project membership.

    Round-1 diagnosis: same (ii) as above — a synchronous 403 from
    ``_revalidate_thread_project_ids`` is now an acknowledgeable 428 (Task 6).
    The old ``_revalidate_thread_datasource_ids`` was never on
    resume_thread's call path either (``_thread_config_drift`` computes
    drift directly), so a mock of it proved nothing here and was dropped
    rather than kept as dead weight; Task 13 later deleted the function
    itself, its last callers having been direct tests in
    tests/test_kb_datasource_api.py, since redirected to
    ``_revalidate_thread_datasource_selection``.
    """
    project_id = "99999999-2222-3333-4444-555555555555"
    thread = {
        "id": "thread-1",
        "execution_lane": "pinned",
        "user_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        "status": "ended",
        "metadata": {},
    }
    user = {"id": thread["user_id"]}
    db = MagicMock()
    db.resume_thread = AsyncMock()
    db.record_thread_config_drift_ack = AsyncMock()
    # See the datasource test above: resume_thread now reads the owner row
    # via get_user(thread["user_id"]) before computing drift. The caller
    # already stands in as the owner here (same id).
    db.get_user = AsyncMock(return_value=user)
    drift = [DriftItem(f"project:{project_id}", "project", "revoked", "gone")]

    with (
        patch(
            "orchestrator.security.access.require_thread_owner",
            AsyncMock(return_value=(user, thread)),
        ),
        patch("orchestrator.main.app.state.resources.postgres_db", db),
        patch(
            "orchestrator.services.thread_resume.thread_config_drift",
            AsyncMock(return_value=drift),
        ),
        pytest.raises(HTTPException) as exc,
    ):
        await control_seams.resume_thread("thread-1", object())

    assert exc.value.status_code == 428
    assert exc.value.detail["drift"][0]["id"] == f"project:{project_id}"
    db.resume_thread.assert_not_awaited()


@pytest.mark.asyncio
async def test_thread_kb_credential_gate_is_narrow():
    db = MagicMock()
    db.get_datasource = AsyncMock(
        side_effect=[
            {"id": UUID(int=1), "type": "postgres"},
            {"id": UUID(int=2), "type": "kb"},
        ]
    )

    with patch("orchestrator.main.app.state.resources.postgres_db", db):
        assert not await thread_project_authorization_module.thread_has_knowledge_scope(
            project_ids=[],
            datasource_ids=[str(UUID(int=1))],
            dependencies=sessions_composition.thread_project_authorization_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert await thread_project_authorization_module.thread_has_knowledge_scope(
            project_ids=[],
            datasource_ids=[str(UUID(int=2))],
            dependencies=sessions_composition.thread_project_authorization_dependencies(
                orchestrator.main.app.state.resources
            ),
        )
        assert await thread_project_authorization_module.thread_has_knowledge_scope(
            project_ids=[str(UUID(int=3))],
            datasource_ids=[],
            dependencies=sessions_composition.thread_project_authorization_dependencies(
                orchestrator.main.app.state.resources
            ),
        )

    # Native project scope short-circuits without an unnecessary datasource read.
    assert db.get_datasource.await_count == 2


@pytest.mark.asyncio
async def test_metadata_only_kb_edit_does_not_schedule_full_rebuild():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {
        "id": datasource_id,
        "type": "kb",
        "name": "Old Name",
        "connection_url": "https://example.test/knowledge.git",
        "credentials": {"token": "stored"},
        "default_branch": "main",
        "config": {"root_path": "vault"},
    }
    db = MagicMock()
    db.update_datasource = AsyncMock(return_value=True)
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.get_datasource = AsyncMock(return_value=existing)
    pending = AsyncMock()
    schedule = MagicMock()

    deps = _ds_deps(db, require_datasource_owner=AsyncMock(return_value=({}, existing)))
    with (
        patch(
            "orchestrator.services.knowledge_index.mark_kb_datasource_pending", pending
        ),
        patch(
            "orchestrator.services.knowledge_index.schedule_kb_datasource_reindex",
            schedule,
        ),
    ):
        result = await update_datasource(
            object(),
            datasource_id,
            DatasourceUpdate(
                name="New Name",
                connection_url=existing["connection_url"],
                default_branch="main",
                config={"root_path": "vault"},
            ),
            dependencies=deps,
        )

    assert result["id"] == datasource_id
    pending.assert_not_awaited()
    schedule.assert_not_called()


@pytest.mark.asyncio
async def test_root_change_schedules_full_rebuild():
    datasource_id = "11111111-2222-3333-4444-555555555555"
    existing = {
        "id": datasource_id,
        "type": "kb",
        "connection_url": "https://example.test/knowledge.git",
        "credentials": {},
        "default_branch": "main",
        "config": {"root_path": "vault"},
    }
    db = MagicMock()
    db.update_datasource = AsyncMock(return_value=True)
    db.list_datasource_projects = AsyncMock(return_value=[])
    db.get_datasource = AsyncMock(return_value=existing)
    pending = AsyncMock()
    schedule = MagicMock()

    deps = _ds_deps(db, require_datasource_owner=AsyncMock(return_value=({}, existing)))
    with (
        patch(
            "orchestrator.services.knowledge_index.mark_kb_datasource_pending", pending
        ),
        patch(
            "orchestrator.services.knowledge_index.schedule_kb_datasource_reindex",
            schedule,
        ),
    ):
        await update_datasource(
            object(),
            datasource_id,
            DatasourceUpdate(config={"root_path": "handbook"}),
            dependencies=deps,
        )

    index_deps = deps.operations.knowledge_index
    pending.assert_awaited_once_with(datasource_id, dependencies=index_deps)
    schedule.assert_called_once_with(
        datasource_id, force_full=True, dependencies=index_deps
    )
