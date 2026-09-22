"""Catalogue HTTP contracts characterized before R1.B01 extraction."""

from copy import deepcopy
from dataclasses import replace
from functools import partial
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID
from zipfile import ZipFile

import pytest
from fastapi.testclient import TestClient

from tests._mounted_router import mount_router
from orchestrator.schemas.expert_catalog import ExpertInfo


USER_ID = "00000000-0000-0000-0000-000000000101"
EXPERT_ID = "00000000-0000-0000-0000-000000000201"
SKILL_ID = "00000000-0000-0000-0000-000000000301"
PROJECT_ID = "00000000-0000-0000-0000-000000000401"
SKILL_TEXT = "---\nname: quiet-helper\ndescription: A local helper.\n---\n# Body\n\nKeep this spacing.\n"


@pytest.fixture
def catalogue_wire(monkeypatch):
    from orchestrator import main
    from orchestrator.routers.expert_catalog import (
        ExpertCatalogRouteDependencies,
        router,
    )
    from orchestrator.services import expert_authoring
    from orchestrator.services.expert_catalog import ExpertCatalogService
    from orchestrator.services.expert_catalog_contracts import (
        ExpertCatalogDependencies,
        ExpertCatalogState,
        ExpertWritePolicy,
    )

    calls = []
    user = {"id": USER_ID, "is_admin": False}

    async def approved(request):
        calls.append("approved")
        return user

    async def owner(request, project_id, *, allow_archived=True):
        calls.append(("owner", allow_archived))
        return user, {"id": project_id}

    async def member(request, project_id, *, allow_archived=True):
        calls.append("member")
        return user, {"id": project_id}

    async def prelude(*_args, **_kwargs):
        calls.append("save-prelude")

    async def save(*_args, **_kwargs):
        calls.append("save")

    store = SimpleNamespace(
        list_experts_visible=AsyncMock(return_value=[]),
        get_expert_visible_by_id=AsyncMock(return_value=None),
        get_expert_by_id=AsyncMock(return_value=None),
        create_expert=AsyncMock(),
        update_expert=AsyncMock(),
        delete_expert=AsyncMock(),
        expert_delete_blockers=AsyncMock(return_value=[]),
        get_skill_by_id=AsyncMock(return_value=None),
        get_skill_files=AsyncMock(return_value={}),
        create_skill=AsyncMock(),
        update_skill=AsyncMock(),
        delete_skill=AsyncMock(),
        list_project_linked_experts=AsyncMock(return_value=[]),
        clear_user_expert_default=AsyncMock(return_value=True),
        get_application_expert_default=AsyncMock(return_value=None),
        set_user_expert_default=AsyncMock(),
        clear_project_default_expert=AsyncMock(return_value=True),
        set_project_default_expert=AsyncMock(),
    )
    settings = SimpleNamespace(experts_enabled=True, skills_enabled=True)
    state = ExpertCatalogState(experts=[], library=[], skills=[])
    catalog = ExpertCatalogService(
        ExpertCatalogDependencies(
            store=store,
            state=state,
            get_config_dir=main._get_config_dir,
            load_settings_matrix=main.app.state.catalogue_resources.load_settings_matrix,
            experts_enabled=lambda: settings.experts_enabled,
            skills_enabled=lambda: settings.skills_enabled,
            account_defaults_layer=AsyncMock(return_value={}),
            visible_project_ids=AsyncMock(return_value=[]),
            with_validated_tool_overrides=main._with_validated_tool_overrides,
            looks_like_uuid=main._looks_like_uuid,
            forge=None,
        )
    )
    authoring = expert_authoring.ExpertAuthoringService(
        store=store,
        catalog=catalog,
        resolve_default_models=AsyncMock(return_value={}),
        prefetch_roster_refs=AsyncMock(return_value={}),
    )
    defaults_policy = AsyncMock(return_value=True)
    monkeypatch.setattr(expert_authoring, "personal_defaults_allowed", defaults_policy)
    deps = ExpertCatalogRouteDependencies(
        catalog=catalog,
        authoring=authoring,
        require_approved_user=approved,
        require_admin=approved,
        require_project_member=member,
        require_project_owner=owner,
        write_policy_factory=lambda request: ExpertWritePolicy(
            enforce_save=partial(save, request),
            enforce_save_prelude=partial(prelude, request),
            strip_save_grants=AsyncMock(),
        ),
    )
    app = mount_router(
        router, factories={"expert_catalog_dependencies_factory": lambda: deps}
    )
    with TestClient(app, raise_server_exceptions=True) as client:
        yield SimpleNamespace(
            client=client,
            store=store,
            user=user,
            calls=calls,
            state=state,
            settings=settings,
            defaults_policy=defaults_policy,
            catalog=catalog,
            authoring=authoring,
            app=app,
            deps=deps,
        )


def expert_row(**extra):
    return {
        "id": UUID(EXPERT_ID),
        "name": "quiet-worker",
        "display_name": "Quiet worker",
        "expert_type": "worker",
        "owner_id": UUID(USER_ID),
        "is_global": False,
        "icon": "smart_toy",
        "color": "#6B7280",
        "config": {},
        "prompts": {},
        **extra,
    }


@pytest.mark.parametrize(
    ("path", "payload", "flag"),
    [
        (
            "/api/experts",
            {"name": "quiet", "display_name": "Quiet", "expert_type": "worker"},
            "experts_enabled",
        ),
        ("/api/skills", {"files": {"SKILL.md": SKILL_TEXT}}, "skills_enabled"),
    ],
)
def test_disabled_authoring_precedes_identity(
    catalogue_wire, monkeypatch, path, payload, flag
):
    env = catalogue_wire
    monkeypatch.setattr(env.settings, flag, False)
    response = env.client.post(path, json=payload)
    assert response.status_code == 404
    assert env.calls == []
    env.store.create_expert.assert_not_awaited()
    env.store.create_skill.assert_not_awaited()


def test_managed_expert_delete_refusal_precedes_owner_refusal(catalogue_wire):
    env = catalogue_wire
    env.store.get_expert_by_id.return_value = expert_row(
        owner_id="someone-else", managed_key="application-worker"
    )
    response = env.client.delete(f"/api/experts/{EXPERT_ID}")
    assert response.status_code == 409
    assert "Managed platform experts" in response.json()["detail"]
    env.store.expert_delete_blockers.assert_not_awaited()
    env.store.delete_expert.assert_not_awaited()


def test_expert_owner_refusal_precedes_config_policy(catalogue_wire):
    env = catalogue_wire
    env.store.get_expert_by_id.return_value = expert_row(owner_id="someone-else")
    response = env.client.put(
        f"/api/experts/{EXPERT_ID}", json={"config": {"llm": {"api_key": "test-only"}}}
    )
    assert response.status_code == 403
    assert env.calls == ["approved"]
    env.store.update_expert.assert_not_awaited()


def test_expert_tag_update_preserves_role_and_unset_config(catalogue_wire):
    env = catalogue_wire
    env.store.get_expert_by_id.return_value = expert_row()
    env.store.update_expert.return_value = expert_row(tags=["research", "worker"])
    response = env.client.put(f"/api/experts/{EXPERT_ID}", json={"tags": ["research"]})
    assert response.status_code == 200
    kwargs = env.store.update_expert.await_args.kwargs
    assert set(kwargs["tags"]) == {"worker", "research"}
    assert "config" not in kwargs
    assert "prompts" not in kwargs
    assert response.json()["id"] == EXPERT_ID
    assert env.calls == ["approved", "save"]


def test_duplicate_rejects_legacy_credentials_before_save_prelude(catalogue_wire):
    env = catalogue_wire
    env.store.get_expert_visible_by_id.return_value = expert_row(
        owner_id="another-author", config={"llm": {"api_key": "test-only"}}
    )
    response = env.client.post(f"/api/experts/{EXPERT_ID}/duplicate")
    assert response.status_code == 422
    assert env.calls == ["approved"]
    env.store.create_expert.assert_not_awaited()


def test_default_clear_still_works_after_personal_default_grant_revoked(catalogue_wire):
    env = catalogue_wire
    env.defaults_policy.return_value = False
    response = env.client.delete("/api/expert-defaults/worker")
    assert response.status_code == 200
    assert response.json() == {
        "deleted": True,
        "default": None,
        "source": "application",
    }
    env.defaults_policy.assert_not_awaited()
    env.store.clear_user_expert_default.assert_awaited_once_with(
        user_id=USER_ID, expert_type="worker"
    )


def test_default_selection_denial_does_not_write(catalogue_wire):
    env = catalogue_wire
    env.defaults_policy.return_value = False
    response = env.client.put(
        "/api/expert-defaults/worker", json={"expert_id": EXPERT_ID}
    )
    assert response.status_code == 403
    env.store.set_user_expert_default.assert_not_awaited()


def test_default_fork_denial_precedes_source_lookup_and_authoring_gate(catalogue_wire):
    env = catalogue_wire
    env.defaults_policy.return_value = False
    response = env.client.post(
        "/api/expert-defaults/worker/fork", json={"expert_id": "missing-source"}
    )
    assert response.status_code == 403
    assert env.calls == ["approved"]
    env.store.get_expert_by_id.assert_not_awaited()


def test_project_default_set_and_clear_keep_distinct_archive_policy(catalogue_wire):
    env = catalogue_wire
    set_response = env.client.put(
        f"/api/projects/{PROJECT_ID}/expert-defaults/worker",
        json={"expert_id": EXPERT_ID},
    )
    assert set_response.status_code == 404
    clear_response = env.client.delete(
        f"/api/projects/{PROJECT_ID}/expert-defaults/worker"
    )
    assert clear_response.status_code == 200
    assert env.calls == [("owner", False), ("owner", True)]


def test_project_experts_db_links_precede_forge_and_keep_bare_array(
    catalogue_wire, monkeypatch
):
    env = catalogue_wire
    env.store.list_project_linked_experts.return_value = [
        expert_row(project_config_override={"temperature": 0}, tags=["worker"])
    ]
    response = env.client.get(f"/api/projects/{PROJECT_ID}/experts")
    assert response.status_code == 200
    body = response.json()
    assert isinstance(body, list)
    assert body[0]["id"] == EXPERT_ID
    assert env.calls == ["member"]


def test_expert_listing_role_or_tag_filter_and_managed_shadow(
    catalogue_wire, monkeypatch
):
    env = catalogue_wire
    monkeypatch.setattr(
        env.state,
        "experts",
        [
            ExpertInfo(
                id="quiet-worker",
                display_name="Bundled",
                description="",
                tags=["worker", "session"],
            ),
            ExpertInfo(
                id="other", display_name="Other", description="", tags=["session"]
            ),
        ],
    )
    env.store.list_experts_visible.return_value = [
        expert_row(tags=["session"], managed_key="application-worker")
    ]
    response = env.client.get("/api/experts?type=session")
    assert response.status_code == 200
    body = response.json()
    assert [row["id"] for row in body] == ["other", EXPERT_ID]
    assert env.store.list_experts_visible.await_args.kwargs["expert_type"] is None


def test_skill_import_then_export_preserves_archive_payload(catalogue_wire):
    env = catalogue_wire
    archive = BytesIO()
    with ZipFile(archive, "w") as packed:
        packed.writestr("quiet-helper/SKILL.md", SKILL_TEXT)
        packed.writestr("quiet-helper/references/example.txt", "local fixture\n")
    files = {"SKILL.md": SKILL_TEXT, "references/example.txt": "local fixture\n"}
    env.store.create_skill.return_value = {"id": SKILL_ID, "name": "quiet-helper"}
    imported = env.client.post(
        "/api/skills/import",
        files={"file": ("quiet-helper.zip", archive.getvalue(), "application/zip")},
    )
    assert imported.status_code == 200
    assert env.store.create_skill.await_args.kwargs["files"] == files
    env.store.get_skill_by_id.return_value = {
        "id": SKILL_ID,
        "name": "quiet-helper",
        "owner_id": USER_ID,
    }
    env.store.get_skill_files.return_value = deepcopy(files)
    exported = env.client.get(f"/api/skills/{SKILL_ID}/export")
    assert exported.status_code == 200
    assert exported.headers["content-type"] == "application/zip"
    assert (
        exported.headers["content-disposition"]
        == 'attachment; filename="quiet-helper.zip"'
    )
    with ZipFile(BytesIO(exported.content)) as packed:
        assert packed.read("quiet-helper/SKILL.md").decode() == SKILL_TEXT
        assert (
            packed.read("quiet-helper/references/example.txt").decode()
            == "local fixture\n"
        )


def test_skill_bad_archive_is_422_after_identity_without_write(catalogue_wire):
    env = catalogue_wire
    response = env.client.post(
        "/api/skills/import",
        files={"file": ("broken.zip", b"not a zip", "application/zip")},
    )
    assert response.status_code == 422
    assert env.calls == ["approved"]
    env.store.create_skill.assert_not_awaited()


def test_skill_owner_refusal_precedes_bundle_parse(catalogue_wire):
    env = catalogue_wire
    # Global, so visible to the caller: the refusal under test is ownership.
    env.store.get_skill_by_id.return_value = {
        "id": SKILL_ID,
        "owner_id": "other",
        "is_global": True,
        "name": "quiet-helper",
    }
    response = env.client.put(
        f"/api/skills/{SKILL_ID}", json={"files": {"../escape": "ignored"}}
    )
    assert response.status_code == 403
    env.store.update_skill.assert_not_awaited()


OTHER_USER_ID = "00000000-0000-0000-0000-000000000102"
SKILL_FILES = {"SKILL.md": SKILL_TEXT, "references/private.md": "owner's notes\n"}


def skill_row(**extra):
    return {
        "id": UUID(SKILL_ID),
        "name": "quiet-helper",
        "display_name": "Quiet helper",
        "description": "A local helper.",
        "owner_id": UUID(OTHER_USER_ID),
        "is_global": False,
        "icon": "extension",
        "color": "#6B7280",
        "tags": [],
        **extra,
    }


class TestSkillByIdVisibility:
    """A DB skill read by UUID follows the listing's visibility rule (owned or
    global) plus admins; anything else is 404, the way experts-by-id are. Before
    this, every by-id read served any row to any approved caller who held the
    UUID — cookie sessions and tokens alike."""

    @pytest.fixture
    def env(self, catalogue_wire):
        catalogue_wire.store.get_skill_by_id.return_value = skill_row()
        catalogue_wire.store.get_skill_files.return_value = deepcopy(SKILL_FILES)
        return catalogue_wire

    @pytest.mark.parametrize(
        ("method", "suffix"),
        [
            ("get", ""),
            ("get", "/export"),
            ("post", "/duplicate"),
            ("put", ""),
            ("delete", ""),
        ],
    )
    def test_another_users_private_skill_is_404_on_every_by_id_route(
        self, env, method, suffix
    ):
        kwargs = {"json": {"description": "hijack"}} if method == "put" else {}
        response = getattr(env.client, method)(
            f"/api/skills/{SKILL_ID}{suffix}", **kwargs
        )
        assert response.status_code == 404
        assert "owner's notes" not in response.text
        env.store.get_skill_files.assert_not_awaited()
        env.store.create_skill.assert_not_awaited()
        env.store.update_skill.assert_not_awaited()
        env.store.delete_skill.assert_not_awaited()

    def test_owner_reads_and_exports_own_private_skill(self, env):
        env.store.get_skill_by_id.return_value = skill_row(owner_id=UUID(USER_ID))
        detail = env.client.get(f"/api/skills/{SKILL_ID}")
        assert detail.status_code == 200
        assert detail.json()["files"] == SKILL_FILES
        assert detail.json()["source"] == "user"
        exported = env.client.get(f"/api/skills/{SKILL_ID}/export")
        assert exported.status_code == 200
        with ZipFile(BytesIO(exported.content)) as packed:
            assert (
                packed.read("quiet-helper/references/private.md") == b"owner's notes\n"
            )

    def test_admin_reads_another_users_private_skill(self, env):
        env.user["is_admin"] = True
        assert env.client.get(f"/api/skills/{SKILL_ID}").status_code == 200
        assert env.client.get(f"/api/skills/{SKILL_ID}/export").status_code == 200

    def test_global_skill_stays_visible_but_not_editable(self, env):
        env.store.get_skill_by_id.return_value = skill_row(is_global=True)
        detail = env.client.get(f"/api/skills/{SKILL_ID}")
        assert detail.status_code == 200
        assert detail.json()["source"] == "global"
        assert env.client.get(f"/api/skills/{SKILL_ID}/export").status_code == 200
        # Visible, so the refusal is the ownership one, not a 404.
        response = env.client.put(
            f"/api/skills/{SKILL_ID}", json={"description": "hijack"}
        )
        assert response.status_code == 403
        env.store.update_skill.assert_not_awaited()

    def test_bundled_skill_stays_visible(self, env):
        detail = env.client.get("/api/skills/word-count")
        assert detail.status_code == 200
        assert detail.json()["source"] == "bundled"
        assert env.client.get("/api/skills/word-count/export").status_code == 200
        env.store.get_skill_by_id.assert_not_awaited()


def test_two_mounted_apps_keep_store_identity_and_reload_cache_separate(
    catalogue_wire, monkeypatch
):
    from orchestrator.routers.expert_catalog import router
    from orchestrator.services.expert_authoring import ExpertAuthoringService
    from orchestrator.services.expert_catalog import ExpertCatalogService
    from orchestrator.services.expert_catalog_contracts import ExpertCatalogState

    env = catalogue_wire
    other_id = "00000000-0000-0000-0000-000000000102"
    other_store = SimpleNamespace(
        list_experts_visible=AsyncMock(return_value=[]),
        create_skill=AsyncMock(return_value={"id": SKILL_ID}),
    )
    other_state = ExpertCatalogState(
        experts=[ExpertInfo(id="second-only", display_name="Second", description="")],
        library=[],
    )
    other_catalog = ExpertCatalogService(
        replace(env.catalog.deps, store=other_store, state=other_state)
    )
    other_deps = replace(
        env.deps,
        catalog=other_catalog,
        authoring=ExpertAuthoringService(
            store=other_store,
            catalog=other_catalog,
            resolve_default_models=env.authoring.resolve_default_models,
            prefetch_roster_refs=env.authoring.prefetch_roster_refs,
        ),
        require_approved_user=AsyncMock(return_value={"id": other_id}),
        require_admin=AsyncMock(return_value={"id": other_id, "is_admin": True}),
    )
    other_app = mount_router(
        router, factories={"expert_catalog_dependencies_factory": lambda: other_deps}
    )
    env.state.experts = [
        ExpertInfo(id="first-only", display_name="First", description="")
    ]
    monkeypatch.setattr(other_catalog, "scan_experts", lambda: [])
    monkeypatch.setattr(other_catalog, "scan_subagent_library", lambda: [])
    with TestClient(other_app) as other_client:
        assert [r["id"] for r in other_client.get("/api/experts").json()] == [
            "second-only"
        ]
        assert [r["id"] for r in env.client.get("/api/experts").json()] == [
            "first-only"
        ]
        response = other_client.post("/api/experts/reload")
        assert response.status_code == 200
        assert response.json() == {"status": "reloaded", "count": 0}
        assert (
            other_client.post(
                "/api/skills", json={"files": {"SKILL.md": SKILL_TEXT}}
            ).status_code
            == 200
        )
        assert [r["id"] for r in env.client.get("/api/experts").json()] == [
            "first-only"
        ]
    assert other_store.list_experts_visible.await_args.kwargs["user_id"] == other_id
    assert env.store.list_experts_visible.await_args.kwargs["user_id"] == USER_ID
    assert other_store.create_skill.await_args.kwargs["owner_id"] == other_id
    env.store.create_skill.assert_not_awaited()
    assert other_state.experts == []
    assert env.state.experts[0].id == "first-only"


def test_router_without_factory_cannot_fall_back_to_the_main_app(catalogue_wire):
    from orchestrator.routers.expert_catalog import router

    env = catalogue_wire
    with TestClient(mount_router(router), raise_server_exceptions=False) as client:
        response = client.get("/api/experts")
    assert response.status_code == 500
    assert env.calls == []
    env.store.list_experts_visible.assert_not_awaited()
