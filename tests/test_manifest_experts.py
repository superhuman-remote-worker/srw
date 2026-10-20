"""Canonical Expert ownership, payload migration and reference-adapter isolation."""

from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from orchestrator.services import manifest_experts as module
from shared.manifests import parse_documents, preview_documents
from shared.manifests.resolution import content_revision
from shared.runtime.core.srw_manifest_config import read_srw_config, srw_private_config

USER = UUID("00000000-0000-0000-0000-000000000101")
EXPERT = UUID("00000000-0000-0000-0000-000000000201")
ROOT = Path(__file__).resolve().parents[1]


def expert_row(**changes):
    return {
        "id": EXPERT,
        "name": "helper",
        "display_name": "Helper",
        "expert_type": "worker",
        "description": "An existing helper",
        "icon": "code",
        "color": "#89b4fa",
        "tags": ["worker", "custom"],
        "owner_id": USER,
        "is_global": True,
        "version": 7,
        "config": {
            "tools": {"custom": ["read_a_bit_differently"]},
            "llm": {"temperature": None},
        },
        "prompts": {"persona": "Keep the authored persona."},
        **changes,
    }


def saved_resource(document, *, linked_id=EXPERT):
    resolved = preview_documents([document])["resolved"][0]
    return {
        "id": uuid4(),
        "kind": "Expert",
        "linked_id": linked_id,
        "document": deepcopy(document),
        "resolved": resolved,
        "revision": content_revision(resolved["spec"]),
        "resource_version": 1,
    }


class MemoryStore:
    def __init__(self, db):
        self.db = db

    async def lock_catalog(self):
        assert self.db.in_transaction
        self.db.events.append("catalog-lock")

    async def lock_identity(self, document):
        assert self.db.in_transaction
        self.db.events.append("identity-lock")

    async def by_link(self, kind, linked_id):
        return deepcopy(self.db.resources.get(str(linked_id)))

    async def by_name(self, kind, scope, name):
        return next(
            (
                deepcopy(r)
                for r in self.db.resources.values()
                if r["document"]["metadata"]["scope"] == scope
                and r["document"]["metadata"]["name"] == name
            ),
            None,
        )

    async def save(self, document, resolved, revision, dependencies, **options):
        assert self.db.in_transaction
        assert self.db.events[-1] == "identity-lock"
        self.db.events.append("resource-save")
        previous = self.db.resources.get(str(options.get("linked_id")))
        if previous:
            assert options["expected_version"] == previous["resource_version"]
        row = saved_resource(document, linked_id=options.get("linked_id"))
        if previous:
            row["id"] = previous["id"]
            row["resource_version"] += previous["resource_version"]
        self.db.resources[str(options.get("linked_id", row["id"]))] = row
        return deepcopy(row), True


class MemoryDatabase:
    def __init__(self, rows):
        self.rows = deepcopy(rows)
        self.resources = {}
        self.events = []
        self.in_transaction = False
        self.default_links = {"application": EXPERT, "project": EXPERT}

    @asynccontextmanager
    async def transaction_scope(self):
        original = deepcopy((self.rows, self.resources, self.default_links))
        self.in_transaction = True
        try:
            yield self
        except BaseException:
            self.rows, self.resources, self.default_links = original
            raise
        finally:
            self.in_transaction = False

    async def fetch(self, query, *args):
        if "FROM experts" in query:
            return deepcopy(self.rows)
        assert "FROM srw_resources" in query
        return deepcopy(list(self.resources.values()))

    async def execute(self, query, *args):
        assert self.in_transaction
        assert query.startswith("UPDATE experts SET config=")
        assert self.events[-1] == "resource-save"
        self.events.append("clear-old-payload")
        row = next(r for r in self.rows if r["id"] == args[0])
        row.update(config={}, prompts={}, manifest_resource_id=args[1])


@pytest.fixture
def memory_store(monkeypatch):
    monkeypatch.setattr(module, "ManifestStore", MemoryStore)


def test_expert_round_trip_keeps_private_values_and_independent_authority():
    row = expert_row()
    document = module.expert_manifest(row, image="trusted/srw:fixed")
    preview_documents([document])
    private = srw_private_config(document)
    assert private["config"] == row["config"]
    assert private["prompts"] == row["prompts"]
    assert private["config"]["llm"]["temperature"] is None
    projected = module.project_expert_resource(row, saved_resource(document))
    for key in ("id", "owner_id", "is_global", "version", "config", "prompts"):
        assert projected[key] == row[key]
    projected["config"]["tools"]["custom"].append("changed")
    assert document["spec"]["runtime"]["config"]["config"] == row["config"]


@pytest.mark.parametrize("name", ["old_helper", "a" * 100])
def test_legacy_names_gain_deterministic_noncolliding_manifest_names(name):
    one = module.expert_manifest(expert_row(name=name))
    two = module.expert_manifest(
        expert_row(name=name, id=UUID("10000000-0000-0000-0000-000000000201"))
    )
    preview_documents([one, two])
    assert len(one["metadata"]["name"]) <= 63
    assert one["metadata"]["name"] != two["metadata"]["name"]
    assert one["metadata"]["annotations"]["srw.io/legacy-name"] == name


def test_generic_projection_never_claims_reference_harness_settings():
    row = expert_row()
    document = module.expert_manifest(row)
    runtime = document["spec"]["runtime"]
    runtime.pop("adapter")
    runtime["image"] = "custom/worker:1"
    runtime["config"] = {"tools": {"read_file": None}, "arbitrary": [True, None]}
    projected = module.project_expert_resource(row, saved_resource(document))
    assert projected["harness_adapter"] is None
    assert projected["config"] == projected["prompts"] == {}
    assert projected["manifest"]["spec"]["runtime"]["config"] == runtime["config"]
    with pytest.raises(ValueError, match="does not select"):
        srw_private_config(document)
    from orchestrator.services.config_resolver import resolve_config

    with pytest.raises(ValueError, match="Generic Experts"):
        resolve_config(base_config_name="worker_base", expert_row=projected)


def test_all_bundled_definitions_are_valid_manifests_with_same_private_leaf():
    paths = sorted(ROOT.glob("config/experts/*/config.yaml")) + sorted(
        ROOT.glob("config/subagents/*/config.yaml")
    )
    assert len(paths) >= 20
    documents = []
    for path in paths:
        (document,) = parse_documents(path.read_text())
        assert "image" not in document["spec"]["runtime"]
        documents.append(document)
        private = srw_private_config(document)
        assert private["config"] == read_srw_config(path)
        assert private["config"]["agent_id"]
        assert private["config_name"] == private["config"]["$extends"]
        assert (
            private["asset_name"]
            == ("subagents/" if path.parent.parent.name == "subagents" else "")
            + path.parent.name
        )
    preview_documents(documents)


@pytest.mark.asyncio
async def test_migration_moves_payload_once_and_preserves_links_and_operator_edits(
    memory_store,
):
    original = expert_row()
    db = MemoryDatabase([original])
    links = deepcopy(db.default_links)
    assert await module.migrate_stored_experts(db, image="trusted/srw:v1") == {
        "migrated": 1,
        "preserved": 0,
    }
    assert db.events == [
        "catalog-lock",
        "identity-lock",
        "resource-save",
        "clear-old-payload",
    ]
    assert db.rows[0]["config"] == db.rows[0]["prompts"] == {}
    assert db.rows[0]["id"] == original["id"]
    assert db.rows[0]["version"] == original["version"]
    assert db.default_links == links
    projected = await module.hydrate_expert_row(db, db.rows[0])
    assert projected["config"] == original["config"]
    assert projected["prompts"] == original["prompts"]
    assert projected["owner_id"] == original["owner_id"] and projected["is_global"]
    resource = db.resources[str(EXPERT)]
    resource["document"]["spec"]["runtime"]["config"]["config"]["operator"] = "new"
    assert await module.migrate_stored_experts(db, image="trusted/srw:v2") == {
        "migrated": 0,
        "preserved": 1,
    }
    assert db.resources[str(EXPERT)] == resource


@pytest.mark.asyncio
async def test_migration_rolls_back_earlier_rows_if_later_conversion_fails(
    memory_store,
):
    rows = [
        expert_row(),
        expert_row(id=uuid4(), name="broken", config=["invalid-object"]),
    ]
    db = MemoryDatabase(rows)
    with pytest.raises(ValueError, match="JSON objects"):
        await module.migrate_stored_experts(db)
    assert db.rows == rows
    assert db.resources == {}


@pytest.mark.asyncio
async def test_deleted_resource_does_not_resurrect_empty_legacy_defaults(memory_store):
    row = expert_row(config={}, prompts={}, manifest_resource_id=uuid4())
    db = MemoryDatabase([row])
    assert await module.hydrate_expert_row(db, row) is None
    assert await module.hydrate_expert_rows(db, [row]) == []


@pytest.mark.asyncio
async def test_dual_payload_migration_refuses_to_discard_unreconciled_changes(
    memory_store,
):
    row = expert_row()
    db = MemoryDatabase([row])
    db.resources[str(EXPERT)] = saved_resource(module.expert_manifest(row))
    with pytest.raises(RuntimeError, match="both a manifest and legacy payload"):
        await module.migrate_stored_experts(db)
    assert db.rows == [row]


@pytest.mark.asyncio
async def test_generic_catalog_detail_never_loads_reference_harness_settings():
    from orchestrator.services.expert_catalog import ExpertCatalogService

    row = expert_row()
    document = module.expert_manifest(row)
    document["spec"]["runtime"].pop("adapter")
    document["spec"]["runtime"]["image"] = "custom/worker:1"
    projected = module.project_expert_resource(row, saved_resource(document))
    deps = SimpleNamespace(
        store=SimpleNamespace(get_expert_by_id=AsyncMock(return_value=projected)),
        state=None,
        experts_enabled=lambda: True,
        looks_like_uuid=lambda _: True,
    )
    detail = await ExpertCatalogService(deps).load_expert_detail(str(EXPERT))
    assert detail["manifest"] == document
    assert detail["settings_matrix"] == {}
    assert detail["harness_adapter"] is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,args,kwargs",
    [
        ("get_expert_by_id", [str(EXPERT)], {}),
        ("get_expert_by_managed_key", ["application-default-worker-seed"], {}),
        ("get_application_expert_default", ["worker"], {}),
        (
            "get_user_expert_default",
            [],
            {"user_id": str(USER), "expert_type": "worker"},
        ),
        (
            "get_project_default_expert",
            [],
            {"project_id": str(USER), "expert_type": "worker"},
        ),
        ("get_project_linked_expert", [str(USER), str(EXPERT)], {}),
        ("get_expert_visible_by_id", [str(EXPERT)], {"user_id": str(USER)}),
    ],
)
async def test_db_read_funnels_project_canonical_payload(
    method, args, kwargs, memory_store
):
    from orchestrator.database.postgres import PostgresDB

    db = PostgresDB.__new__(PostgresDB)
    original = expert_row()
    resource = saved_resource(module.expert_manifest(original))
    raw = {
        **original,
        "config": {},
        "prompts": {},
        "manifest_resource_id": resource["id"],
        "project_id": USER,
        "default_for": "worker",
        "config_override": None,
    }
    db.resources = {str(EXPERT): resource}
    db.fetchrow = AsyncMock(return_value=raw)
    result = await getattr(db, method)(*args, **kwargs)
    assert result["config"] == original["config"]
    assert result["prompts"] == original["prompts"]
    assert result["manifest_uid"] == str(resource["id"])


@pytest.mark.asyncio
async def test_shipped_catalog_provenance_cannot_be_requested_through_annotations():
    document = module.expert_manifest(expert_row())
    document["metadata"]["annotations"]["srw.io/bundled-selector"] = "assistant"
    resource = saved_resource(document, linked_id=None)
    resource["owner_id"] = USER
    db = SimpleNamespace(execute=AsyncMock())
    await module.bind_native_expert_identity(db, resource)
    assert resource["linked_id"]
    insert = db.execute.call_args_list[0].args
    assert insert[9] == USER
    assert insert[10] is False


@pytest.mark.asyncio
async def test_ownerless_catalog_import_does_not_duplicate_bundled_entries():
    resource = saved_resource(module.expert_manifest(expert_row()), linked_id=None)
    resource["owner_id"] = None
    db = SimpleNamespace(execute=AsyncMock())
    assert await module.bind_native_expert_identity(db, resource) is resource
    db.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_project_managed_expert_refuses_legacy_editor_write(memory_store):
    row = expert_row()
    db = MemoryDatabase([row])
    resource = saved_resource(module.expert_manifest(row))
    resource["managed_by"] = uuid4()
    db.resources[str(EXPERT)] = resource
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as caught:
        async with db.transaction_scope():
            await module.persist_expert_resource(db, row)
    assert caught.value.status_code == 409
    assert db.resources[str(EXPERT)] == resource


@pytest.mark.asyncio
async def test_bundled_default_identity_is_listed_once_by_resource_uid():
    from orchestrator.schemas.expert_catalog import ExpertInfo
    from orchestrator.services.expert_catalog import ExpertCatalogService

    raw = expert_row(
        name="bound-catalog-alias", owner_id=None, managed_key="bundled-resource:stable"
    )
    document = module.expert_manifest(raw)
    document["metadata"]["name"] = "developer"
    document["metadata"]["annotations"]["srw.io/bundled-selector"] = "developer"
    resource = saved_resource(document)
    projected = module.project_expert_resource(raw, resource)
    deps = SimpleNamespace(
        state=SimpleNamespace(
            experts=[
                ExpertInfo(
                    id="developer",
                    display_name="Developer",
                    description="A bundled expert",
                )
            ],
            library=[],
        ),
        store=SimpleNamespace(list_experts_visible=AsyncMock(return_value=[projected])),
        manifests=SimpleNamespace(list_scope=AsyncMock(return_value=[resource])),
        experts_enabled=lambda: True,
        visible_project_ids=AsyncMock(return_value=[]),
    )
    result = await ExpertCatalogService(deps).list_experts(user={"id": USER})
    assert len(result) == 1
    assert result[0]["id"] == str(EXPERT)
    assert result[0]["manifest_uid"] == str(resource["id"])


@pytest.mark.asyncio
async def test_retired_bundled_resource_does_not_fall_back_to_disk():
    from fastapi import HTTPException
    from orchestrator.schemas.expert_catalog import ExpertInfo
    from orchestrator.services.expert_catalog import ExpertCatalogService

    deps = SimpleNamespace(
        state=SimpleNamespace(
            experts=[
                ExpertInfo(id="developer", display_name="Developer", description="")
            ],
            library=[],
        ),
        store=SimpleNamespace(),
        manifests=SimpleNamespace(
            list_scope=AsyncMock(return_value=[]),
            by_name=AsyncMock(return_value=None),
        ),
        experts_enabled=lambda: False,
    )
    catalog = ExpertCatalogService(deps)
    assert await catalog.list_experts(user={"id": USER}) == []
    with pytest.raises(HTTPException) as missing:
        await catalog.bundled_manifest("developer")
    assert missing.value.status_code == 404


@pytest.mark.asyncio
async def test_legacy_export_refuses_to_drop_a_canonical_asset_selection():
    from fastapi import HTTPException
    from orchestrator.services.expert_authoring import ExpertAuthoringService

    row = expert_row(harness_asset_name="developer")
    authoring = ExpertAuthoringService(
        store=SimpleNamespace(get_expert_visible_by_id=AsyncMock(return_value=row)),
        catalog=SimpleNamespace(
            deps=SimpleNamespace(
                looks_like_uuid=lambda value: True,
                visible_project_ids=AsyncMock(return_value=[]),
            )
        ),
        resolve_default_models=None,
        prefetch_roster_refs=None,
    )
    with pytest.raises(HTTPException, match="manifest") as refused:
        await authoring.export_expert(str(EXPERT), user={"id": USER})
    assert refused.value.status_code == 409


def _source_authoring(*, row=None, bundled=None):
    from orchestrator.services.expert_authoring import ExpertAuthoringService

    return ExpertAuthoringService(
        store=SimpleNamespace(get_expert_visible_by_id=AsyncMock(return_value=row)),
        catalog=SimpleNamespace(
            deps=SimpleNamespace(
                looks_like_uuid=lambda value: value == str(EXPERT),
                visible_project_ids=AsyncMock(return_value=[]),
            ),
            bundled_expert_source=AsyncMock(return_value=bundled),
        ),
        resolve_default_models=None,
        prefetch_roster_refs=None,
    )


@pytest.mark.asyncio
async def test_source_reads_an_asset_backed_copy_that_export_refuses():
    authoring = _source_authoring(
        row=expert_row(
            harness_asset_name="developer",
            harness_config_name="worker_base",
            harness_config_layers=[{"config": {"limits": {"max_turns": 9}}}],
        )
    )

    source = await authoring.expert_source(str(EXPERT), user={"id": USER})

    assert source["name"] == "helper"
    assert source["display_name"] == "Helper"
    assert source["config"] == expert_row()["config"]
    assert source["prompts"] == {"persona": "Keep the authored persona."}
    assert not any(key.startswith("harness_") for key in source)


@pytest.mark.asyncio
async def test_source_reads_a_bundled_expert_with_its_prompt_files():
    authoring = _source_authoring(
        bundled={
            "name": "developer",
            "display_name": "Developer",
            "expert_type": "worker",
            "tags": ["worker"],
            "config": {"limits": {"max_turns": 9}},
            "prompts": {"persona": "From persona.txt", "tactical": "From the skill"},
            "harness_asset_name": "developer",
        }
    )

    source = await authoring.expert_source("developer", user={"id": USER})

    assert source["prompts"] == {
        "persona": "From persona.txt",
        "tactical": "From the skill",
    }
    assert "harness_asset_name" not in source


@pytest.mark.asyncio
async def test_source_hides_a_missing_or_invisible_expert():
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as missing_row:
        await _source_authoring().expert_source(str(EXPERT), user={"id": USER})
    with pytest.raises(HTTPException) as missing_bundled:
        await _source_authoring().expert_source("nobody", user={"id": USER})

    assert missing_row.value.status_code == 404
    assert missing_bundled.value.status_code == 404
