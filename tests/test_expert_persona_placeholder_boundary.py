"""DB expert personas are content, never nested prompt templates."""

from __future__ import annotations

from tests._expert_catalog import authoring_service, catalogue_route
from orchestrator.routers import expert_catalog as expert_routes
from orchestrator.schemas import expert_catalog as expert_schemas
from orchestrator.services import expert_authoring as expert_authoring_module


import json
import pathlib
import re
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
from fastapi.testclient import TestClient
from fastapi import HTTPException
from pydantic import ValidationError

from orchestrator import main
from orchestrator.database.migrate import discover
from orchestrator.services.default_experts import load_seed_bundle
from shared.runtime.core.expert_resolution import (
    ASSEMBLER_OWNED_PROMPT_TOKENS,
    validate_expert_persona_placeholders,
)
from orchestrator.security import access as access_module
from orchestrator.security import auth as auth_module
from orchestrator.services import grant_enforcement as grant_enforcement_module


ROOT = pathlib.Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "src" / "orchestrator" / "database" / "migrations" / "app"
NAME = "0209_expert_persona_identity_backfill.sql"
SQL = MIGRATIONS / NAME
PREDECESSOR = "0208_threads_subagent_validate.sql"


def _statements() -> str:
    return "\n".join(
        line
        for line in SQL.read_text().splitlines()
        if not line.lstrip().startswith("--")
    )


def _compact(sql: str) -> str:
    return " ".join(sql.split())


def _expert_payload(persona: str) -> dict:
    return {
        "name": "plain-persona",
        "display_name": "Plain Persona",
        "expert_type": "worker",
        "prompts": {"persona": persona},
    }


def test_reserved_token_inventory_matches_the_assembler_contract():
    assert ASSEMBLER_OWNED_PROMPT_TOKENS == (
        "{phase_number}",
        "{agent_display_name}",
        "{expert_identity}",
        "{available_skills}",
        "{subagent_environment}",
        "{prompt_content}",
    )


@pytest.mark.parametrize("token", ASSEMBLER_OWNED_PROMPT_TOKENS)
def test_create_and_update_models_reject_every_reserved_persona_token(token: str):
    with pytest.raises(ValidationError, match="reserved prompt placeholders") as create:
        expert_schemas.ExpertCreate(**_expert_payload(f"before {token} after"))
    assert token in str(create.value)

    with pytest.raises(ValidationError, match="reserved prompt placeholders") as update:
        expert_schemas.ExpertUpdate(prompts={"persona": f"before {token} after"})
    assert token in str(update.value)


def test_persona_validation_allows_non_reserved_braces_and_does_not_mutate():
    prompts = {
        "persona": 'Use JSON such as {"status": "ok"} and {not_a_framework_token}.',
        # This boundary is intentionally persona-only. Other expert prompt
        # segments retain their existing semantics.
        "instructions": "Explain {available_skills} literally.",
    }
    assert validate_expert_persona_placeholders(prompts) is prompts
    assert expert_schemas.ExpertCreate(
        **_expert_payload(prompts["persona"])
    ).prompts == {"persona": prompts["persona"]}


def test_create_update_and_import_http_surfaces_return_clear_422s():
    """FastAPI validates before the route's manual auth/DB calls.

    Import deliberately reuses ExpertCreate, so exercising both POST paths
    pins that shared boundary instead of merely assuming it from annotations.
    """
    client = TestClient(main.app)
    token = "{available_skills}"
    for method, path, payload in (
        ("post", "/api/experts", _expert_payload(token)),
        ("post", "/api/experts/import", _expert_payload(token)),
        (
            "put",
            f"/api/experts/{uuid4()}",
            {"prompts": {"persona": token}},
        ),
    ):
        response = getattr(client, method)(path, json=payload)
        assert response.status_code == 422, (method, path, response.text)
        assert "reserved prompt placeholders" in response.text
        assert token in response.text


@pytest.mark.asyncio
async def test_copy_boundary_accepts_clean_prompts_and_refuses_legacy_tokens(
    monkeypatch,
):
    created = AsyncMock(return_value={"id": "copy"})
    monkeypatch.setattr(main.app.state.resources.postgres_db, "create_expert", created)
    source = {
        "name": "source",
        "display_name": "Source",
        "expert_type": "worker",
        "prompts": {"persona": "Remain a careful analyst."},
    }

    assert await authoring_service().create_forked_expert(source, str(uuid4())) == {
        "id": "copy"
    }
    assert created.await_args.kwargs["prompts"] == source["prompts"]

    source["prompts"] = {"persona": "You are {agent_display_name}."}
    with pytest.raises(HTTPException, match="reserved prompt placeholders") as exc:
        await authoring_service().create_forked_expert(source, str(uuid4()))
    assert exc.value.status_code == 422
    created.assert_awaited_once()


@pytest.mark.asyncio
async def test_duplicate_route_returns_422_for_a_legacy_source(monkeypatch):
    source_id = str(uuid4())
    source = {
        "id": source_id,
        "owner_id": str(uuid4()),
        "name": "legacy-source",
        "display_name": "Legacy Source",
        "expert_type": "worker",
        "icon": "smart_toy",
        "color": "#6B7280",
        "config": {},
        "prompts": {"persona": "Use {available_skills}."},
    }
    fake = AsyncMock()
    fake.get_expert_visible_by_id = AsyncMock(return_value=source)
    fake.create_expert = AsyncMock(side_effect=AssertionError("must not write"))
    monkeypatch.setattr(main.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": str(uuid4()), "is_admin": False}),
    )
    monkeypatch.setattr(
        access_module, "user_visible_project_ids", AsyncMock(return_value=[])
    )
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_expert_save_prelude", AsyncMock()
    )
    monkeypatch.setattr(
        grant_enforcement_module, "strip_save_grants", AsyncMock(return_value=({}, []))
    )

    with pytest.raises(HTTPException, match="reserved prompt placeholders") as exc:
        await catalogue_route(expert_routes.duplicate_expert)(AsyncMock(), source_id)

    assert exc.value.status_code == 422
    fake.create_expert.assert_not_awaited()


@pytest.mark.asyncio
async def test_personal_default_fork_returns_422_for_a_legacy_source(monkeypatch):
    source = {
        "id": str(uuid4()),
        "owner_id": str(uuid4()),
        "name": "legacy-source",
        "display_name": "Legacy Source",
        "expert_type": "worker",
        "icon": "smart_toy",
        "color": "#6B7280",
        "config": {},
        "prompts": {"persona": "Use {prompt_content}."},
    }
    fake = AsyncMock()
    fake.fork_and_set_user_expert_default = AsyncMock(
        side_effect=AssertionError("must not write")
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", fake)
    monkeypatch.setattr(
        auth_module,
        "require_approved_user",
        AsyncMock(return_value={"id": str(uuid4()), "is_admin": False}),
    )
    monkeypatch.setattr(
        expert_authoring_module,
        "personal_defaults_allowed",
        AsyncMock(return_value=True),
    )
    monkeypatch.setattr(
        expert_authoring_module,
        "resolve_root_expert",
        AsyncMock(return_value=type("Selection", (), {"expert": source})()),
    )

    with pytest.raises(HTTPException, match="reserved prompt placeholders") as exc:
        await catalogue_route(expert_routes.fork_my_expert_default)(
            AsyncMock(), "worker", expert_schemas.ExpertDefaultForkRequest()
        )

    assert exc.value.status_code == 422
    fake.fork_and_set_user_expert_default.assert_not_awaited()


def test_managed_seed_rejects_a_reserved_persona_token(tmp_path):
    expert_dir = tmp_path / "experts" / "legacy-seed"
    expert_dir.mkdir(parents=True)
    (expert_dir / "config.yaml").write_text(
        "$extends: worker_base\nagent_id: legacy-seed\n",
        encoding="utf-8",
    )
    (expert_dir / "persona.txt").write_text(
        "Remain {agent_display_name}.", encoding="utf-8"
    )

    with pytest.raises(ValueError, match="reserved prompt placeholders"):
        load_seed_bundle(tmp_path, directory="legacy-seed", expert_type="worker")


def test_migration_is_the_unique_transactional_head_after_0208():
    assert SQL.exists()
    assert SQL.read_text().startswith(f"-- migration:     {NAME}")
    assert PREDECESSOR in SQL.read_text()
    assert "-- transactional: yes" in SQL.read_text()
    names = [path.name for path in discover(MIGRATIONS)]
    # This pinned `names[-1] == NAME`, i.e. "no migration ever lands after
    # 0209" — not an invariant, just true on the day it was written. It went
    # red the moment 0210 landed (f1bae1ec). What the test is actually for is
    # this migration's position relative to its declared predecessor; the
    # global head is pinned once, in test_infrastructure_metering_migrations.
    assert names.index(NAME) == names.index(PREDECESSOR) + 1


def test_migration_updates_only_string_personas_with_the_exact_legacy_token():
    sql = _compact(_statements())
    assert sql.count("UPDATE ") == 1
    assert "UPDATE public.experts AS experts" in sql
    assert "SET prompts = jsonb_set(" in sql
    assert "'{persona}'" in sql
    assert "to_jsonb(" in sql
    assert "replace(" in sql
    assert "prompts ->> 'persona', '{agent_display_name}'," in sql
    assert "WITH RECURSIVE legacy_personas AS" in sql
    assert "FROM neutralized_names" in sql
    assert "FROM safe_names" in sql
    assert "LIKE ANY" not in sql
    assert "false )" in sql
    assert "jsonb_typeof(prompts -> 'persona') = 'string'" in sql
    assert "strpos(prompts ->> 'persona', '{agent_display_name}') > 0" in sql
    assert not re.search(r"\b(ALTER|CREATE|DROP|TRUNCATE|INSERT|DELETE)\b", sql, re.I)
    for untouched_column in ("version", "updated_at", "updated_by"):
        assert untouched_column not in sql


@pytest.fixture(scope="module")
def scratch_pg_dsn() -> str:
    testcontainers = pytest.importorskip("testcontainers.postgres")
    try:
        container = testcontainers.PostgresContainer("postgres:16")
        container.start()
    except Exception as exc:
        pytest.skip(f"no container runtime for the persona backfill test: {exc}")
    try:
        yield re.sub(
            r"^postgresql\+\w+://", "postgresql://", container.get_connection_url()
        )
    finally:
        container.stop()


@pytest.mark.asyncio
async def test_migration_backfills_exactly_matching_rows_and_is_idempotent(
    scratch_pg_dsn: str,
):
    conn = await asyncpg.connect(scratch_pg_dsn)
    try:
        await conn.execute(
            """
            CREATE TABLE public.experts (
                id bigserial PRIMARY KEY,
                key text UNIQUE NOT NULL,
                display_name text NOT NULL,
                prompts jsonb NOT NULL DEFAULT '{}'::jsonb
            )
            """
        )
        fixtures = {
            "one": (
                "Ada",
                {
                    "persona": "You are {agent_display_name}.",
                    "instructions": "leave me byte-for-byte",
                },
            ),
            "many": (
                "Grace",
                {
                    "persona": "{agent_display_name} / {agent_display_name}",
                    "strategic": {"nested": [1, "two"]},
                },
            ),
            "token-shaped-name": (
                "Dr {{available_skills}} {agent_display_name} {EU} {phaseXnumber}",
                {"persona": "Remain {agent_display_name}."},
            ),
            "plain": ("Plain", {"persona": "No token.", "x": "same"}),
            "other-key": (
                "Other",
                {"persona": "No token.", "instructions": "{agent_display_name}"},
            ),
            "non-string": ("Number", {"persona": 17, "x": "same"}),
            "missing": ("Missing", {"instructions": "same"}),
            "null": ("Null", {"persona": None, "x": "same"}),
        }
        await conn.executemany(
            "INSERT INTO public.experts (key, display_name, prompts) "
            "VALUES ($1, $2, $3::jsonb)",
            [
                (key, name, json.dumps(prompts))
                for key, (name, prompts) in fixtures.items()
            ],
        )
        before = {
            row["key"]: (row["prompts_text"], row["xmin"])
            for row in await conn.fetch(
                "SELECT key, prompts::text AS prompts_text, xmin::text AS xmin "
                "FROM public.experts"
            )
        }

        await conn.execute(SQL.read_text())

        after_rows = await conn.fetch(
            "SELECT key, prompts, prompts::text AS prompts_text, xmin::text AS xmin "
            "FROM public.experts"
        )
        after = {row["key"]: row for row in after_rows}
        one = json.loads(after["one"]["prompts"])
        many = json.loads(after["many"]["prompts"])
        assert one["persona"] == "You are Ada."
        assert many["persona"] == "Grace / Grace"
        assert one["instructions"] == "leave me byte-for-byte"
        assert many["strategic"] == {"nested": [1, "two"]}
        token_shaped = json.loads(after["token-shaped-name"]["prompts"])
        assert token_shaped["persona"] == (
            "Remain Dr available_skills agent_display_name {EU} {phaseXnumber}."
        )
        assert not any(
            token in token_shaped["persona"] for token in ASSEMBLER_OWNED_PROMPT_TOKENS
        )

        for key in ("plain", "other-key", "non-string", "missing", "null"):
            assert after[key]["prompts_text"] == before[key][0], key
            assert after[key]["xmin"] == before[key][1], key

        once = {row["key"]: (row["prompts_text"], row["xmin"]) for row in after_rows}
        await conn.execute(SQL.read_text())
        twice = {
            row["key"]: (row["prompts_text"], row["xmin"])
            for row in await conn.fetch(
                "SELECT key, prompts::text AS prompts_text, xmin::text AS xmin "
                "FROM public.experts"
            )
        }
        assert twice == once
    finally:
        await conn.close()
