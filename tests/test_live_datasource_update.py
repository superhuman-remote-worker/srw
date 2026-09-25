"""Tests for live datasource changes (live_session_settings.md Slice B).

Covers the three backend surfaces:

- ``PostgresDB.set_thread_datasource_ids`` — the new top-level metadata
  writer (full-set replace; ``merge_thread_config_override`` can't touch
  sibling keys of ``config_override``).
- ``agent_update_thread_config`` — the internal PATCH's ``datasource_ids``
  field: create-parity authorization (including the lite-tier/repository
  rule against the thread's CURRENT backend), the flip-then-grant-check
  ordering, and persist-after-checks.
- ``PersistentSession.resetup_datasources`` — the agent-side live rewire:
  in-place registry swap, deferred-close contract, category application
  outside the validated tools vocabulary, repo/kb handling, index rewrite.

Plus ``inject_workspace_facts``'s replace-the-marked-block (not re-append) semantics.

Slice C additions: the owner-facing disconnected-session PATCH
(``update_thread_config``) that runs the same ``_apply_thread_config_update``
core (redacted response, connected-session 409 gate), and the
``session_config_updated`` audit event both endpoints now emit.
"""

import json
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# R1.B06: these handlers moved to services/thread_config_update with their
# routes in routers/thread_config. main's dependency factory still reads
# main's attributes at call time, so the patches below keep steering what
# they steered before.
from orchestrator.services import thread_config_update  # noqa: E402
from fastapi import HTTPException
from orchestrator.application import sessions as sessions_composition
from orchestrator.schemas import thread_config as thread_config_module
from orchestrator.security import access as access_module
from orchestrator.services import dispatch_credentials as dispatch_credentials_module
from orchestrator.services import grant_enforcement as grant_enforcement_module
from orchestrator.services import thread_config_update as thread_config_update_module
from orchestrator.services import thread_mount_rows as thread_mount_rows_module
import fastapi as fastapi_module

THREAD_ID = "11111111-2222-3333-4444-555555555555"
USER_ID = "aaaaaaaa-1111-4111-8111-111111111111"
DATASOURCE_A_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1"
DATASOURCE_B_ID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbb2"
REPOSITORY_ID = "cccccccc-cccc-4ccc-8ccc-ccccccccccc3"


# ---------------------------------------------------------------------------
# PostgresDB.set_thread_datasource_ids
# ---------------------------------------------------------------------------


def _make_db(update_result="UPDATE 1"):
    from orchestrator.database.postgres import (
        PostgresDB,
        _thread_datasource_lock_key,
    )

    db = PostgresDB.__new__(PostgresDB)
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=update_result)

    @asynccontextmanager
    async def transaction():
        yield

    conn.transaction = MagicMock(side_effect=transaction)
    pool_cm = AsyncMock()
    pool_cm.__aenter__.return_value = conn
    pool_cm.__aexit__.return_value = False
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=pool_cm)
    db._pool = pool
    db._test_datasource_lock_keys = []

    @asynccontextmanager
    async def datasource_lock(thread_id, **_kwargs):
        db._test_datasource_lock_keys.append(_thread_datasource_lock_key(thread_id))
        yield

    db.thread_datasource_lock = datasource_lock
    return db, conn


class TestSetThreadDatasourceIds:
    @pytest.mark.asyncio
    async def test_credential_connector_cannot_be_removed_from_selection(self):
        from shared.credential_connectors import CredentialConnectorAttachedError

        db, conn = _make_db()
        conn.fetch.return_value = [{"id": DATASOURCE_A_ID, "type": "credentials"}]
        with pytest.raises(CredentialConnectorAttachedError, match="lifetime"):
            await db.set_thread_datasource_ids(THREAD_ID, [])
        conn.execute.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_replaces_ids_and_policy_provenance_atomically(self):
        db, conn = _make_db()
        conn.fetch.return_value = [
            {"id": uuid.UUID(DATASOURCE_A_ID), "policy_revision": 3},
            {"id": uuid.UUID(DATASOURCE_B_ID), "policy_revision": 5},
        ]
        ok = await db.set_thread_datasource_ids(
            THREAD_ID,
            [DATASOURCE_B_ID, DATASOURCE_A_ID],
            datasource_policy_revisions={
                DATASOURCE_A_ID: 3,
                DATASOURCE_B_ID: 5,
            },
            datasource_selection_provenance={"origin": "explicit"},
        )

        assert ok is True
        sql = conn.execute.call_args.args[0]
        assert "metadata = COALESCE(metadata, '{}'::jsonb) || $1::jsonb" in sql
        patch = json.loads(conn.execute.call_args.args[1])
        assert patch == {
            "datasource_ids": [DATASOURCE_B_ID, DATASOURCE_A_ID],
            "datasource_selection": {
                "origin": "explicit",
                "datasource_ids": [DATASOURCE_B_ID, DATASOURCE_A_ID],
                "policy_revisions": {
                    DATASOURCE_B_ID: 5,
                    DATASOURCE_A_ID: 3,
                },
            },
        }
        conn.transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_empty_list_detaches_all(self):
        db, conn = _make_db()
        assert (
            await db.set_thread_datasource_ids(
                THREAD_ID,
                [],
                datasource_policy_revisions={},
                datasource_selection_provenance={"origin": "explicit"},
            )
            is True
        )
        patch = json.loads(conn.execute.call_args.args[1])
        assert patch["datasource_ids"] == []
        assert patch["datasource_selection"]["policy_revisions"] == {}
        conn.fetch.assert_awaited_once()
        assert "d.type = 'credentials'" in conn.fetch.await_args.args[0]
        assert db._test_datasource_lock_keys
        conn.transaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_unknown_thread_returns_false(self):
        db, _ = _make_db(update_result="UPDATE 0")
        assert (
            await db.set_thread_datasource_ids(
                THREAD_ID,
                [],
                datasource_policy_revisions={},
            )
            is False
        )

    @pytest.mark.asyncio
    async def test_invalid_uuid_returns_false_without_query(self):
        db, conn = _make_db()
        assert await db.set_thread_datasource_ids("not-a-uuid", ["ds-a"]) is False
        conn.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_policy_conflict_leaves_thread_metadata_unchanged(self):
        from orchestrator.database.postgres import DatasourcePolicyConflictError

        db, conn = _make_db()
        conn.fetch.return_value = [
            {"id": uuid.UUID(DATASOURCE_A_ID), "policy_revision": 4}
        ]

        with pytest.raises(DatasourcePolicyConflictError):
            await db.set_thread_datasource_ids(
                THREAD_ID,
                [DATASOURCE_A_ID],
                datasource_policy_revisions={DATASOURCE_A_ID: 3},
                datasource_selection_provenance={"origin": "explicit"},
            )

        # The shared delivery/write lock is acquired before the revision
        # snapshot. The metadata UPDATE itself is never reached.
        assert conn.execute.await_count == 0
        assert db._test_datasource_lock_keys

    @pytest.mark.asyncio
    async def test_delivery_lock_uses_same_key_as_empty_selection_writer(self):
        db, conn = _make_db()

        async with db.thread_datasource_lock(THREAD_ID):
            pass
        delivery_key = db._test_datasource_lock_keys[-1]

        conn.reset_mock()
        await db.set_thread_datasource_ids(
            THREAD_ID,
            [],
            datasource_policy_revisions={},
        )
        writer_key = db._test_datasource_lock_keys[-1]

        assert delivery_key == writer_key


# ---------------------------------------------------------------------------
# agent_update_thread_config: the datasource_ids PATCH field
# ---------------------------------------------------------------------------


def _thread_row(user_id=USER_ID, backend="sandbox", metadata_extra=None):
    metadata = {"config_override": {"workspace": {"backend": backend}}}
    metadata.update(metadata_extra or {})
    return {
        "id": THREAD_ID,
        "user_id": user_id,
        "project_id": None,
        "metadata": metadata,
    }


@pytest.fixture
def patched_main(monkeypatch):
    """orchestrator.main with the PATCH endpoint's collaborators mocked."""
    import orchestrator.main as main

    policy_rows = {
        DATASOURCE_A_ID: {
            "id": DATASOURCE_A_ID,
            "type": "postgresql",
            "is_global": True,
            "scope_mode": "all",
            "policy_revision": 1,
            "project_ids": [],
        },
        DATASOURCE_B_ID: {
            "id": DATASOURCE_B_ID,
            "type": "postgresql",
            "is_global": True,
            "scope_mode": "all",
            "policy_revision": 1,
            "project_ids": [],
        },
        REPOSITORY_ID: {
            "id": REPOSITORY_ID,
            "type": "repository",
            "is_global": True,
            "scope_mode": "all",
            "policy_revision": 1,
            "project_ids": [],
        },
    }
    db = SimpleNamespace(
        get_thread=AsyncMock(return_value=_thread_row()),
        get_user=AsyncMock(
            return_value={"id": USER_ID, "is_admin": False, "is_approved": True}
        ),
        user_is_member_of_projects=AsyncMock(return_value=True),
        get_datasource_policy_rows=AsyncMock(
            side_effect=lambda ids: [policy_rows[value] for value in ids]
        ),
        get_datasource=AsyncMock(
            return_value={
                "id": DATASOURCE_A_ID,
                "type": "postgresql",
                "is_global": True,
            }
        ),
        resolve_datasources_for_thread=AsyncMock(
            return_value=[
                {"type": "postgresql", "name": "PG", "project_read_only": False}
            ]
        ),
        set_thread_datasource_ids=AsyncMock(return_value=True),
        merge_thread_config_override=AsyncMock(return_value=True),
        record_security_event=AsyncMock(),
        resolve_api_keys_for_job=AsyncMock(return_value={}),
    )
    db.datasource_policy_rows = policy_rows

    @asynccontextmanager
    async def transaction_scope(_thread_id):
        yield SimpleNamespace(
            fetchrow=AsyncMock(
                side_effect=lambda query, *_: None
                if "srw_execution_specs" in query
                else db.get_thread.return_value
            )
        )

    db.thread_configuration_transaction = transaction_scope
    db.refresh_session_execution = AsyncMock(
        side_effect=lambda _thread_id, *, conn, config_override: {
            "delivery_override": config_override
        }
    )
    monkeypatch.setattr(main.app.state.resources, "postgres_db", db)
    monkeypatch.setattr(access_module, "require_internal", AsyncMock())
    monkeypatch.setattr(
        thread_mount_rows_module, "thread_project_ids", AsyncMock(return_value=[])
    )
    grants = AsyncMock()
    monkeypatch.setattr(
        grant_enforcement_module, "enforce_session_create_grants", grants
    )
    return main, db, grants


def _body(main, config_override=None, datasource_ids=None):
    return thread_config_module.AgentThreadConfigUpdateRequest(
        config_override=config_override or {}, datasource_ids=datasource_ids
    )


class TestAgentPatchDatasourceIds:
    @pytest.mark.asyncio
    async def test_credential_detach_fails_before_config_is_written(self, patched_main):
        main, db, _ = patched_main
        db.datasource_policy_rows[DATASOURCE_A_ID]["type"] = "credentials"
        db.get_thread.return_value = _thread_row(
            metadata_extra={"datasource_ids": [DATASOURCE_A_ID]}
        )
        with pytest.raises(fastapi_module.HTTPException) as caught:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert caught.value.status_code == 409
        assert "stay attached" in caught.value.detail
        db.merge_thread_config_override.assert_not_awaited()
        db.set_thread_datasource_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_authorizes_against_current_workspace_backend(self, patched_main):
        """A live add is create-like: unlike attach revalidation (which
        deliberately passes None), the PATCH must pass the thread's current
        backend so the lite/repository rule stays alive."""
        main, db, _ = patched_main
        db.get_thread.return_value = _thread_row(backend="virtual")
        db.get_datasource.return_value = {
            "id": REPOSITORY_ID,
            "type": "repository",
            "is_global": True,
        }

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[REPOSITORY_ID]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 400
        assert "repository" in exc.value.detail.lower()
        db.set_thread_datasource_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flip_reaches_grant_check_before_persist(self, patched_main):
        """Flip-then-grant-check: the derived datasource tool categories must
        be in the grant-checked fragment, so a datasource_tools-denied
        principal fails at the PATCH — and nothing persists on denial."""
        main, db, grants = patched_main

        await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, datasource_ids=[DATASOURCE_A_ID]),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        fragment = grants.await_args.args[0]
        # RW postgresql resolved above → write tools in the checked fragment.
        assert "sql_execute" in fragment["tools"]["sql"]
        persisted = db.set_thread_datasource_ids.await_args
        assert persisted.args == (THREAD_ID, [DATASOURCE_A_ID])
        assert persisted.kwargs["datasource_policy_revisions"] == {DATASOURCE_A_ID: 1}
        provenance = persisted.kwargs["datasource_selection_provenance"]
        assert provenance["origin"] == "explicit"
        assert provenance["creation_path"] == "live_thread_internal"
        assert provenance["policy_revisions"] == {DATASOURCE_A_ID: 1}

    @pytest.mark.asyncio
    async def test_grant_denial_prevents_persist(self, patched_main):
        main, db, grants = patched_main
        grants.side_effect = fastapi_module.HTTPException(
            status_code=422, detail="denied"
        )

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[DATASOURCE_A_ID]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 422
        db.set_thread_datasource_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_policy_change_before_selection_persist_returns_conflict(
        self, patched_main
    ):
        from orchestrator.database.postgres import DatasourcePolicyConflictError

        main, db, _ = patched_main
        db.set_thread_datasource_ids.side_effect = DatasourcePolicyConflictError(
            "changed"
        )

        with pytest.raises(HTTPException) as exc:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[DATASOURCE_A_ID]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )

        assert exc.value.status_code == 409
        assert "retry" in exc.value.detail.lower()

    @pytest.mark.asyncio
    async def test_flip_never_persists_into_config_override(self, patched_main):
        """The sql/graph/mongodb/webdav categories are outside the validated
        session vocabulary — they must not leak into the durable
        config_override merge (attach re-derives them from datasource_ids)."""
        main, db, _ = patched_main

        result = await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, datasource_ids=[DATASOURCE_A_ID]),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        merged = db.merge_thread_config_override.await_args.args[1]
        assert "sql" not in (merged.get("tools") or {})
        assert result["datasource_ids"] == [DATASOURCE_A_ID]

    @pytest.mark.asyncio
    async def test_ownerless_thread_rejects_arbitrary_connector_addition(
        self, patched_main
    ):
        """An internal key is not ambient authority for an ownerless thread."""
        main, db, grants = patched_main
        db.get_thread.return_value = _thread_row(user_id=None)

        with pytest.raises(HTTPException) as exc:
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[DATASOURCE_A_ID]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )

        assert exc.value.status_code == 403
        grants.assert_not_awaited()
        db.set_thread_datasource_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_ownerless_thread_can_narrow_materialized_selection(
        self, patched_main
    ):
        main, db, grants = patched_main
        db.get_thread.return_value = _thread_row(
            user_id=None,
            metadata_extra={"datasource_ids": [DATASOURCE_A_ID, DATASOURCE_B_ID]},
        )

        result = await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, datasource_ids=[DATASOURCE_A_ID]),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        grants.assert_not_awaited()
        persisted = db.set_thread_datasource_ids.await_args
        assert persisted.args == (THREAD_ID, [DATASOURCE_A_ID])
        assert persisted.kwargs["datasource_policy_revisions"] == {DATASOURCE_A_ID: 1}
        assert result["datasource_ids"] == [DATASOURCE_A_ID]

    @pytest.mark.asyncio
    async def test_no_datasource_field_means_no_datasource_write(self, patched_main):
        main, db, _ = patched_main

        result = await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, config_override={"llm": {"temperature": 0.5}}),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        db.set_thread_datasource_ids.assert_not_awaited()
        assert result["datasource_ids"] is None

    @pytest.mark.asyncio
    async def test_empty_list_detaches_all(self, patched_main):
        main, db, _ = patched_main

        await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, datasource_ids=[]),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        persisted = db.set_thread_datasource_ids.await_args
        assert persisted.args == (THREAD_ID, [])
        assert persisted.kwargs["datasource_policy_revisions"] == {}


# ---------------------------------------------------------------------------
# Slice C — owner-facing disconnected-session PATCH + config-change audit
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_owner(patched_main, monkeypatch):
    """patched_main plus the owner endpoint's collaborators (owner gate,
    model-transport enrichment)."""
    main, db, grants = patched_main
    owner_user = {"id": USER_ID, "is_admin": False, "auth_method": "oidc"}
    require_owner = AsyncMock(
        side_effect=lambda request, _db, tid: (owner_user, db.get_thread.return_value)
    )
    monkeypatch.setattr(access_module, "require_thread_owner", require_owner)

    async def fake_inject(*, section, model_id, user_id, resolved_keys, dependencies):
        # Enrichment resolves transport; the api_key is the secret that must
        # never reach the browser-facing response. R1.B12: the composition
        # binds this owner, so the call also carries its ``dependencies``.
        section["api_key"] = "sk-secret"

    monkeypatch.setattr(
        dispatch_credentials_module, "inject_model_credentials", fake_inject
    )
    return main, db, require_owner


def _patch_body(main, config_override=None, datasource_ids=None):
    return thread_config_module.ThreadConfigPatchRequest(
        config_override=config_override or {}, datasource_ids=datasource_ids
    )


class TestOwnerConfigPatch:
    @pytest.mark.asyncio
    async def test_connected_thread_rejected_409(self, patched_owner):
        main, db, _ = patched_owner
        row = _thread_row()
        row.update(agent_id="agent-1", status="active")
        db.get_thread.return_value = row

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.update_thread_config(
                THREAD_ID,
                _patch_body(main, {"llm": {"temperature": 0.2}}),
                MagicMock(),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 409
        db.merge_thread_config_override.assert_not_awaited()
        db.record_security_event.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_stale_agent_id_on_suspended_thread_is_editable(self, patched_owner):
        """Drain-suspend clears agent_id; a hard pod kill may not. No live
        agent serves a suspended thread, so it must stay editable."""
        main, db, _ = patched_owner
        row = _thread_row()
        row.update(agent_id="agent-dead", status="suspended")
        db.get_thread.return_value = row

        result = await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(main, {"llm": {"temperature": 0.2}}),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )
        assert result["status"] == "updated"
        assert result["effective"] == "next_attach"
        db.merge_thread_config_override.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_active_stateless_thread_is_editable_and_effective_next_turn(
        self, patched_owner
    ):
        """A queue-served session binds no agent, so this endpoint IS its
        live settings path (the Cockpit routes the pane here when
        /connection declares config.update: rest). Must persist while the
        thread is active, and say the change lands at the next turn."""
        main, db, _ = patched_owner
        row = _thread_row(backend="virtual")
        row.update(execution_lane="stateless", agent_id=None, status="active")
        db.get_thread.return_value = row

        result = await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(main, {"llm": {"reasoning_level": "max"}}),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )
        assert result["status"] == "updated"
        assert result["effective"] == "next_turn"
        assert result["config_override"] == {"llm": {"reasoning_level": "max"}}
        merged = db.merge_thread_config_override.await_args.args[1]
        assert merged == {"llm": {"reasoning_level": "max"}}
        db.record_security_event.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delegation_gate_rides_the_patch_validated(self, patched_owner):
        """The pane's Delegation tick sends the names AND the gate; the gate
        must persist (the factory needs both) and a malformed block is a 400,
        never a silent drop."""
        main, db, _ = patched_owner
        row = _thread_row(backend="virtual")
        row.update(execution_lane="stateless", agent_id=None, status="active")
        db.get_thread.return_value = row

        result = await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(
                main,
                {
                    "tools": {"delegation": ["delegate_agent"]},
                    "delegation": {"enabled": True, "mode": "light"},
                },
            ),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )
        assert result["config_override"]["delegation"] == {"enabled": True}
        merged = db.merge_thread_config_override.await_args.args[1]
        assert merged["delegation"] == {"enabled": True}
        assert merged["tools"]["delegation"] == ["delegate_agent"]

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.update_thread_config(
                THREAD_ID,
                _patch_body(main, {"delegation": {"enabled": "yes"}}),
                MagicMock(),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 400

    @pytest.mark.asyncio
    async def test_response_and_persist_redacted_with_transport_sentinels(
        self, patched_owner
    ):
        """A model change enriches transport exactly like the internal PATCH
        (explicit None sentinels so the next attach's deep-merge clears the
        previous model's transport) — but the secret never leaves: neither in
        the response nor in the durable merge."""
        main, db, _ = patched_owner

        result = await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(main, {"llm": {"model": "minimax-m3"}}),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        assert "api_key" not in result["config_override"]["llm"]
        assert result["config_override"]["llm"]["base_url"] is None
        merged = db.merge_thread_config_override.await_args.args[1]
        assert "api_key" not in merged["llm"]
        assert merged["llm"]["base_url"] is None
        assert merged["llm"]["provider"] is None

    @pytest.mark.asyncio
    async def test_datasource_change_shares_create_authorization(self, patched_owner):
        """The lite/repository rule and flip-then-grant-check come from the
        shared core — a repo datasource on a lite thread fails here exactly
        like at the internal endpoint."""
        main, db, _ = patched_owner
        db.get_thread.return_value = _thread_row(backend="virtual")
        db.get_datasource.return_value = {
            "id": REPOSITORY_ID,
            "type": "repository",
            "is_global": True,
        }

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.update_thread_config(
                THREAD_ID,
                _patch_body(main, datasource_ids=[REPOSITORY_ID]),
                MagicMock(),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 400
        db.set_thread_datasource_ids.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_datasource_set_persists_and_returns(self, patched_owner):
        main, db, _ = patched_owner

        result = await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(main, datasource_ids=[DATASOURCE_A_ID]),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )
        persisted = db.set_thread_datasource_ids.await_args
        assert persisted.args == (THREAD_ID, [DATASOURCE_A_ID])
        assert persisted.kwargs["datasource_policy_revisions"] == {DATASOURCE_A_ID: 1}
        assert result["datasource_ids"] == [DATASOURCE_A_ID]

    @pytest.mark.asyncio
    async def test_empty_body_rejected(self, patched_owner):
        main, db, _ = patched_owner
        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.update_thread_config(
                THREAD_ID,
                _patch_body(main),
                MagicMock(),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 400
        db.merge_thread_config_override.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_owner_denial_propagates_before_any_write(self, patched_owner):
        main, db, require_owner = patched_owner
        require_owner.side_effect = fastapi_module.HTTPException(
            status_code=403, detail="Not your thread"
        )

        with pytest.raises(fastapi_module.HTTPException) as exc:
            await thread_config_update.update_thread_config(
                THREAD_ID,
                _patch_body(main, {"llm": {"temperature": 0.1}}),
                MagicMock(),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        assert exc.value.status_code == 403
        db.merge_thread_config_override.assert_not_awaited()


class TestConfigChangeAudit:
    @pytest.mark.asyncio
    async def test_owner_patch_records_audit_event(self, patched_owner):
        main, db, _ = patched_owner

        await thread_config_update.update_thread_config(
            THREAD_ID,
            _patch_body(
                main,
                {"llm": {"model": "minimax-m3"}},
                datasource_ids=[DATASOURCE_A_ID],
            ),
            MagicMock(),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        kwargs = db.record_security_event.await_args.kwargs
        assert kwargs["event_type"] == "session_config_updated"
        assert kwargs["user_id"] == USER_ID
        assert kwargs["resource_type"] == "thread"
        assert kwargs["resource_id"] == THREAD_ID
        # Key paths only — never values (the enriched fragment holds secrets).
        assert "llm.model" in kwargs["detail"]
        assert "datasource_ids=1" in kwargs["detail"]
        assert "minimax-m3" not in kwargs["detail"]
        assert "sk-secret" not in kwargs["detail"]

    @pytest.mark.asyncio
    async def test_internal_patch_records_audit_event_without_user(self, patched_main):
        main, db, _ = patched_main

        await thread_config_update.agent_update_thread_config(
            MagicMock(),
            THREAD_ID,
            _body(main, config_override={"llm": {"temperature": 0.4}}),
            dependencies=sessions_composition.thread_config_update_dependencies(
                main.app.state.resources
            ),
        )

        kwargs = db.record_security_event.await_args.kwargs
        assert kwargs["event_type"] == "session_config_updated"
        assert kwargs["user_id"] is None
        assert "llm.temperature" in kwargs["detail"]

    @pytest.mark.asyncio
    async def test_denied_update_records_no_config_audit(self, patched_main):
        main, db, grants = patched_main
        grants.side_effect = fastapi_module.HTTPException(
            status_code=422, detail="denied"
        )

        with pytest.raises(fastapi_module.HTTPException):
            await thread_config_update.agent_update_thread_config(
                MagicMock(),
                THREAD_ID,
                _body(main, datasource_ids=[DATASOURCE_A_ID]),
                dependencies=sessions_composition.thread_config_update_dependencies(
                    main.app.state.resources
                ),
            )
        db.record_security_event.assert_not_awaited()


class TestConfigChangeSummary:
    def test_flattens_one_level_and_counts_datasources(self):
        detail = thread_config_update_module.config_change_summary(
            {"llm": {"model": "x", "temperature": 0.1}, "narration": "full"},
            ["a", "b"],
        )
        assert detail == "keys=llm.model,llm.temperature,narration datasource_ids=2"

    def test_empty_change(self):
        assert thread_config_update_module.config_change_summary({}, None) == "empty"


# ---------------------------------------------------------------------------
# PersistentSession.resetup_datasources
# ---------------------------------------------------------------------------


def _make_session(datasource_configs=None, datasources=None, clients=None):
    from agent.api.persistent_session import PersistentSession
    from shared.runtime.core.loader import ToolsConfig

    cfg = MagicMock()
    cfg.tools = ToolsConfig(sql=["stale_sql_tool"])
    cfg.extra = {}
    session = PersistentSession(
        thread_id=str(uuid.uuid4()),
        config=cfg,
        datasources=datasources if datasources is not None else {},
        _datasource_clients=clients if clients is not None else {},
        datasource_configs=datasource_configs or [],
    )
    session.tool_context = SimpleNamespace(datasources=session.datasources)
    session.workspace_manager = MagicMock()
    session.workspace_manager.source_repos = {}
    session.resetup_tools_for_backend = MagicMock()
    return session


def _ds(ds_type, name, read_only=False):
    return {"type": ds_type, "name": name, "project_read_only": read_only}


class TestResetupDatasources:
    @pytest.mark.asyncio
    async def test_live_detach_invalidates_an_already_bound_email_send_tool(self):
        """A prior ready observation and stale closure are not authorization."""

        from agent.tools.context import ToolContext
        from agent.tools.email.tools import create_email_tools

        old_connection = SimpleNamespace(
            access="send",
            unattended_send=True,
            open_smtp=MagicMock(),
        )
        session = _make_session(
            datasource_configs=[_ds("email", "Mailbox")],
            datasources={"email": old_connection},
        )
        action_context = ToolContext(datasources=session.datasources)
        bound_send = next(
            tool
            for tool in create_email_tools(action_context)
            if tool.name == "email_send"
        )

        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            await session.resetup_datasources([])

        result = bound_send.invoke(
            {"subject": "Hi", "body": "text", "to": ["person@example.test"]}
        )
        assert "Error" in result and "binding changed" in result
        old_connection.open_smtp.assert_not_called()

    @pytest.mark.asyncio
    async def test_email_runtime_facts_refresh_on_failed_ready_and_removed_attachment(
        self,
    ):
        """The published snapshot tracks aggregate state without retaining identity."""

        session = _make_session()
        email = {
            "type": "email",
            "name": "Private mailbox",
            "datasource_id": "private-datasource-id",
            "config": {
                "access": "send",
                "folders": ["INBOX/Private"],
            },
        }

        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            await session.resetup_datasources([email])
        failed = session.tool_context.session_runtime_facts
        assert failed.attached_datasource_types == ("email",)
        assert failed.email_access_tier == "send"
        assert failed.email_connection_failed is True

        connection = SimpleNamespace(access="send", unattended_send=True)
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({"email": connection}, {}, []),
        ):
            await session.resetup_datasources([email])
        ready = session.tool_context.session_runtime_facts
        assert ready.attached_datasource_types == ("email",)
        assert ready.email_access_tier == "send"
        assert ready.email_connection_failed is False
        assert ready.email_direct_send_enabled is True

        # The live type-keyed connection is the action authority when several
        # attached mailboxes resolve to one tool closure. A lower live tier
        # must not inherit the higher configured aggregate tier.
        connection.access = "read"
        session._refresh_runtime_facts()
        clamped = session.tool_context.session_runtime_facts
        assert clamped.email_access_tier == "read"

        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            await session.resetup_datasources([])
        removed = session.tool_context.session_runtime_facts
        assert removed.attached_datasource_types == ()
        assert removed.email_access_tier is None
        assert removed.email_connection_failed is False
        assert removed.email_direct_send_enabled is False
        assert "Private mailbox" not in repr(removed)
        assert "private-datasource-id" not in repr(removed)
        assert "INBOX/Private" not in repr(removed)

    @pytest.mark.asyncio
    async def test_swaps_registry_in_place_and_returns_stale_for_deferred_close(
        self,
    ):
        """ToolContext shares the dict by reference — the swap must mutate in
        place — and the replaced connections must come back UNCLOSED (bound
        tools hold them in closures; the caller closes after turn end)."""
        old_conn, old_client = MagicMock(), MagicMock()
        session = _make_session(
            datasource_configs=[_ds("postgresql", "Old PG")],
            datasources={"postgresql": old_conn},
            clients={"postgresql": old_client},
        )
        registry_ref = session.datasources
        new_conn = MagicMock()
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({"webdav": new_conn}, {}, []),
        ):
            summary = await session.resetup_datasources([_ds("webdav", "Cloud")])

        assert session.datasources is registry_ref
        assert session.tool_context.datasources is registry_ref
        assert registry_ref == {"webdav": new_conn}
        assert summary["stale_connections"] == {"postgresql": old_conn}
        assert summary["stale_clients"] == {"postgresql": old_client}
        old_conn.close.assert_not_called()
        old_client.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_applies_categories_directly_to_config_tools(self):
        """sql/graph/mongodb/webdav ride config.tools directly — the validated
        session tools override's closed vocabulary would drop them."""
        session = _make_session(datasource_configs=[_ds("postgresql", "PG")])
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            await session.resetup_datasources([_ds("webdav", "Cloud")])

        assert "webdav_write" in session.config.tools.webdav
        # Removed type stripped — stale sql tools must not survive.
        assert session.config.tools.sql == []
        assert session.config.extra["_cli_datasources"] == []
        session.resetup_tools_for_backend.assert_called_once()

    @pytest.mark.asyncio
    async def test_leaves_session_tool_groups_untouched(self):
        """Pairwise preservation: a datasource change must not clobber a
        prior live tool-group toggle (only the 4 datasource categories are
        written)."""
        session = _make_session()
        session.config.tools.canvas = []  # user disabled Canvas live
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            await session.resetup_datasources([_ds("postgresql", "PG")])

        assert session.config.tools.canvas == []

    @pytest.mark.asyncio
    async def test_summary_diff_by_type_and_name(self):
        session = _make_session(
            datasource_configs=[_ds("postgresql", "Keep"), _ds("webdav", "Drop")]
        )
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ):
            summary = await session.resetup_datasources(
                [_ds("postgresql", "Keep"), _ds("mongodb", "Add")]
            )

        assert summary["added"] == ["Add"]
        assert summary["removed"] == ["Drop"]

    @pytest.mark.asyncio
    async def test_kb_entries_skip_processing_and_flag_deferred(self):
        session = _make_session()
        with patch(
            "agent.core.datasource_setup.process_datasources",
            return_value=({}, {}, []),
        ) as process:
            summary = await session.resetup_datasources(
                [{"type": "kb", "name": "Docs KB", "datasource_id": "kb-1"}]
            )

        assert process.call_args.args[0] == []  # kb never opens a connector
        assert summary["kb_deferred"] is True

    @pytest.mark.asyncio
    async def test_rewrites_index_with_full_new_list(self):
        session = _make_session(datasource_configs=[_ds("postgresql", "PG")])
        new_list = [_ds("postgresql", "PG"), {"type": "kb", "name": "KB"}]
        with (
            patch(
                "agent.core.datasource_setup.process_datasources",
                return_value=({}, {}, []),
            ),
            patch("agent.core.datasource_setup.inject_workspace_facts") as inject,
        ):
            await session.resetup_datasources(new_list)

        inject.assert_called_once()
        assert inject.call_args.args == (new_list, session.workspace_manager)
        assert session.datasource_configs == new_list

    @pytest.mark.asyncio
    async def test_clones_added_repos_and_drops_removed_registration(self):
        repo_old = {
            "type": "repository",
            "name": "Old Repo",
            "connection_url": "https://git.example/org/old-repo.git",
        }
        repo_new = {
            "type": "repository",
            "name": "New Repo",
            "connection_url": "https://git.example/org/new-repo.git",
        }
        session = _make_session(datasource_configs=[repo_old])
        session.workspace_manager.source_repos = {"old-repo": MagicMock()}
        with (
            patch(
                "agent.core.datasource_setup.process_datasources",
                return_value=({}, {}, []),
            ),
            patch("agent.core.datasource_setup.clone_repository_datasources") as clone,
            patch("agent.core.datasource_setup.inject_workspace_facts"),
        ):
            await session.resetup_datasources([repo_new])

        clone.assert_called_once_with([repo_new], session.workspace_manager)
        # Removal keeps the clone on disk (documented) but drops the
        # session-side registration.
        assert "old-repo" not in session.workspace_manager.source_repos

    @pytest.mark.asyncio
    async def test_removal_also_drops_the_forge_metadata_and_its_token(self):
        """``source_repo_meta`` holds the repository's plaintext token.

        Popping only ``source_repos`` leaves that credential live on the
        workspace manager for the rest of the session, after the user has
        detached the datasource.
        """
        repo_old = {
            "type": "repository",
            "name": "Old Repo",
            "connection_url": "https://git.example/org/old-repo.git",
        }
        session = _make_session(datasource_configs=[repo_old])
        session.workspace_manager.source_repos = {"old-repo": MagicMock()}
        session.workspace_manager.source_repo_meta = {
            "old-repo": {"forge": "gitea", "token": "sekrit"}
        }
        with (
            patch(
                "agent.core.datasource_setup.process_datasources",
                return_value=({}, {}, []),
            ),
            patch("agent.core.datasource_setup.clone_repository_datasources"),
            patch("agent.core.datasource_setup.inject_workspace_facts"),
        ):
            await session.resetup_datasources([])

        assert "old-repo" not in session.workspace_manager.source_repos
        assert "old-repo" not in session.workspace_manager.source_repo_meta

    @pytest.mark.asyncio
    async def test_mcp_live_attach_connects_registers_and_grants_wildcard(self):
        session = _make_session()
        manager = MagicMock()
        manager.connect_all = AsyncMock()
        manager.get_langchain_tools.return_value = []
        manager.statuses = {"MCP": "connected"}

        with (
            patch(
                "agent.core.datasource_setup.process_datasources",
                return_value=({"mcp": manager}, {}, []),
            ),
            patch("agent.core.datasource_setup.inject_workspace_facts"),
            patch("agent.tools.registry.register_mcp_tools") as register,
        ):
            await session.resetup_datasources([_ds("mcp", "MCP")])

        manager.connect_all.assert_awaited_once()
        manager.annotate_configs.assert_called_once()
        register.assert_called_once_with(manager)
        assert session.config.tools.mcp == ["*"]
        session.resetup_tools_for_backend.assert_called_once()

    @pytest.mark.asyncio
    async def test_mcp_live_detach_purges_registry_and_defers_close(self):
        old_manager = MagicMock()
        session = _make_session(
            datasource_configs=[_ds("mcp", "MCP")],
            datasources={"mcp": old_manager},
        )

        with (
            patch(
                "agent.core.datasource_setup.process_datasources",
                return_value=({}, {}, []),
            ),
            patch("agent.core.datasource_setup.inject_workspace_facts"),
            patch("agent.tools.registry.register_mcp_tools") as register,
        ):
            summary = await session.resetup_datasources([])

        register.assert_called_once_with(None)
        assert session.config.tools.mcp == []
        assert summary["stale_connections"] == {"mcp": old_manager}
        old_manager.close.assert_not_called()

    @pytest.mark.asyncio
    async def test_noop_before_tool_setup(self):
        session = _make_session()
        session.tool_context = None
        summary = await session.resetup_datasources([_ds("postgresql", "PG")])
        assert summary["added"] == []
        session.resetup_tools_for_backend.assert_not_called()


# ---------------------------------------------------------------------------
# inject_workspace_facts: replace the marked README block, never re-append
# ---------------------------------------------------------------------------


class TestWorkspaceFactsRewrite:
    START = "<!-- srw:workspace-facts:start -->"
    END = "<!-- srw:workspace-facts:end -->"

    def _ws(self, existing=""):
        ws = MagicMock()
        if existing:
            ws.read_file.return_value = existing
        else:
            ws.read_file.side_effect = FileNotFoundError
        written = {}
        ws.write_file.side_effect = lambda path, content: written.update(
            {path: content}
        )
        return ws, written

    def test_second_injection_replaces_previous_block(self):
        from agent.core.datasource_setup import inject_workspace_facts

        ws, written = self._ws()
        inject_workspace_facts([_ds("postgresql", "First DB")], ws)
        ws.read_file.side_effect = None
        ws.read_file.return_value = written["README.md"]
        inject_workspace_facts([_ds("mongodb", "Second DB")], ws)

        content = written["README.md"]
        assert content.count(self.START) == 1
        assert content.count(self.END) == 1
        assert content.count("## Connectors") == 1
        assert "First DB" not in content
        assert "Second DB" in content

    def test_human_readme_is_appended_to_not_rewritten(self):
        from agent.core.datasource_setup import inject_workspace_facts

        ws, written = self._ws(existing="# Datasources\n\nintro text\n")
        inject_workspace_facts([_ds("postgresql", "PG")], ws)

        content = written["README.md"]
        assert content.startswith("# Datasources\n\nintro text")
        assert content.index("**PG**") > content.index("intro text")

    def test_remove_all_writes_explicit_empty_state(self):
        from agent.core.datasource_setup import inject_workspace_facts

        ws, written = self._ws(
            existing=(
                f"intro\n\n{self.START}\n## Connectors\n\n"
                f"- **Gone** (postgresql)\n{self.END}\n"
            )
        )
        inject_workspace_facts([], ws)

        content = written["README.md"]
        assert content.startswith("intro\n\n")
        assert "Gone" not in content
        assert "## Connectors" in content
        assert "_No connectors attached._" in content
