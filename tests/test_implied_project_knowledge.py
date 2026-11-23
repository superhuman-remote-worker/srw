"""Work that was never offered its project's knowledge-base connector keeps
the knowledge base; work that was offered it and left it out does not.

``with_implied_project_knowledge`` decides this at delivery. The agent then
binds what is delivered, no more (``tests/test_kb_attached_connectors_decide.py``).
"""

from __future__ import annotations

import uuid
from contextlib import ExitStack
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from agent.services.knowledge.bindings import build_knowledge_bindings
from orchestrator.services.implied_project_knowledge import (
    IMPLIED_KEY,
    NO_CONNECTOR,
    PREDATES_CONNECTOR,
    with_implied_project_knowledge,
)
from shared.native_kb import NATIVE_PROJECT_CONFIG_KEY

PROJECT = "1a387b4d-0000-4000-8000-00000000000a"
OTHER_PROJECT = "1a387b4d-0000-4000-8000-00000000000b"
ROW_ID = "1a387b4d-0000-4000-8000-0000000000c1"
ROW_CREATED = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _row(project_id: str = PROJECT, **overrides) -> dict:
    row = {
        "id": ROW_ID,
        "name": "Thesis Knowledge",
        "description": "notes",
        "config": {"root_path": "vault", NATIVE_PROJECT_CONFIG_KEY: project_id},
        "created_at": ROW_CREATED,
    }
    row.update(overrides)
    return row


class _Store:
    """The one lookup the helper makes: a project's knowledge-base connector
    by its managed key."""

    def __init__(self, rows: dict[str, dict | None]):
        self.rows = rows
        self.asked: list[str] = []

    async def get_datasource_by_managed_key(self, managed_key: str):
        kind, _, project_id = managed_key.partition(":")
        assert kind == "project-kb"
        self.asked.append(project_id)
        return self.rows.get(project_id)


def _database() -> dict:
    return {"type": "postgresql", "name": "db", "datasource_id": str(uuid.uuid4())}


def _delivered_kb(project_id: str = PROJECT) -> dict:
    return {
        "type": "kb",
        "name": "Thesis Knowledge",
        "datasource_id": ROW_ID,
        "config": {"root_path": "vault", NATIVE_PROJECT_CONFIG_KEY: project_id},
    }


class TestTheSelectionDecidesOnceTheConnectorExists:
    @pytest.mark.asyncio
    async def test_work_created_after_the_connector_gets_nothing_added(self):
        """The thesis job: a project with its knowledge-base connector, a job
        created now that names only its database."""
        store = _Store({PROJECT: _row()})
        payload = [_database()]

        result = await with_implied_project_knowledge(
            payload,
            store=store,
            project_ids=[PROJECT],
            created_at=ROW_CREATED + timedelta(days=30),
        )

        assert result is payload
        assert build_knowledge_bindings(project_ids=[PROJECT], datasources=result) == []

    @pytest.mark.asyncio
    async def test_an_explicit_empty_selection_stays_empty(self):
        store = _Store({PROJECT: _row()})

        result = await with_implied_project_knowledge(
            [],
            store=store,
            project_ids=[PROJECT],
            created_at=ROW_CREATED + timedelta(seconds=1),
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_work_created_the_moment_the_connector_was_is_not_older(self):
        store = _Store({PROJECT: _row()})

        result = await with_implied_project_knowledge(
            [], store=store, project_ids=[PROJECT], created_at=ROW_CREATED
        )

        assert result == []

    @pytest.mark.asyncio
    async def test_a_delivered_knowledge_base_is_not_looked_up_or_doubled(self):
        store = _Store({PROJECT: _row()})
        payload = [_delivered_kb()]

        result = await with_implied_project_knowledge(
            payload,
            store=store,
            project_ids=[PROJECT],
            created_at=ROW_CREATED - timedelta(days=1),
        )

        assert result is payload
        assert store.asked == []


class TestWorkThatNeverHadTheChoiceKeepsIt:
    @pytest.mark.asyncio
    async def test_a_project_without_a_connector(self):
        store = _Store({PROJECT: None})

        result = await with_implied_project_knowledge(
            [_database()],
            store=store,
            project_ids=[PROJECT],
            created_at=datetime(2026, 11, 16, tzinfo=timezone.utc),
        )

        assert len(result) == 2
        entry = result[-1]
        assert entry[IMPLIED_KEY] == NO_CONNECTOR
        assert entry["type"] == "kb"
        assert entry["datasource_id"] is None
        assert entry["credentials"] == {}
        assert entry["project_read_only"] is True
        assert entry["config"] == {
            "root_path": "knowledge",
            NATIVE_PROJECT_CONFIG_KEY: PROJECT,
        }

    @pytest.mark.asyncio
    async def test_work_older_than_its_projects_connector(self):
        store = _Store({PROJECT: _row()})

        result = await with_implied_project_knowledge(
            None,
            store=store,
            project_ids=[PROJECT],
            created_at=ROW_CREATED - timedelta(days=40),
        )

        (entry,) = result
        assert entry[IMPLIED_KEY] == PREDATES_CONNECTOR
        assert entry["datasource_id"] == ROW_ID
        assert entry["name"] == "Thesis Knowledge"
        assert entry["config"] == {
            "root_path": "vault",
            NATIVE_PROJECT_CONFIG_KEY: PROJECT,
        }

    @pytest.mark.asyncio
    async def test_a_naive_timestamp_is_read_as_utc(self):
        store = _Store({PROJECT: _row()})

        result = await with_implied_project_knowledge(
            [],
            store=store,
            project_ids=[PROJECT],
            created_at=(ROW_CREATED - timedelta(hours=1)).replace(tzinfo=None),
        )

        assert [entry[IMPLIED_KEY] for entry in result] == [PREDATES_CONNECTOR]

    @pytest.mark.asyncio
    @pytest.mark.parametrize("row", [None, _row()], ids=["no-connector", "older"])
    async def test_the_agent_binds_it_as_the_writable_project_knowledge_base(self, row):
        """End to end with the agent's builder: the implied entry is what the
        project's own connector would have been."""
        store = _Store({PROJECT: row})

        result = await with_implied_project_knowledge(
            [_database()],
            store=store,
            project_ids=[PROJECT],
            created_at=ROW_CREATED - timedelta(days=1),
        )
        bindings = build_knowledge_bindings(project_ids=[PROJECT], datasources=result)

        assert len(bindings) == 1
        assert str(bindings[0].kb_id) == PROJECT
        assert bindings[0].kind == "native"
        assert bindings[0].writable is True

    @pytest.mark.asyncio
    async def test_each_project_is_judged_on_its_own(self):
        """A two-project session: one project has had its connector all along
        (its absence is a choice), the other has none."""
        store = _Store({PROJECT: _row(), OTHER_PROJECT: None})

        result = await with_implied_project_knowledge(
            [],
            store=store,
            project_ids=[PROJECT, OTHER_PROJECT, PROJECT],
            created_at=ROW_CREATED + timedelta(days=1),
        )

        assert [e["config"][NATIVE_PROJECT_CONFIG_KEY] for e in result] == [
            OTHER_PROJECT
        ]
        assert store.asked == [PROJECT, OTHER_PROJECT]


class TestNothingToDecide:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("project_ids", [None, [], [None, ""]])
    async def test_work_without_a_project(self, project_ids):
        store = _Store({})
        payload = [_database()]

        result = await with_implied_project_knowledge(
            payload, store=store, project_ids=project_ids, created_at=ROW_CREATED
        )

        assert result is payload
        assert store.asked == []

    @pytest.mark.asyncio
    async def test_a_malformed_project_id_decides_nothing(self):
        store = _Store({})
        payload = [_database()]

        result = await with_implied_project_knowledge(
            payload, store=store, project_ids=["not-a-uuid"], created_at=ROW_CREATED
        )

        assert result is payload
        assert store.asked == []

    @pytest.mark.asyncio
    async def test_a_store_without_the_lookup_adds_nothing(self):
        payload = [_database()]

        result = await with_implied_project_knowledge(
            payload, store=object(), project_ids=[PROJECT], created_at=ROW_CREATED
        )

        assert result is payload

    @pytest.mark.asyncio
    @pytest.mark.parametrize("store", [MagicMock(), AsyncMock()], ids=["sync", "async"])
    async def test_a_mock_store_adds_nothing(self, store):
        """Neither a row nor ``None``: the helper cannot tell, so it leaves
        the selection as it is."""
        payload = [_database()]

        result = await with_implied_project_knowledge(
            payload, store=store, project_ids=[PROJECT], created_at=ROW_CREATED
        )

        assert result is payload

    @pytest.mark.asyncio
    async def test_unknown_creation_time_with_an_existing_connector(self):
        """Without a creation time the work cannot be shown to be older."""
        store = _Store({PROJECT: _row()})

        result = await with_implied_project_knowledge(
            [], store=store, project_ids=[PROJECT], created_at=None
        )

        assert result == []


# =============================================================================
# Wiring: the job start bundle and the resume payload carry the entry
# =============================================================================


def _implied(datasources) -> list[dict]:
    return [entry for entry in datasources or [] if entry.get(IMPLIED_KEY)]


class TestJobDeliveryCarriesIt:
    async def _start(self, lookup: AsyncMock, **job_fields):
        import orchestrator.main as orch_main
        from tests import _b09_control_seams as control_seams
        from tests.test_pr_authority_payload import _job, _repository_row
        from tests.test_workspace_ssh_delivery_wiring import _dispatch_patches

        with ExitStack() as stack:
            _dispatch_patches(stack, _repository_row())
            stack.enter_context(
                patch.object(
                    orch_main.app.state.resources.postgres_db,
                    "get_datasource_by_managed_key",
                    lookup,
                )
            )
            request = await control_seams.build_job_start_request(
                {**_job(), **job_fields}
            )
        assert request is not None
        return request

    @pytest.mark.asyncio
    async def test_a_job_in_a_project_without_a_connector(self):
        from tests.test_pr_authority_payload import PROJECT_ID

        lookup = AsyncMock(return_value=None)

        request = await self._start(lookup, created_at=ROW_CREATED)

        lookup.assert_awaited_once_with(f"project-kb:{PROJECT_ID}")
        (entry,) = _implied(request.datasources)
        assert entry[IMPLIED_KEY] == NO_CONNECTOR
        assert entry["config"][NATIVE_PROJECT_CONFIG_KEY] == PROJECT_ID
        # The selected connector is still there, first.
        assert len(request.datasources) == 2
        assert IMPLIED_KEY not in request.datasources[0]
        # What the agent builds from it: the project's writable knowledge base.
        (binding,) = build_knowledge_bindings(
            project_ids=[PROJECT_ID], datasources=request.datasources
        )
        assert binding.kind == "native" and binding.writable is True

    @pytest.mark.asyncio
    async def test_a_job_older_than_its_projects_connector(self):
        lookup = AsyncMock(return_value=_row())

        request = await self._start(lookup, created_at=ROW_CREATED - timedelta(days=3))

        (entry,) = _implied(request.datasources)
        assert entry[IMPLIED_KEY] == PREDATES_CONNECTOR

    @pytest.mark.asyncio
    async def test_a_new_job_that_left_the_connector_out_gets_nothing(self):
        """The thesis job: created after the connector existed, with its own
        connector list. The start bundle carries that list and no more."""
        from tests.test_pr_authority_payload import PROJECT_ID

        lookup = AsyncMock(return_value=_row())

        request = await self._start(lookup, created_at=ROW_CREATED + timedelta(days=3))

        assert _implied(request.datasources) == []
        assert len(request.datasources) == 1
        assert (
            build_knowledge_bindings(
                project_ids=[PROJECT_ID], datasources=request.datasources
            )
            == []
        )

    @pytest.mark.asyncio
    async def test_a_paused_job_resumes_with_it(self):
        import orchestrator.main as orch_main
        from tests.test_connector_lease_delivery_wiring import _issued, _resume
        from tests.test_pr_authority_payload import PROJECT_ID, _Client

        lookup = AsyncMock(return_value=None)
        with patch.object(
            orch_main.app.state.resources.postgres_db,
            "get_datasource_by_managed_key",
            lookup,
        ):
            assert await _resume(_issued()) is True

        (posted,) = _Client.posts
        (entry,) = _implied(posted["datasources"])
        assert entry[IMPLIED_KEY] == NO_CONNECTOR
        assert entry["config"][NATIVE_PROJECT_CONFIG_KEY] == PROJECT_ID
