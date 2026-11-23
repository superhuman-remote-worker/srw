"""The orchestrator's side of MCP server questions (Phase C).

knowledge-base/knowledge/features/mcp_client_2026_07_28_elicitation.md, owner
decisions of 2026-11-17:

- each MCP connector's ``config.server_questions`` (decline / accept), with
  an admin default for a connector that has no choice of its own;
- what the agent receives: the setting the connector runs with, a Session's
  own "Always accept in this session" first;
- the approve endpoint deciding a server's question: the answer must fit
  the form, a yes/no accepts with its yes, and "Always accept" is kept on
  the Session by the connector's id.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from orchestrator.services.connector_drivers import builtin_connector_drivers
from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DeploymentGates,
    delivered_server_questions,
)
from orchestrator.services.connector_drivers.managed_mcp import ManagedMcpDriver
from orchestrator.services.thread_datasource_authorization import (
    stamp_session_server_questions,
)
from orchestrator.services.thread_permissions import decide_permission_request
from shared.connectors.builtin import GITEA_MCP_SPEC
from tests._mounted_router import mount_router

CONNECTOR = "66666666-7777-4888-8999-aaaaaaaaaaaa"
THREAD = "11111111-2222-4333-8444-555555555555"
GITEA_IMAGE = "docker.gitea.com/gitea-mcp-server:1.8.0@sha256:" + "a" * 64


def _context() -> BindContext:
    return BindContext(
        gates=DeploymentGates(mcp_datasources_enabled=lambda: True),
        logger=logging.getLogger(__name__),
        default_known_hosts="",
    )


def _remote_row(**over: Any) -> dict[str, Any]:
    row = {
        "id": CONNECTOR,
        "type": "mcp",
        "name": "Civitai",
        "description": None,
        "connection_url": "https://mcp.civitai.example/mcp",
        "credentials": {"transport": "http"},
        "config": {},
        "project_read_only": False,
    }
    row.update(over)
    return row


# -- what the agent receives --------------------------------------------------


@pytest.mark.parametrize(
    ("config", "default", "session", "delivered"),
    [
        ({}, None, None, "decline"),
        ({}, "accept", None, "accept"),
        ({"server_questions": "decline"}, "accept", None, "decline"),
        ({"server_questions": "accept"}, "decline", None, "accept"),
        ({"server_questions": "decline"}, "decline", "accept", "accept"),
        (None, "bogus", None, "decline"),
    ],
)
def test_the_setting_delivered_is_session_then_connector_then_default(
    config, default, session, delivered
):
    row = _remote_row(config=config)
    if default is not None:
        row["_server_questions_default"] = default
    if session is not None:
        row["_session_server_questions"] = session
    assert delivered_server_questions(row) == delivered

    entry = (
        builtin_connector_drivers()
        .for_type("mcp")
        .bind(row, row["credentials"], ctx=_context())
    )
    assert entry["server_questions"] == delivered
    assert entry["datasource_id"] == CONNECTOR
    # The Session's own "Always accept" is marked, so the model is told why.
    assert entry.get("server_questions_by_session", False) is (session == "accept")


def test_a_managed_server_carries_the_setting_beside_its_config():
    driver = ManagedMcpDriver(GITEA_MCP_SPEC, GITEA_IMAGE)
    row = {
        "id": CONNECTOR,
        "type": "gitea_mcp",
        "name": "Gitea",
        "credentials": {"token": "t"},
        "config": {"url": "https://g.example", "server_questions": "accept"},
        "project_read_only": False,
    }
    entry = driver.bind(row, row["credentials"], ctx=_context())
    assert entry["server_questions"] == "accept"


@pytest.mark.asyncio
async def test_a_managed_connector_stores_the_setting_outside_its_schema():
    driver = ManagedMcpDriver(GITEA_MCP_SPEC, GITEA_IMAGE)
    draft = ConnectorDraft(
        name="Gitea",
        connection_url=None,
        credentials={"token": "gitea-token"},
        config={"url": "https://g.example", "server_questions": "accept"},
        read_only=None,
        is_global=None,
        default_branch=None,
    )
    normalized = await driver.validate(draft, existing=None, ctx=MagicMock())
    assert normalized.config["server_questions"] == "accept"
    assert normalized.config["host"] == "g.example"

    bad = ConnectorDraft(
        **{
            **draft.__dict__,
            "config": {"url": "https://g.example", "server_questions": "yes"},
        }
    )
    with pytest.raises(HTTPException) as refused:
        await driver.validate(bad, existing=None, ctx=MagicMock())
    assert refused.value.status_code == 400


def test_a_sessions_always_accept_is_stamped_by_connector_id():
    rows = [_remote_row(), _remote_row(id="other")]
    thread = {"metadata": json.dumps({"mcp_server_questions": {CONNECTOR: "accept"}})}
    stamp_session_server_questions(thread, rows)
    assert rows[0]["_session_server_questions"] == "accept"
    assert "_session_server_questions" not in rows[1]

    untouched = [_remote_row()]
    stamp_session_server_questions({"metadata": None}, untouched)
    assert "_session_server_questions" not in untouched[0]


# -- the approve endpoint deciding a question ---------------------------------


class _Conn:
    def __init__(self, row: dict[str, Any] | None):
        self.row = row
        self.updates: list[tuple] = []
        self.executed: list[tuple] = []

    def transaction(self):
        conn = self

        class _Tx:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return None

        return _Tx()

    async def fetchrow(self, sql: str, *args):
        if sql.lstrip().startswith("SELECT"):
            return self.row
        self.updates.append((sql, args))
        return {"id": args[0], "status": args[1], "tool_call_id": "call-9"}

    async def execute(self, sql: str, *args):
        self.executed.append((sql, args))


class _Store:
    def __init__(self, conn: _Conn):
        self.conn = conn

    def acquire(self):
        conn = self.conn

        class _Acquire:
            async def __aenter__(self):
                return conn

            async def __aexit__(self, *exc):
                return None

        return _Acquire()


CONFIRM_SCHEMA = {
    "type": "object",
    "properties": {"value": {"type": "boolean"}},
    "required": ["value"],
}
PICK_SCHEMA = {
    "type": "object",
    "required": ["size"],
    "properties": {"size": {"type": "string", "enum": ["small", "large"]}},
}


def _question_row(**request: Any) -> dict[str, Any]:
    body = {
        "server": "Civitai",
        "datasource_id": CONNECTOR,
        "mode": "form",
        "schema": CONFIRM_SCHEMA,
        "confirmation": {"value": True},
    }
    body.update(request)
    return {
        "id": "row-q",
        "status": "pending",
        "tool_call_id": "call-9",
        "kind": "question",
        "request": json.dumps(body),
    }


async def _decide(row, decision, **kwargs):
    conn = _Conn(row)
    outcome = await decide_permission_request(
        _Store(conn), THREAD, "row-q", decision, decided_by="user-1", **kwargs
    )
    return outcome, conn


def _response(conn: _Conn) -> Any:
    ((_sql, args),) = conn.updates
    return json.loads(args[3]) if args[3] is not None else None


@pytest.mark.asyncio
async def test_accepting_a_yes_no_sends_its_yes_and_keeps_always_accept():
    outcome, conn = await _decide(_question_row(), "approve", always_accept=True)
    assert outcome["status"] == "approved"
    assert _response(conn) == {"content": {"value": True}, "always_accept": True}
    ((sql, args),) = conn.executed
    assert "mcp_server_questions" in sql
    assert args == (THREAD, CONNECTOR)


@pytest.mark.asyncio
async def test_a_form_answer_must_fit_and_never_turns_on_always_accept():
    row = _question_row(schema=PICK_SCHEMA, confirmation=None)
    with pytest.raises(HTTPException) as misfit:
        await _decide(row, "approve", content={"size": "huge"})
    assert misfit.value.status_code == 422
    assert misfit.value.detail == "The answer 'size' is not one of the choices."

    with pytest.raises(HTTPException) as refused:
        await _decide(row, "approve", content={"size": "small"}, always_accept=True)
    assert refused.value.status_code == 400

    outcome, conn = await _decide(row, "approve", content={"size": "small"})
    assert _response(conn) == {"content": {"size": "small"}, "always_accept": False}
    assert conn.executed == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("decision", "status"), [("deny", "denied"), ("cancel", "expired")]
)
async def test_decline_and_dismiss_send_no_content(decision, status):
    outcome, conn = await _decide(_question_row(), decision)
    assert outcome["status"] == status
    assert _response(conn) == {"content": None, "always_accept": False}


@pytest.mark.asyncio
async def test_a_url_question_accepts_without_content():
    row = _question_row(mode="url", schema=None, confirmation=None, url="https://x")
    _outcome, conn = await _decide(row, "approve")
    assert _response(conn) == {"content": None, "always_accept": False}


@pytest.mark.asyncio
async def test_a_tool_approval_has_no_cancel_and_stores_no_response():
    tool_row = {
        "id": "row-q",
        "status": "pending",
        "tool_call_id": "c",
        "kind": "tool",
        "request": None,
    }
    with pytest.raises(HTTPException) as refused:
        await _decide(tool_row, "cancel")
    assert refused.value.status_code == 400
    _outcome, conn = await _decide(tool_row, "approve", content={"ignored": 1})
    assert _response(conn) is None


# -- the admin default ----------------------------------------------------------

PATH = "/api/admin/system-settings/mcp_server_questions"
UPDATED_AT = datetime(2026, 11, 17, 9, 0, tzinfo=timezone.utc)


def _client(row=None):
    from orchestrator.routers.system_settings import (
        SystemSettingsDependencies as RouteDeps,
    )
    from orchestrator.routers.system_settings import router
    from orchestrator.services.system_settings import (
        SystemSettingsDependencies as OpDeps,
    )

    async def upsert(key, value, *, updated_by):
        return {"value": value, "updated_at": UPDATED_AT, "updated_by": updated_by}

    store = SimpleNamespace(
        get_system_setting=AsyncMock(return_value=row),
        upsert_system_setting=AsyncMock(side_effect=upsert),
    )

    async def require_admin(_request):
        return {"id": "admin-1", "email": "admin@test"}

    deps = RouteDeps(operations=OpDeps(store=store), require_admin=require_admin)
    app = mount_router(
        router, factories={"system_settings_dependencies_factory": lambda: deps}
    )
    return TestClient(app), store


def test_the_admin_default_reads_decline_until_set():
    client, store = _client()
    assert client.get(PATH).json() == {
        "default": "decline",
        "updated_at": None,
        "updated_by": None,
    }
    store.get_system_setting.assert_awaited_once_with("mcp.server_questions_default")


def test_the_admin_default_is_set_and_checked():
    client, store = _client()
    answer = client.put(PATH, json={"default": "accept"})
    assert answer.status_code == 200
    assert answer.json()["default"] == "accept"
    assert answer.json()["updated_by"] == "admin@test"
    store.upsert_system_setting.assert_awaited_once()
    assert store.upsert_system_setting.await_args.args == (
        "mcp.server_questions_default",
        {"default": "accept"},
    )
    assert client.put(PATH, json={"default": "always"}).status_code == 400
