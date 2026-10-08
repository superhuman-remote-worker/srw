"""Representative request-body contracts for MCP job/project/connectors."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import httpx
import pytest

from shared.orch_surface.client import AsyncCockpitClient, CockpitClient
from shared.orch_surface.formatters import (
    format_connector_drivers,
    format_created_datasource,
    format_datasource_detail,
    format_datasources,
)

ROOT = Path(__file__).resolve().parents[1]
DRIVER_MATRIX = ROOT / "cockpit/src/app/core/models/fixtures/connector-drivers.json"


@pytest.mark.asyncio
async def test_project_client_forwards_external_kb_only_when_supplied() -> None:
    captured: list[tuple[str, dict[str, Any]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.url.path, json.loads(request.content)))
        return httpx.Response(201, json={"id": "project-1"})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    external = {
        "repo_url": "https://github.com/acme/vault.git",
        "branch": "main",
        "token": "test-pat",
    }
    try:
        await client.create_project("External", "user-1", external_kb=external)
        await client.create_project("Default", "user-1")
        await client.attach_project_knowledge_repository("project-2", external)
    finally:
        await client.close()

    assert captured[0] == (
        "/api/projects",
        {"name": "External", "user_id": "user-1", "external_kb": external},
    )
    assert "external_kb" not in captured[1][1]
    assert captured[2] == (
        "/api/projects/project-2/knowledge/repository",
        external,
    )


@pytest.mark.asyncio
async def test_async_job_clients_preserve_explicit_empty_connector_selection() -> None:
    captured: list[tuple[str, dict[str, Any]]] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.append((request.url.path, json.loads(request.content)))
        return httpx.Response(201, json={"id": "job-1", "status": "created"})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.create_job("root-none", datasource_ids=[])
        await client.create_project_job("project-1", "project-none", datasource_ids=[])
        await client.create_job("root-defaults")
    finally:
        await client.close()

    assert captured[0][0] == "/api/jobs"
    assert captured[0][1]["datasource_ids"] == []
    assert "use_datasource_defaults" not in captured[0][1]
    assert captured[1][0] == "/api/projects/project-1/jobs"
    assert captured[1][1]["datasource_ids"] == []
    assert "use_datasource_defaults" not in captured[1][1]
    assert "datasource_ids" not in captured[2][1]
    assert captured[2][1]["use_datasource_defaults"] is True


def test_sync_job_client_preserves_explicit_empty_connector_selection() -> None:
    captured: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content))
        return httpx.Response(201, json={"id": "job-1", "status": "created"})

    client = CockpitClient("http://orchestrator.test")
    client._client.close()
    client._client = httpx.Client(
        base_url="http://orchestrator.test",
        transport=httpx.MockTransport(handler),
    )
    try:
        client.create_job("explicit-none", datasource_ids=[])
        client.create_job("automatic-defaults")
    finally:
        client.close()

    assert captured[0]["datasource_ids"] == []
    assert "use_datasource_defaults" not in captured[0]
    assert "datasource_ids" not in captured[1]
    assert captured[1]["use_datasource_defaults"] is True


@pytest.mark.asyncio
async def test_job_creation_forwards_preferred_expert_kickoff_context_and_priority() -> (
    None
):
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(201, json={"id": "job-1", "status": "created"})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.create_project_job(
            project_id="project-1",
            description="contract job",
            expert_id="expert-1",
            kickoff_message="Begin with the supplied evidence.",
            context={"case": "alpha"},
            priority=8,
            required_deliverables=["output/report.md"],
        )
    finally:
        await client.close()

    assert captured["expert_id"] == "expert-1"
    assert captured["kickoff_message"] == "Begin with the supplied evidence."
    assert captured["context"] == {"case": "alpha"}
    assert captured["priority"] == 8
    assert captured["required_deliverables"] == ["output/report.md"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("ds_type", "fields"),
    [
        (
            "email",
            {
                "credentials": {
                    "imap": {"username": "mailbox", "password": "test-only"}
                },
                "config": {
                    "access": "draft",
                    "folders": ["INBOX"],
                    "drafts_folder": "Drafts",
                },
            },
        ),
        (
            "mcp",
            {
                "connection_url": "https://connector.invalid/mcp",
                "credentials": {"transport": "http", "token": "test-only"},
            },
        ),
        (
            "kubeconfig",
            {
                "credentials": {
                    "files": [
                        {
                            "content": "test-only-kubeconfig",
                            "target_path": "~/.kube/config",
                            "mode": "0600",
                        }
                    ]
                }
            },
        ),
        (
            "kb",
            {
                "connection_url": "https://git.invalid/knowledge.git",
                "config": {"root_path": "notes"},
                "read_only": True,
            },
        ),
    ],
)
async def test_representative_connector_contracts_are_forwarded(
    ds_type: str, fields: dict[str, Any]
) -> None:
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(201, json={"id": "connector-1", "type": ds_type})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.create_datasource(
            name=f"test-{ds_type}",
            ds_type=ds_type,
            is_global=False,
            **fields,
        )
    finally:
        await client.close()

    assert captured["type"] == ds_type
    assert captured["is_global"] is False
    for key, value in fields.items():
        assert captured[key] == value


@pytest.mark.asyncio
async def test_connector_update_can_publish_and_replace_type_config() -> None:
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(200, json={"status": "updated"})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.update_datasource(
            "connector-1",
            config={},
            is_global=True,
            read_only=True,
            scope_mode="all",
            project_ids=[],
            auto_attach=False,
            policy_revision=7,
        )
    finally:
        await client.close()

    assert captured == {
        "config": {},
        "is_global": True,
        "read_only": True,
        "scope_mode": "all",
        "project_ids": [],
        "auto_attach": False,
        "policy_revision": 7,
    }


@pytest.mark.asyncio
async def test_connector_create_forwards_project_scope_and_auto_attach() -> None:
    captured: dict[str, Any] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(json.loads(request.content))
        return httpx.Response(201, json={"id": "connector-1", "type": "postgresql"})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        await client.create_datasource(
            name="project database",
            ds_type="postgresql",
            scope_mode="projects",
            project_ids=["project-a", "project-b"],
            auto_attach=True,
        )
    finally:
        await client.close()

    assert captured["scope_mode"] == "projects"
    assert captured["project_ids"] == ["project-a", "project-b"]
    assert captured["auto_attach"] is True
    assert "job_id" not in captured


@pytest.mark.asyncio
async def test_connector_catalog_forwards_authorized_filters_and_cursor() -> None:
    captured: dict[str, str] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(
            200,
            json={
                "items": [{"id": "connector-1", "policy_revision": 9}],
                "next_cursor": "cursor-2",
            },
        )

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        page = await client.list_datasources(
            ds_type="postgresql",
            q="application",
            project_id="project-1",
            scope_mode="projects",
            auto_attach=True,
            visibility="private",
            ownership="mine",
            availability="projects",
            limit=25,
            cursor="cursor-1",
        )
    finally:
        await client.close()

    assert captured == {
        "limit": "25",
        "type": "postgresql",
        "q": "application",
        "project_id": "project-1",
        "scope_mode": "projects",
        "auto_attach": "true",
        "visibility": "private",
        "ownership": "mine",
        "availability": "projects",
        "cursor": "cursor-1",
    }
    assert page["next_cursor"] == "cursor-2"


@pytest.mark.asyncio
async def test_get_datasource_loads_exact_management_record() -> None:
    requested_path = ""
    connector_id = "11111111-2222-4333-8444-555555555555"

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requested_path
        requested_path = request.url.path
        return httpx.Response(
            200,
            json={"id": connector_id, "policy_revision": 12},
        )

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        connector = await client.get_datasource(connector_id)
    finally:
        await client.close()

    assert requested_path == f"/api/datasources/{connector_id}"
    assert connector == {"id": connector_id, "policy_revision": 12}


def test_connector_formatters_report_availability_and_default_separately() -> None:
    connector_id = "11111111-2222-4333-8444-555555555555"
    connector = {
        "id": connector_id,
        "name": "Application database",
        "type": "postgresql",
        "scope_mode": "projects",
        "project_ids": ["project-a", "project-b"],
        "auto_attach": True,
        "is_global": False,
        "read_only": False,
        "policy_revision": 17,
    }

    created = format_created_datasource(connector)
    listed = format_datasources([connector])
    detailed = format_datasource_detail(connector)

    for rendered in (created, listed, detailed):
        assert connector_id in rendered
        assert "projects (project-a, project-b)" in rendered
        assert "Auto-attach default: True" in rendered
        assert "Published: False" in rendered
        assert "Policy revision: 17" in rendered
        assert "Scope: global" not in rendered


@pytest.mark.asyncio
async def test_connector_driver_matrix_is_read_from_its_route() -> None:
    requested_path = ""

    async def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requested_path
        requested_path = request.url.path
        return httpx.Response(200, json={"protocol_version": "1.0", "drivers": []})

    client = AsyncCockpitClient(
        "http://orchestrator.test", transport=httpx.MockTransport(handler)
    )
    try:
        matrix = await client.list_connector_drivers()
    finally:
        await client.close()

    assert requested_path == "/api/datasources/drivers"
    assert matrix == {"protocol_version": "1.0", "drivers": []}


def test_connector_driver_formatter_names_what_enforces_each_level() -> None:
    matrix = json.loads(DRIVER_MATRIX.read_text())
    rendered = format_connector_drivers(matrix)

    assert f"Connector drivers ({len(matrix['drivers'])})" in rendered
    for driver in matrix["drivers"]:
        assert driver["name"] in rendered
        for level in driver["access_levels"]:
            assert level["enforced_by"] in rendered
    assert "Access ReadOnly [advisory]" in rendered
    assert "Access: none (this delivery cannot enforce a level)" in rendered
    assert "Trust: builtin  Holds upstream credentials: True" in rendered
    assert "every claim declared by its author" not in rendered
    # In-process built-ins have no driver pod: not "declared: none".
    assert "Egress: no driver pod" in rendered
    assert "Egress declared: none" not in rendered

    custom = dict(matrix["drivers"][0])
    custom["trust"] = {"tier": "custom", "claims_declared_by_author": True}
    custom["egress"] = {
        "declared": {
            "rules": [{"host": "${config.host}", "ports": [443], "protocol": "tcp"}]
        },
        "enforced": {
            "status": "not_enforced",
            "reason": "driver_hosting_not_available",
        },
    }
    rendered = format_connector_drivers({"drivers": [custom]})
    assert "Trust: custom (every claim declared by its author)" in rendered
    assert "Egress declared: ${config.host}:443/tcp  enforced: not_enforced" in rendered
    assert (
        format_connector_drivers({"drivers": []}) == "No connector drivers installed."
    )
