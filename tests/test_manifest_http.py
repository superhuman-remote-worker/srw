"""Authentication, public request shape, and side-effect-free manifest operations."""

import json
from unittest.mock import AsyncMock, Mock

from fastapi import FastAPI, HTTPException
from httpx import ASGITransport, AsyncClient
import pytest

from orchestrator.routers.manifests import ManifestDependencies, router
from orchestrator.services.manifests import ManifestService


def manifest():
    return {
        "apiVersion": "srw/v1alpha1",
        "kind": "Expert",
        "metadata": {"name": "custom-worker"},
        "spec": {
            "runtime": {"image": "example/custom:1", "config": {"customTool": None}}
        },
    }


def make_app(*, service=None, guard=None, resources=None, execution=None, trigger=None):
    app = FastAPI()
    dependencies = ManifestDependencies(
        service=service or ManifestService(),
        require_approved_user=guard or AsyncMock(return_value={"id": "approved-user"}),
        resources=resources,
        execution=execution,
        trigger_dispatch=trigger,
    )
    app.state.manifest_dependencies_factory = lambda: dependencies
    app.include_router(router)
    return app


def test_secret_request_remains_documented_without_echoing_validation_inputs():
    body = make_app().openapi()["paths"]["/api/resource-secrets/{name}"]["put"][
        "requestBody"
    ]
    schema = body["content"]["application/json"]["schema"]
    assert body["required"] is True
    assert schema["required"] == ["values"]
    assert schema["properties"]["values"]["additionalProperties"]["writeOnly"] is True
    assert "$ref" not in json.dumps(schema)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "method,path,body",
    [
        ("GET", "/api/resources", None),
        ("GET", "/api/resources/00000000-0000-0000-0000-000000000001", None),
        (
            "DELETE",
            "/api/resources/00000000-0000-0000-0000-000000000001?expected_version=1",
            None,
        ),
        ("POST", "/api/manifests/apply", {"source": "{}"}),
        (
            "PUT",
            "/api/resource-secrets/provider",
            {"values": {"key": "private-sentinel"}},
        ),
        (
            "POST",
            "/api/resources/00000000-0000-0000-0000-000000000001/outcome",
            {"attempt": 1, "outcome": "Succeeded"},
        ),
        ("GET", "/api/workspace-instances/00000000-0000-0000-0000-000000000001", None),
        (
            "DELETE",
            "/api/workspace-instances/00000000-0000-0000-0000-000000000001?expected_generation=0",
            None,
        ),
    ],
)
async def test_native_operations_require_approval_before_store_or_runtime(
    method, path, body
):
    resources, execution = Mock(), Mock()
    app = make_app(
        resources=resources,
        execution=execution,
        guard=AsyncMock(side_effect=HTTPException(403, "Denied")),
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.request(
            method, path, **({"json": body} if body is not None else {})
        )
    assert response.status_code == 403
    assert not resources.mock_calls and not execution.mock_calls


@pytest.mark.asyncio
async def test_credential_validation_never_echoes_invalid_secret_values():
    resources = Mock()
    resources.put_secret = AsyncMock(return_value={"resourceVersion": 1})
    app = make_app(resources=resources)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.put(
            "/api/resource-secrets/provider",
            json={"values": {"key": {"private-sentinel": "invalid-secret-shape"}}},
        )
        assert response.status_code == 422
        assert (
            "private-sentinel" not in response.text
            and "invalid-secret-shape" not in response.text
        )
        resources.put_secret.assert_not_awaited()
        response = await client.put(
            "/api/resource-secrets/provider",
            json={"values": {"key": "private-sentinel"}},
        )
        assert response.json() == {"resourceVersion": 1}
        assert resources.put_secret.await_args.kwargs["values"] == {
            "key": "private-sentinel"
        }


@pytest.mark.asyncio
async def test_apply_triggers_dispatch_only_after_successful_resource_commit():
    events = []
    resources = Mock()

    async def apply(*args, **kwargs):
        events.append("committed")
        return {"operationId": "receipt"}

    resources.apply = AsyncMock(side_effect=apply)
    app = make_app(resources=resources, trigger=lambda: events.append("dispatch"))
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/manifests/apply",
            json={"source": "{}", "idempotency_key": "submission"},
        )
        assert response.status_code == 200 and events == ["committed", "dispatch"]
        resources.apply.side_effect = HTTPException(409, "Conflict")
        response = await client.post("/api/manifests/apply", json={"source": "{}"})
        assert response.status_code == 409 and events == ["committed", "dispatch"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "operation,method",
    [("schema", "GET"), ("validate", "POST"), ("preview", "POST"), ("export", "POST")],
)
@pytest.mark.parametrize("status", [401, 403])
async def test_every_operation_requires_an_approved_user_before_service_access(
    operation, method, status
):
    service = Mock(spec=ManifestService)
    guard = AsyncMock(side_effect=HTTPException(status_code=status, detail="Denied"))
    app = make_app(service=service, guard=guard)
    kwargs = (
        {}
        if method == "GET"
        else {"json": {"source": json.dumps(manifest()), "format": "json"}}
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.request(method, f"/api/manifests/{operation}", **kwargs)
    assert response.status_code == status
    guard.assert_awaited_once()
    assert service.mock_calls == []


@pytest.mark.asyncio
async def test_authenticated_validate_preview_and_export_roundtrip():
    guard = AsyncMock(return_value={"id": "approved-user"})
    app = make_app(guard=guard)
    body = {"source": json.dumps(manifest()), "format": "json"}
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        schema = await client.get("/api/manifests/schema")
        assert schema.status_code == 200
        assert schema.json()["properties"]["apiVersion"]["const"] == "srw/v1alpha1"
        validation = await client.post("/api/manifests/validate", json=body)
        assert validation.status_code == 200
        assert validation.json()["valid"] is True
        preview = await client.post("/api/manifests/preview", json=body)
        assert preview.status_code == 422
        assert preview.json()["detail"]["code"] == "MissingScope"
        scoped = {**body, "default_scope": {"kind": "Account", "name": "personal"}}
        preview = await client.post("/api/manifests/preview", json=scoped)
        assert preview.status_code == 200
        result = preview.json()
        assert result["admissionReady"] is False
        assert result["effects"] == []
        assert result["resolved"][0]["spec"]["runtime"]["config"] == {
            "customTool": None
        }
        exported = await client.post(
            "/api/manifests/export", json={**scoped, "output_format": "yaml"}
        )
        assert exported.status_code == 200
        again = await client.post(
            "/api/manifests/preview",
            json={"source": exported.json()["source"], "format": "yaml"},
        )
        assert again.status_code == 200
        assert again.json() == result
    assert guard.await_count == 6


@pytest.mark.asyncio
async def test_malformed_source_has_structured_diagnostics_without_echoed_values():
    async with AsyncClient(
        transport=ASGITransport(app=make_app()), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/manifests/validate",
            json={"source": "kind: Expert\nkind: PRIVATE-SENTINEL"},
        )
    assert response.status_code == 422
    assert response.json()["detail"] == {
        "code": "DuplicateKey",
        "message": "Duplicate object key.",
        "document": 1,
        "path": "/",
    }
    assert "PRIVATE-SENTINEL" not in response.text


@pytest.mark.asyncio
async def test_request_model_rejects_undeclared_controls_and_invalid_formats():
    async with AsyncClient(
        transport=ASGITransport(app=make_app()), base_url="http://test"
    ) as client:
        for extra in ({"apply": True}, {"actor_id": "someone-else"}, {"format": "xml"}):
            response = await client.post(
                "/api/manifests/preview",
                json={"source": json.dumps(manifest()), **extra},
            )
            assert response.status_code == 422


@pytest.mark.asyncio
async def test_bundle_preview_does_not_claim_live_scope_or_secret_authorization():
    doc = manifest()
    doc["metadata"]["scope"] = {"kind": "Account", "name": "declared-scope"}
    doc["spec"]["runtime"]["env"] = {
        "TOKEN": {"secretRef": {"name": "auth", "key": "token"}}
    }
    async with AsyncClient(
        transport=ASGITransport(app=make_app()), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/manifests/preview", json={"source": json.dumps(doc), "format": "json"}
        )
    assert response.status_code == 200
    result = response.json()
    assert result["admissionReady"] is False
    assert {"resourceAuthorization", "credentialDelivery"} <= set(
        result["pendingChecks"]
    )
    assert result["resolved"][0]["spec"]["runtime"]["env"]["TOKEN"] == {
        "secretRef": {"name": "auth", "key": "token", "scope": doc["metadata"]["scope"]}
    }


@pytest.mark.asyncio
async def test_main_mount_uses_the_real_approved_user_dependency(monkeypatch):
    from orchestrator import main

    guard = AsyncMock(return_value={"id": "approved-user"})
    monkeypatch.setattr(main, "require_approved_user", guard)
    async with AsyncClient(
        transport=ASGITransport(app=main.app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/api/manifests/validate",
            json={"source": json.dumps(manifest()), "format": "json"},
        )
    assert response.status_code == 200
    assert response.json()["valid"] is True
    guard.assert_awaited_once()
    assert guard.await_args.args[1] is main.postgres_db


def _secret_project_rows(project_id, owner_id):
    """A migrated Project resource and its managed child Expert, as stored:
    both documents carry the project override verbatim in the Expert's
    ``runtime.config.layers``."""
    from uuid import UUID, uuid4

    from orchestrator.services.manifest_experts import (
        expert_manifest,
        project_expert_resource,
    )
    from orchestrator.services.manifest_projects import project_document
    from shared.manifests.resolution import content_revision

    raw = {
        "id": uuid4(),
        "name": "builder",
        "display_name": "Builder",
        "expert_type": "worker",
        "owner_id": UUID(owner_id),
        "is_global": False,
        "icon": "build",
        "color": "#123456",
        "tags": ["worker"],
        "config": {"settings": {"expert": True}},
        "prompts": {},
        "default_for": "worker",
        "config_override": {"llm": {"api_key": "sk-link-secret"}},
    }
    document = expert_manifest(raw, image="trusted/srw:v1")
    source = project_expert_resource(
        raw,
        {
            "id": uuid4(),
            "document": document,
            "revision": content_revision(document["spec"]),
            "resource_version": 1,
        },
    )
    shared = {
        "llm": {"model": "m", "api_key": "sk-project-secret"},
        "workspace": {"remote": {"host": "10.0.0.7"}},
    }
    project_doc, _ = project_document(
        {"id": project_id, "name": "P", "default_config_override": shared},
        owner_id=owner_id,
        experts=[source],
    )
    alias, entry = next(iter(project_doc["spec"]["resources"]["experts"].items()))
    child_doc = {
        "apiVersion": project_doc["apiVersion"],
        "kind": "Expert",
        "metadata": {"name": alias, "scope": {"kind": "Project", "name": project_id}},
        "spec": entry["inline"],
    }

    def row(kind, document, linked_id):
        return {
            "id": uuid4(),
            "kind": kind,
            "name": document["metadata"]["name"],
            "linked_id": linked_id,
            "document": document,
            "resource_version": 1,
            "revision": "r1",
            "active_revision": "r1",
        }

    return row("Project", project_doc, project_id), row("Expert", child_doc, raw["id"])


RESOURCE_SECRETS = ("sk-project-secret", "sk-link-secret", "10.0.0.7")


class TestResourceReadRedaction:
    """``/api/resources`` grants a Project viewer a read, and served the stored
    document whole — the same override layers ``GET /api/projects`` redacts.
    Below owner (and admin) a reader now gets that same redaction."""

    PROJECT_ID = "00000000-0000-0000-0000-00000000aa01"
    OWNER_ID = "00000000-0000-0000-0000-00000000aa02"
    VIEWER_ID = "00000000-0000-0000-0000-00000000aa03"

    def _app(self, caller_id, role):
        from orchestrator.services.manifest_resources import ManifestResourceService

        project_row, child_row = _secret_project_rows(self.PROJECT_ID, self.OWNER_ID)
        db = Mock()
        db.get_project = AsyncMock(
            return_value={"id": self.PROJECT_ID, "status": "active"}
        )
        db.get_user_role_in_project = AsyncMock(return_value=role)
        db.get_projects_for_user = AsyncMock(return_value=[{"id": self.PROJECT_ID}])
        db.get_expert_visible_by_id = AsyncMock(return_value={"is_global": False})
        service = ManifestResourceService(db)
        rows = {str(project_row["id"]): project_row, str(child_row["id"]): child_row}
        service.store.by_id = AsyncMock(side_effect=lambda uid: rows.get(str(uid)))
        service.store.list_scope = AsyncMock(return_value=[project_row, child_row])
        app = make_app(
            resources=service,
            guard=AsyncMock(return_value={"id": caller_id, "is_admin": False}),
        )
        return app, project_row, child_row

    async def _get(self, app, path):
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            return await client.get(path)

    @pytest.mark.asyncio
    async def test_a_viewer_reads_the_project_and_child_redacted(self):
        from orchestrator.security.access import redact_public_config_override

        app, project_row, child_row = self._app(self.VIEWER_ID, "viewer")
        for row in (project_row, child_row):
            response = await self._get(app, f"/api/resources/{row['id']}")
            assert response.status_code == 200
            for secret in RESOURCE_SECRETS:
                assert secret not in response.text
            # Byte-identical to what public_project serves the same reader.
            assert response.json()["resource"] == redact_public_config_override(
                row["document"]
            )
        listing = await self._get(
            app, f"/api/resources?scope_kind=Project&scope_name={self.PROJECT_ID}"
        )
        assert listing.status_code == 200
        assert len(listing.json()["resources"]) == 2
        for secret in RESOURCE_SECRETS:
            assert secret not in listing.text

    @pytest.mark.asyncio
    async def test_stored_preview_redacts_what_resolution_inlined(self):
        """Resolution inlines referenced stored resources after a READ check,
        so a viewer's stored preview could carry the project's layers."""
        from types import SimpleNamespace

        from orchestrator.services.manifest_store import resource_key

        app, _project_row, child_row = self._app(self.VIEWER_ID, "viewer")
        service = app.state.manifest_dependencies_factory().resources
        doc = child_row["document"]
        service.resolve = AsyncMock(
            return_value=SimpleNamespace(
                documents=[doc],
                prepared={resource_key(doc): {"resolved": doc}},
                observed=[],
                plan_revision=lambda _scope: "plan",
            )
        )
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                "/api/manifests/preview",
                json={
                    "source": json.dumps(doc),
                    "format": "json",
                    "resolution": "stored",
                },
            )
        assert response.status_code == 200
        resolved = json.dumps(response.json()["resolved"])
        for secret in RESOURCE_SECRETS:
            assert secret not in resolved

    @pytest.mark.asyncio
    async def test_an_apply_replay_echo_is_redacted_below_owner(self):
        from contextlib import asynccontextmanager

        from shared.manifests.resolution import content_revision

        app, project_row, _child = self._app(self.VIEWER_ID, "viewer")
        service = app.state.manifest_dependencies_factory().resources
        doc = project_row["document"]

        @asynccontextmanager
        async def transaction_scope():
            yield

        service.db.transaction_scope = transaction_scope
        service.store.lock_catalog = AsyncMock()
        revision = content_revision(
            {
                "documents": [doc],
                "scope": None,
                "expectedVersions": {},
                "planRevision": None,
            }
        )
        service.db.fetchrow = AsyncMock(
            return_value={
                "request_revision": revision,
                "result": {
                    "resources": [{"uid": str(project_row["id"]), "resource": doc}]
                },
            }
        )
        result = await service.apply(
            json.dumps(doc),
            {"id": self.VIEWER_ID, "is_admin": False},
            format="json",
            idempotency_key="replayed",
        )
        echoed = json.dumps(result)
        for secret in RESOURCE_SECRETS:
            assert secret not in echoed

    @pytest.mark.asyncio
    async def test_the_owner_keeps_full_fidelity_for_export(self):
        app, project_row, child_row = self._app(self.OWNER_ID, "owner")
        for row in (project_row, child_row):
            response = await self._get(app, f"/api/resources/{row['id']}")
            assert response.status_code == 200
            assert response.json()["resource"] == json.loads(
                json.dumps(row["document"])
            )
