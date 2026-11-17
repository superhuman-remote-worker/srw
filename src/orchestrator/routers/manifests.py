"""Authenticated manifest desired-state and execution operations."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
import json
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, Query, Response
from starlette.concurrency import run_in_threadpool

from orchestrator.schemas.manifests import (
    ManifestExportInput,
    ManifestInput,
    ManifestPreviewInput,
    ManifestApplyInput,
    ManifestSecretInput,
    ManifestOutcomeInput,
    ManifestScope,
)
from orchestrator.services.manifests import ManifestService
from shared.manifests import ManifestError, load_schema, parse_sources
from shared.manifests.validation import MAX_SOURCE_BYTES

router = APIRouter(tags=["Manifests"])

#: The kinds a manifest can declare, in the schema's order.
RESOURCE_KINDS: tuple[str, ...] = tuple(load_schema()["properties"]["kind"]["enum"])


@dataclass(frozen=True)
class ManifestDependencies:
    service: ManifestService
    require_approved_user: Callable[[Request], Awaitable[dict[str, Any]]]
    # Composition supplies live resource operations. The schema/preview router
    # remains importable without loading database authority or the SRW adapter.
    resources: Any | None = None
    trigger_dispatch: Callable[[], None] | None = None
    execution: Callable[[], Any] | None = None


def get_manifest_dependencies(request: Request) -> ManifestDependencies:
    return request.app.state.manifest_dependencies_factory()


def _refused(exc: ManifestError, origins) -> HTTPException:
    """The 422 for a manifest error, naming the source when there were several."""
    if origins:
        exc = exc.located(origins)
    return HTTPException(status_code=422, detail=exc.as_dict())


async def _source(body: ManifestInput):
    """What to hand a service as its source, and where each document came from.

    One text goes through unparsed. Several are read here as one bundle
    (``shared.manifests.parse_sources``); the services accept that bundle in
    place of the text. The second value is None for one text, and otherwise
    the bundle's origins, which turn an error's document number back into a
    source name.
    """
    if body.sources is None:
        return body.source, None
    try:
        bundle = await run_in_threadpool(
            parse_sources, [item.model_dump() for item in body.sources]
        )
    except ManifestError as exc:
        raise _refused(exc, None) from None
    return bundle, bundle.origins


async def _call(operation, *args, origins=None, **kwargs):
    try:
        return await run_in_threadpool(operation, *args, **kwargs)
    except ManifestError as exc:
        raise _refused(exc, origins) from None


@router.get("/api/manifests/schema")
async def manifest_schema(
    request: Request,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """Return the packaged v1alpha1 authored-resource JSON Schema."""
    await dependencies.require_approved_user(request)
    return await _call(dependencies.service.schema)


@router.post("/api/manifests/validate")
async def validate_manifests(
    request: Request,
    body: ManifestInput,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """Check JSON/YAML structure and project-local aliases without resolving access."""
    await dependencies.require_approved_user(request)
    source, origins = await _source(body)
    result = await _call(
        dependencies.service.validate, source, format=body.format, origins=origins
    )
    if origins:
        for document, (name, _) in zip(result["documents"], origins):
            document["source"] = name
    return result


@router.post("/api/manifests/preview")
async def preview_manifests(
    request: Request,
    body: ManifestPreviewInput,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """Resolve a supplied bundle or inspect authorized stored dependencies.

    Neither preview mode starts work or issues credentials. Stored preview
    returns a dependency plan digest that apply can check for intervening edits.
    """
    user = await dependencies.require_approved_user(request)
    source, origins = await _source(body)
    if body.resolution == "stored":
        return await _resource_call(
            _resources(dependencies).preview,
            source,
            user,
            request=request,
            format=body.format,
            default_scope=body.default_scope.model_dump()
            if body.default_scope
            else None,
            origins=origins,
        )
    return await _call(
        dependencies.service.preview,
        source,
        format=body.format,
        default_scope=body.default_scope.model_dump() if body.default_scope else None,
        origins=origins,
    )


def _resources(dependencies):
    if dependencies.resources is None:
        raise HTTPException(503, "Manifest resource store is unavailable.")
    return dependencies.resources


async def _resource_call(operation, *args, origins=None, **kwargs):
    try:
        return await operation(*args, **kwargs)
    except ManifestError as exc:
        raise _refused(exc, origins) from None


@router.post("/api/manifests/apply")
async def apply_manifests(
    request: Request,
    body: ManifestApplyInput,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    source, origins = await _source(body)
    result = await _resource_call(
        _resources(dependencies).apply,
        source,
        user,
        request=request,
        format=body.format,
        default_scope=body.default_scope.model_dump() if body.default_scope else None,
        expected_versions=body.expected_versions,
        plan_revision=body.plan_revision,
        idempotency_key=body.idempotency_key,
        origins=origins,
    )
    if dependencies.trigger_dispatch:
        dependencies.trigger_dispatch()
    return result


@router.get("/api/resources")
async def list_resources(
    request: Request,
    scope_kind: str = "Account",
    scope_name: str = "me",
    kind: str | None = None,
    name: str | None = Query(default=None, min_length=1, max_length=253),
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """List the stored resources of one scope that the caller may see.

    ``kind`` and ``name`` narrow the list; together they identify at most one
    resource. An unknown ``kind`` is refused, because an empty list would
    read as "none stored". ``truncated`` is true when the scope holds more
    resources than one call returns; narrow the list with ``kind`` or
    ``name`` then.
    """
    user = await dependencies.require_approved_user(request)
    try:
        scope = ManifestScope(kind=scope_kind, name=scope_name).model_dump()
    except ValueError:
        raise HTTPException(422, "Invalid resource scope.") from None
    if kind is not None and kind not in RESOURCE_KINDS:
        raise HTTPException(
            422, f"Unknown resource kind. Choose one of: {', '.join(RESOURCE_KINDS)}."
        )
    return await _resource_call(
        _resources(dependencies).list,
        user,
        scope=scope,
        kind=kind,
        name=name,
        request=request,
    )


@router.get("/api/resources/{resource_id}")
async def get_resource(
    resource_id: UUID,
    request: Request,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    return await _resource_call(
        _resources(dependencies).get, resource_id, user, request=request
    )


@router.delete("/api/resources/{resource_id}")
async def delete_resource(
    resource_id: UUID,
    request: Request,
    response: Response,
    expected_version: int = Query(ge=1),
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """Delete a resource. A Project's delete answers 202 (``"deleting":
    true``) when it was decided but finishes in the background."""
    user = await dependencies.require_approved_user(request)
    result = await _resource_call(
        _resources(dependencies).delete,
        resource_id,
        user,
        expected_version=expected_version,
        request=request,
    )
    if isinstance(result, dict) and result.get("deleting"):
        response.status_code = 202
    return result


def _secret_request_schema():
    schema = ManifestSecretInput.model_json_schema()
    definitions = schema.pop("$defs", {})

    def inline(value):
        if isinstance(value, dict):
            if "$ref" in value:
                return inline(definitions[value["$ref"].rsplit("/", 1)[-1]])
            return {key: inline(item) for key, item in value.items()}
        if isinstance(value, list):
            return [inline(item) for item in value]
        return value

    return inline(schema)


@router.put(
    "/api/resource-secrets/{name}",
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": _secret_request_schema()}},
        }
    },
)
async def put_resource_secret(
    name: str,
    request: Request,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    # FastAPI's general validation response includes invalid input. Credential
    # writes use an explicitly sanitized boundary, including invalid JSON.
    try:
        raw = await request.body()
        if len(raw) > MAX_SOURCE_BYTES:
            raise ValueError("Credential request too large")
        body = ManifestSecretInput.model_validate(json.loads(raw))
    except (ValueError, TypeError):
        raise HTTPException(422, "Invalid credential request.") from None
    try:
        ManifestScope(kind="Account", name=name)
    except ValueError:
        raise HTTPException(422, "Invalid credential name.") from None
    return await _resource_call(
        _resources(dependencies).put_secret,
        user,
        scope=body.scope.model_dump() if body.scope else None,
        name=name,
        values={key: value.get_secret_value() for key, value in body.values.items()},
        expected_version=body.expected_version,
        request=request,
    )


def _execution(dependencies):
    if dependencies.execution is None:
        raise HTTPException(503, "Manifest execution hosting is unavailable.")
    return dependencies.execution()


@router.post("/api/resources/{resource_id}/outcome")
async def report_resource_outcome(
    resource_id: UUID,
    request: Request,
    body: ManifestOutcomeInput,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    result = await _execution(dependencies).report_outcome(
        resource_id, user, attempt=body.attempt, outcome=body.outcome, request=request
    )
    if dependencies.trigger_dispatch:
        dependencies.trigger_dispatch()
    return result


@router.get("/api/workspace-instances/{instance_id}")
async def get_workspace_instance(
    instance_id: UUID,
    request: Request,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    return await _execution(dependencies).workspace.view(
        instance_id, user, request=request
    )


@router.get("/api/workspace-cache")
async def list_workspace_cache(
    request: Request,
    scope_kind: Literal["Account", "Project"] = "Account",
    scope_name: str = "me",
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    return await _execution(dependencies).workspace.preparation_cache(
        user,
        {"kind": scope_kind, "name": scope_name},
        request=request,
    )


@router.delete("/api/workspace-cache/{artifact_id}")
async def delete_workspace_cache(
    artifact_id: UUID,
    request: Request,
    scope_kind: Literal["Account", "Project"] = "Account",
    scope_name: str = "me",
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    return await _execution(dependencies).workspace.preparation_cache(
        user,
        {"kind": scope_kind, "name": scope_name},
        uid=artifact_id,
        request=request,
    )


@router.delete("/api/workspace-instances/{instance_id}")
async def delete_workspace_instance(
    instance_id: UUID,
    request: Request,
    expected_generation: int = Query(ge=0),
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    user = await dependencies.require_approved_user(request)
    return await _execution(dependencies).workspace.delete(
        instance_id, user, expected_generation=expected_generation, request=request
    )


@router.post("/api/manifests/export")
async def export_manifests(
    request: Request,
    body: ManifestExportInput,
    *,
    dependencies: ManifestDependencies = Depends(get_manifest_dependencies),
) -> dict:
    """Export authored ownership/references with explicit scopes, without deployment."""
    await dependencies.require_approved_user(request)
    source, origins = await _source(body)
    return await _call(
        dependencies.service.export,
        source,
        format=body.format,
        default_scope=body.default_scope.model_dump() if body.default_scope else None,
        output_format=body.output_format,
        origins=origins,
    )
