"""Phone HTTP adapters; knowledge-base/knowledge/features/phone_calls_as_subagents.md."""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, SecretStr

from orchestrator.security.access import (
    require_admin,
    require_internal,
    require_job_access,
)
from orchestrator.services.agent_pod_entrypoint import (
    InvalidConfigNameError,
    validate_config_name,
)
from orchestrator.services.telephony.front import TelephonyFront
from orchestrator.services.telephony.instructions import (
    greeting_instruction,
    session_instructions,
)
from orchestrator.services.telephony.store import (
    TelephonyStore,
    call_view,
    settings_view,
)
from orchestrator.services.telephony.stream import call_stream

router = APIRouter()


@dataclass(frozen=True)
class TelephonyDependencies:
    store: TelephonyStore
    front: TelephonyFront
    require_admin: Callable[..., Awaitable[Any]] = require_admin
    require_internal: Callable[..., Awaitable[Any]] = require_internal
    require_job_access: Callable[..., Awaitable[Any]] = require_job_access


async def get_dependencies(request: Request) -> TelephonyDependencies:
    return request.app.state.telephony_dependencies_factory()


class SettingsPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str | None = Field(default=None, min_length=1)
    project_id: UUID | None = None
    openai_project_id: str | None = Field(default=None, min_length=1)
    api_base: str | None = Field(default=None, pattern=r"^https?://")
    ws_base: str | None = Field(default=None, pattern=r"^wss?://")
    extra_headers: dict[str, str] | None = None
    webhook_secret: SecretStr | None = None
    api_key: SecretStr | None = None
    enabled: bool | None = None


class SettingsCreate(SettingsPatch):
    name: str = Field(min_length=1)
    openai_project_id: str = Field(min_length=1)
    webhook_secret: SecretStr


class LinePatch(BaseModel):
    model_config = ConfigDict(extra="forbid")
    settings_id: UUID | None = None
    project_id: UUID | None = None
    owner_user_id: UUID | None = None
    name: str | None = Field(default=None, min_length=1)
    number: str | None = Field(default=None, pattern=r"^\+[1-9][0-9]{1,14}$")
    is_default: bool | None = None
    enabled: bool | None = None
    engine: Literal["gpt_live"] | None = None
    model: str | None = Field(default=None, min_length=1)
    voice: str | None = Field(default=None, min_length=1)
    expert: str | None = Field(default=None, min_length=1)
    persona: str | None = None
    greeting: str | None = Field(default=None, min_length=1)
    fact_sheet: str | None = None
    transfer_target: str | None = None
    max_duration_s: int | None = Field(default=None, ge=1, le=7200)
    answer_timeout_s: int | None = Field(default=None, ge=1)
    no_job_fallback_s: int | None = Field(default=None, ge=1)
    goodbye: str | None = None


class LineCreate(LinePatch):
    settings_id: UUID
    project_id: UUID
    owner_user_id: UUID
    name: str = Field(min_length=1)
    greeting: str = Field(min_length=1)


EventKind = Literal[
    "transcript",
    "delegation",
    "answer",
    "instruction",
    "thinking",
    "fallback",
    "question",
    "follow_up",
    "transfer",
    "hangup",
    "dtmf",
    "usage",
    "closed",
    "error",
    "system",
]


class EventWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: str | None = None
    kind: EventKind
    speaker: str | None = None
    text: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None
    delegation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class EventsWrite(BaseModel):
    model_config = ConfigDict(extra="forbid")
    events: list[EventWrite] = Field(max_length=500)


class ActorAttach(BaseModel):
    model_config = ConfigDict(extra="forbid")
    job_id: UUID


def _values(body, nullable):
    data = body.model_dump(exclude_unset=True)
    if any(value is None and key not in nullable for key, value in data.items()):
        raise HTTPException(422, "Required phone configuration cannot be null")
    for key in ("webhook_secret", "api_key"):
        if isinstance(data.get(key), SecretStr):
            data[key] = data[key].get_secret_value()
            if not data[key]:
                raise HTTPException(422, "Phone secrets cannot be empty")
    return data


async def _save(store, table, data, row_id=None):
    try:
        row = await store.save(table, data, row_id)
    except asyncpg.IntegrityConstraintViolationError as exc:
        raise HTTPException(
            409, "Phone configuration conflicts with existing records"
        ) from exc
    if row is None:
        raise HTTPException(404, "Phone configuration not found")
    return settings_view(row) if table == "telephony_settings" else row


async def _delete(store, table, row_id):
    try:
        deleted = await store.delete(table, row_id)
    except (asyncpg.IntegrityConstraintViolationError, ValueError) as exc:
        raise HTTPException(
            409, "Phone configuration is still in use; disable it instead"
        ) from exc
    if not deleted:
        raise HTTPException(404, "Phone configuration not found")
    return {"deleted": True}


async def _line_values(store, body, line_id=None):
    data = _values(body, {"number", "transfer_target", "goodbye"})
    current = (
        await store.get_line(line_id)
        if line_id
        else {"id": "new", "expert": "phone-desk"}
    )
    if current is None:
        raise HTTPException(404, "Phone line not found")
    candidate = {**current, **data}
    try:
        greeting_instruction(candidate)
        session_instructions(candidate)
        validate_config_name(candidate["expert"])
    except (ValueError, InvalidConfigNameError) as exc:
        raise HTTPException(
            422, "Phone instructions or expert name are invalid"
        ) from exc
    return data


async def _call(store, call_id, job_id=None):
    call = await store.get_call(call_id, job_id=job_id)
    if call is None:
        raise HTTPException(404, "Call not found")
    return call


def _page(events, after_id):
    return {"events": events, "next_after_id": events[-1]["id"] if events else after_id}


# nosec: public OpenAI Standard Webhooks signature and timestamp verified inside the handler.
@router.post("/api/telephony/openai/webhook")
async def openai_webhook(
    request: Request, dependencies: TelephonyDependencies = Depends(get_dependencies)
):
    await dependencies.front.webhook(request.headers, await request.body())
    return {"received": True}


@router.get("/api/telephony/settings")
async def list_settings(
    request: Request, dependencies: TelephonyDependencies = Depends(get_dependencies)
):
    await dependencies.require_admin(request, dependencies.store.db)
    return {
        "settings": [settings_view(row) for row in await dependencies.store.settings()]
    }


@router.post("/api/telephony/settings")
async def create_settings(
    request: Request,
    body: SettingsCreate,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _save(
        dependencies.store,
        "telephony_settings",
        _values(body, {"project_id", "api_key"}),
    )


@router.patch("/api/telephony/settings/{settings_id}")
async def patch_settings(
    settings_id: UUID,
    request: Request,
    body: SettingsPatch,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _save(
        dependencies.store,
        "telephony_settings",
        _values(body, {"project_id", "api_key"}),
        settings_id,
    )


@router.delete("/api/telephony/settings/{settings_id}")
async def delete_settings(
    settings_id: UUID,
    request: Request,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _delete(dependencies.store, "telephony_settings", settings_id)


@router.get("/api/telephony/lines")
async def list_lines(
    request: Request,
    settings_id: UUID | None = None,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return {"lines": await dependencies.store.lines(settings_id)}


@router.post("/api/telephony/lines")
async def create_line(
    request: Request,
    body: LineCreate,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _save(
        dependencies.store,
        "telephony_lines",
        await _line_values(dependencies.store, body),
    )


@router.patch("/api/telephony/lines/{line_id}")
async def patch_line(
    line_id: UUID,
    request: Request,
    body: LinePatch,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _save(
        dependencies.store,
        "telephony_lines",
        await _line_values(dependencies.store, body, line_id),
        line_id,
    )


@router.delete("/api/telephony/lines/{line_id}")
async def delete_line(
    line_id: UUID,
    request: Request,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_admin(request, dependencies.store.db)
    return await _delete(dependencies.store, "telephony_lines", line_id)


@router.get("/api/jobs/{job_id}/calls")
async def job_calls(
    job_id: UUID,
    request: Request,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    return {"calls": await dependencies.store.job_calls(job_id)}


@router.get("/api/jobs/{job_id}/calls/{call_id}/events")
async def job_events(
    job_id: UUID,
    call_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=500),
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    await _call(dependencies.store, call_id, job_id)
    return _page(await dependencies.store.events(call_id, after_id, limit), after_id)


@router.get("/api/jobs/{job_id}/calls/{call_id}/stream")
async def job_stream(
    job_id: UUID,
    call_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_job_access(request, dependencies.store.db, str(job_id))
    await _call(dependencies.store, call_id, job_id)
    return StreamingResponse(
        call_stream(
            dependencies.store,
            call_id,
            job_id,
            after_id,
            request,
            dependencies.require_job_access,
        ),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/internal/telephony/calls/{call_id}/events")
async def internal_events(
    call_id: UUID,
    request: Request,
    after_id: int = Query(0, ge=0),
    limit: int = Query(500, ge=1, le=500),
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_internal(request)
    call = await _call(dependencies.store, call_id)
    return {
        **_page(await dependencies.store.events(call_id, after_id, limit), after_id),
        "call": call_view(call),
    }


@router.post("/api/internal/telephony/calls/{call_id}/events")
async def internal_write(
    call_id: UUID,
    request: Request,
    body: EventsWrite,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_internal(request)
    try:
        count = await dependencies.store.write_events(
            call_id, [event.model_dump() for event in body.events]
        )
    except LookupError as exc:
        raise HTTPException(404, "Call not found") from exc
    return {"inserted": count}


@router.post("/api/internal/telephony/calls/{call_id}/attach")
async def internal_attach(
    call_id: UUID,
    request: Request,
    body: ActorAttach,
    dependencies: TelephonyDependencies = Depends(get_dependencies),
):
    await dependencies.require_internal(request)
    call = await dependencies.store.attach_actor(call_id, body.job_id)
    if call is None:
        raise HTTPException(404, "Call is not bound to this job")
    return call_view(call)
