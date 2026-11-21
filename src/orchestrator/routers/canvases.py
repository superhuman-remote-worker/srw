"""Dynamic Canvas state, file content, and delegated-agent adapters."""

from __future__ import annotations

import asyncio
from datetime import datetime
from email.utils import format_datetime
from email.utils import parsedate_to_datetime
from pathlib import PurePosixPath
import json
import logging
import re
import secrets
from tempfile import SpooledTemporaryFile
import time
from typing import Any, Literal
from urllib.parse import quote, urlencode
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from orchestrator.security.access import require_internal, require_thread_owner
from orchestrator.services.canvas import (
    BrowserSource,
    CanvasCapabilities,
    CanvasEditError,
    CanvasPreconditionFailed,
    CanvasPreconditionRequired,
    CanvasPublicState,
    CanvasRecord,
    CanvasRenderer,
    CanvasService,
    CanvasSetInput,
    WorkspaceAppSource,
    WorkspaceFileSource,
    build_public_canvas_representation,
    strong_state_precondition,
)
from orchestrator.services.canvas_apps import (
    CanvasAppError,
    ThreadWorkspaceAppGateway,
    canvas_live_preview_enabled,
)
from orchestrator.services.canvas_awareness import (
    CANVAS_AWARENESS_CLEANUP_LIMIT,
    CANVAS_AWARENESS_MAX_SEQUENCE,
    CANVAS_AWARENESS_TTL_SECONDS,
    CanvasAwarenessConflict,
    cleanup_canvas_awareness,
    fetch_canvas_awareness_snapshot,
    mutate_canvas_awareness,
)
from orchestrator.services.canvas_files import (
    MAX_TEXT_BYTES,
    CanvasFileError,
    CanvasResponseLease,
    ThreadWorkspaceFileGateway,
    ValidatedCanvasFile,
    acquire_canvas_response_lease,
    current_workspace_generation,
    vm_canvas_write_authority,
)
from orchestrator.services.canvas_office import (
    CanvasOfficeError,
    WopiTokenGrant,
    collabora_config,
    create_wopi_token_service,
    get_collabora_discovery_service,
)
from orchestrator.services.canvas_snapshots import CanvasSnapshot, CanvasSnapshotStore
from orchestrator.services.canvas_viewer_config import (
    CanvasViewerConfigurationError,
    canvas_viewer_config,
)
from orchestrator.services.canvas_viewer_sessions import (
    CanvasViewerError,
    CanvasViewerSessionService,
    canvas_viewer_error_detail,
)
from orchestrator.services.shared_browser_canvas import (
    BrowserCanvasError,
    browser_capability,
    commit_browser_canvas,
    prepare_browser_canvas,
    require_browser_capability,
)

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/persistent/threads/{thread_id}/canvases",
    tags=["Dynamic Canvas"],
)
internal_router = APIRouter(
    prefix="/api/internal/persistent/threads/{thread_id}/canvases",
    tags=["Dynamic Canvas (internal)"],
)

_STATE_CACHE_CONTROL = "private, no-cache"
_CONTENT_CACHE_CONTROL = "private, no-cache"
_VIEWER_SECRET_PATTERN = r"^[A-Za-z0-9_-]{32,128}$"
_AWARENESS_SESSION_PATTERN = r"^[A-Za-z0-9_-]{8,128}$"
_CANVAS_AWARENESS_STREAM_POLL_S = 1.0
_CANVAS_AWARENESS_STREAM_REAUTH_S = 10.0
_CANVAS_AWARENESS_STREAM_KEEPALIVE_S = 10.0
_CANVAS_AWARENESS_STREAM_CLEANUP_S = 60.0

# Re-pin attempts already made, keyed by (thread, current generation, revision,
# source version). Insertion-ordered so the oldest entry evicts first.
_REPIN_ATTEMPTED: dict[tuple[str, str, int, str], bool] = {}
_REPIN_ATTEMPT_CAP = 512

# Workspace-side failures that mean "we cannot reach these bytes right now",
# as opposed to "these bytes changed". Only these fall back to a snapshot; a
# `source_changed` must keep telling the truth about the workspace.
_SNAPSHOT_FALLBACK_CODES = frozenset(
    {
        "workspace_unavailable",
        "workspace_generation_changed",
        "canvas_file_not_found",
    }
)


class _LeasedContentResponse(Response):
    """Keep a Canvas buffer lease until the body is sent or cancelled."""

    def __init__(self, *args: Any, lease: CanvasResponseLease, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._canvas_lease = lease

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._canvas_lease.release()


class CanvasSetRequest(BaseModel):
    """Closed flat tool transport; no caller-owned context or address fields."""

    model_config = ConfigDict(extra="forbid")

    source_type: Literal["workspace_file", "workspace_port", "browser"]
    path: str | None = Field(default=None, min_length=1, max_length=4096)
    port: int | None = Field(default=None, ge=1, le=65535)
    entry_path: str | None = Field(default=None, min_length=1, max_length=2048)
    browser_id: Literal["current"] | None = None
    title: str | None = Field(default=None, max_length=200)
    renderer: CanvasRenderer = "auto"
    editable: bool = False
    alt_text: str | None = Field(default=None, max_length=1000)
    new_app: bool = False

    @field_validator("title")
    @classmethod
    def _nonblank_title(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("title must not be blank")
        return value

    @model_validator(mode="after")
    def _source_fields_do_not_mix(self) -> "CanvasSetRequest":
        if self.source_type == "workspace_file":
            if self.path is None:
                raise ValueError("workspace_file requires path")
            if (
                self.port is not None
                or self.entry_path is not None
                or self.browser_id is not None
                or self.new_app
            ):
                raise ValueError("workspace_file does not accept live-app fields")
            return self

        if self.source_type == "browser":
            if self.browser_id != "current":
                raise ValueError("browser requires browser_id='current'")
            if (
                self.path is not None
                or self.port is not None
                or self.entry_path is not None
                or self.alt_text is not None
                or self.new_app
            ):
                raise ValueError("browser does not accept file or live-app fields")
            if self.renderer != "auto" or self.editable:
                raise ValueError("browser requires renderer='auto' and editable=false")
            return self

        if self.port is None:
            raise ValueError("workspace_port requires port")
        if (
            self.path is not None
            or self.alt_text is not None
            or self.browser_id is not None
        ):
            raise ValueError("workspace_port does not accept file fields")
        if self.renderer != "auto" or self.editable:
            raise ValueError(
                "workspace_port requires renderer='auto' and editable=false"
            )
        return self


class CanvasBootstrapAuthorizeRequest(BaseModel):
    """Closed parent-to-BFF proof for one gateway-created frame challenge."""

    model_config = ConfigDict(extra="forbid")

    challenge: str = Field(
        min_length=32, max_length=128, pattern=_VIEWER_SECRET_PATTERN
    )
    ready_receipt: str = Field(
        min_length=32, max_length=128, pattern=_VIEWER_SECRET_PATTERN
    )
    bridge_nonce: str = Field(
        min_length=32, max_length=128, pattern=_VIEWER_SECRET_PATTERN
    )


class CanvasAwarenessRequest(BaseModel):
    """One monotonic Canvas editor heartbeat or idle tombstone."""

    model_config = ConfigDict(extra="forbid")

    sequence: int = Field(ge=1, le=CANVAS_AWARENESS_MAX_SEQUENCE)
    state: Literal["editing", "idle"]
    path: str = Field(min_length=1, max_length=4096)
    presentation_revision: int = Field(ge=1)
    source_version: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")


class CanvasAwarenessResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    applied: bool
    sender_id: str
    sequence: int
    state: Literal["editing", "idle"]
    expires_at: datetime


def _get_db(request: Request) -> Any:
    """Resolve the store of the application handling *this* request.

    Composition publishes it as ``app.state.store``; reading it here rather
    than importing ``orchestrator.main`` keeps this module free of the
    application module while still resolving per call, never at import, so
    two applications in one process each keep their own store (R1.B04
    caller-boundary closure, same shape as ``uploads.py`` in B03).
    """

    return request.app.state.store


def _get_canvas_service(db: Any) -> CanvasService:
    return CanvasService(db, snapshot_store=CanvasSnapshotStore(db))


def _get_file_gateway(db: Any | None = None) -> ThreadWorkspaceFileGateway:
    return ThreadWorkspaceFileGateway(
        thread_loader=getattr(db, "get_thread", None) if db is not None else None
    )


def _get_snapshot_store(db: Any) -> CanvasSnapshotStore:
    return CanvasSnapshotStore(db)


def _get_app_gateway(db: Any | None = None) -> ThreadWorkspaceAppGateway:
    return ThreadWorkspaceAppGateway(
        thread_loader=getattr(db, "get_thread", None) if db is not None else None
    )


def _get_viewer_service(db: Any) -> CanvasViewerSessionService:
    return CanvasViewerSessionService(db)


def _get_collabora_config():
    return collabora_config()


def _get_office_discovery():
    return get_collabora_discovery_service()


def _get_wopi_token_service(db: Any):
    return create_wopi_token_service(db)


def _state_response(
    payload: bytes,
    etag: str,
    *,
    status_code: int = 200,
    extra_headers: dict[str, str] | None = None,
) -> Response:
    headers = {"ETag": etag, "Cache-Control": _STATE_CACHE_CONTROL}
    headers.update(extra_headers or {})
    return Response(
        content=payload,
        status_code=status_code,
        media_type="application/json",
        headers=headers,
    )


def _no_content_response() -> Response:
    return Response(status_code=204, headers={"Cache-Control": _STATE_CACHE_CONTROL})


def _if_none_match_matches(header_value: str | None, current_etag: str) -> bool:
    if not header_value:
        return False
    for candidate in header_value.split(","):
        candidate = candidate.strip()
        if candidate == "*":
            return True
        if candidate.startswith("W/"):
            candidate = candidate[2:].lstrip()
        if candidate == current_etag:
            return True
    return False


def _raise_file_error(error: CanvasFileError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


def _raise_edit_error(error: CanvasEditError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


def _raise_app_error(error: CanvasAppError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


def _raise_browser_error(error: BrowserCanvasError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


def _raise_viewer_error(error: CanvasViewerError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail=canvas_viewer_error_detail(error),
    ) from error


def _raise_office_error(error: CanvasOfficeError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


def _raise_awareness_conflict(error: CanvasAwarenessConflict) -> None:
    status_code = 503 if error.code == "canvas_awareness_capacity_exhausted" else 409
    raise HTTPException(
        status_code=status_code,
        detail={"code": error.code, "message": str(error)},
    ) from error


def _required_parent_session_id(request: Request) -> UUID:
    """Require exactly one canonical BFF cookie, never Bearer/MCP fallback."""

    if request.headers.get("Authorization") or any(
        request.headers.get(name)
        for name in ("X-Internal-Key", "X-MCP-User-Id", "X-MCP-Token")
    ):
        raise HTTPException(
            status_code=401,
            detail={
                "code": "canvas_parent_session_required",
                "message": "A current Cockpit session is required",
            },
        )
    values: list[str] = []
    for header in request.headers.getlist("cookie"):
        for item in header.split(";"):
            name, separator, value = item.strip().partition("=")
            if separator and name == "srw_session":
                values.append(value.strip())
    if len(values) != 1:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "canvas_parent_session_required",
                "message": "A current Cockpit session is required",
            },
        )
    try:
        session_id = UUID(values[0])
    except (TypeError, ValueError) as exc:
        raise HTTPException(
            status_code=401,
            detail={
                "code": "canvas_parent_session_required",
                "message": "A current Cockpit session is required",
            },
        ) from exc
    if str(session_id) != values[0].lower():
        raise HTTPException(
            status_code=401,
            detail={
                "code": "canvas_parent_session_required",
                "message": "A current Cockpit session is required",
            },
        )
    return session_id


def _require_canvas_parent_fetch_metadata(request: Request) -> None:
    """Reject browsers that cannot enforce the live-app fetch boundary."""

    site = request.headers.getlist("sec-fetch-site")
    mode = request.headers.getlist("sec-fetch-mode")
    destination = request.headers.getlist("sec-fetch-dest")
    if (
        len(site) != 1
        or site[0].strip().lower() not in {"same-origin", "same-site"}
        or len(mode) != 1
        or mode[0].strip().lower() != "cors"
        or len(destination) != 1
        or destination[0].strip().lower() != "empty"
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "canvas_browser_unsupported",
                "message": (
                    "This browser cannot enforce the Canvas live-app isolation contract"
                ),
            },
        )


def _required_edit_preconditions(request: Request) -> tuple[str, int]:
    """Parse both conditional-save headers without touching the body."""

    content_etag = request.headers.get("If-Match")
    revision = request.headers.get("X-Canvas-Presentation-Revision")
    if content_etag is None or revision is None:
        raise CanvasEditError(
            428,
            "canvas_precondition_required",
            "If-Match and X-Canvas-Presentation-Revision are required",
        )
    value = content_etag.strip()
    if not value or not revision.strip():
        raise CanvasEditError(
            400,
            "invalid_canvas_precondition",
            "Canvas edit precondition headers are malformed",
        )
    if not re.fullmatch(r'"sha256:[0-9a-f]{64}"', value):
        raise CanvasEditError(
            400,
            "invalid_canvas_precondition",
            "If-Match must contain one quoted strong Canvas content hash",
        )
    if not re.fullmatch(r"[1-9][0-9]*", revision.strip()):
        raise CanvasEditError(
            400,
            "invalid_canvas_precondition",
            "X-Canvas-Presentation-Revision must be a positive integer",
        )
    return value[1:-1], int(revision)


async def _read_bounded_edit_body(request: Request) -> bytes:
    """Spool one bounded request before any coordinator or Canvas row lock."""

    content_encoding = request.headers.get("Content-Encoding")
    if content_encoding is not None and content_encoding.strip().lower() != "identity":
        raise CanvasFileError(
            415,
            "unsupported_canvas_content_encoding",
            "Canvas edits require identity content encoding",
        )

    raw_length = request.headers.get("Content-Length")
    declared: int | None = None
    if raw_length is not None:
        try:
            declared = int(raw_length)
        except ValueError as exc:
            raise CanvasFileError(
                400, "invalid_canvas_content", "Content-Length is invalid"
            ) from exc
        if declared < 0:
            raise CanvasFileError(
                400, "invalid_canvas_content", "Content-Length is invalid"
            )
        if declared > MAX_TEXT_BYTES:
            raise CanvasFileError(
                413, "canvas_file_too_large", "Canvas text content is too large"
            )

    spool = SpooledTemporaryFile(max_size=min(MAX_TEXT_BYTES, 256 * 1024), mode="w+b")
    total = 0
    try:
        async for chunk in request.stream():
            total += len(chunk)
            if total > MAX_TEXT_BYTES:
                raise CanvasFileError(
                    413, "canvas_file_too_large", "Canvas text content is too large"
                )
            spool.write(chunk)
        if declared is not None and declared != total:
            raise CanvasFileError(
                400,
                "invalid_canvas_content",
                "Content-Length does not match the received Canvas content",
            )
        spool.seek(0)
        return spool.read()
    finally:
        spool.close()


async def _require_empty_refresh_body(request: Request) -> None:
    """Reject source/path payloads; refresh derives everything from state."""

    raw_length = request.headers.get("Content-Length")
    if raw_length is not None:
        try:
            declared = int(raw_length)
        except ValueError as exc:
            raise CanvasFileError(
                400, "invalid_canvas_content", "Content-Length is invalid"
            ) from exc
        if declared != 0:
            raise CanvasFileError(
                400,
                "invalid_canvas_refresh",
                "Canvas refresh does not accept a request body",
            )
    async for chunk in request.stream():
        if chunk:
            raise CanvasFileError(
                400,
                "invalid_canvas_refresh",
                "Canvas refresh does not accept a request body",
            )


def _office_file_error(error: CanvasOfficeError) -> CanvasFileError:
    """Translate deployment availability into the closed tool-facing vocabulary."""

    return CanvasFileError(
        422 if error.code == "canvas_office_unavailable" else error.status_code,
        error.code,
        error.message,
    )


async def _require_office_view(path: str) -> str:
    try:
        _get_collabora_config().require_enabled()
        return await _get_office_discovery().get_urlsrc(PurePosixPath(path).suffix)
    except CanvasOfficeError as exc:
        raise _office_file_error(exc) from exc


def _build_office_session_payload(
    *,
    urlsrc: str,
    wopi_base_url: str,
    grant: WopiTokenGrant,
) -> dict[str, str | int]:
    return {
        "urlsrc": urlsrc,
        "WOPISrc": f"{wopi_base_url.rstrip('/')}/wopi/files/{grant.file_id}",
        "access_token": grant.access_token,
        "access_token_ttl": grant.expires_at_ms,
    }


def _content_url(thread_id: str, record: CanvasRecord) -> str:
    assert record.source_fingerprint is not None
    assert record.source_version is not None
    query = urlencode(
        {
            "presentation_revision": record.presentation_revision,
            "source_fingerprint": record.source_fingerprint,
            "source_version": record.source_version,
            "ngsw-bypass": "true",
        }
    )
    return (
        f"/api/persistent/threads/{quote(thread_id, safe='')}/canvases/main/content"
        f"?{query}"
    )


async def _represent(
    thread_id: str,
    thread: dict[str, Any],
    record: CanvasRecord,
    *,
    browser_content_url: bool,
    db: Any | None = None,
):
    status = "cleared" if record.source is None else "unavailable"
    capabilities = CanvasCapabilities()
    content_url = None
    content_origin: Literal["workspace", "snapshot"] | None = None
    captured_at = None
    if isinstance(record.source, WorkspaceFileSource):
        try:
            gateway = _get_file_gateway(db)
            materialized = await gateway.materialize_current(thread, record)
            if materialized.source_version == record.source_version:
                status = "ready"
                content_origin = "workspace"
                if record.renderer == "office":
                    can_view_office = False
                    try:
                        await _require_office_view(record.source.path)
                        can_view_office = True
                    except CanvasFileError:
                        pass
                    capabilities = CanvasCapabilities(
                        can_edit=(
                            can_view_office
                            and record.editable
                            and gateway.supports_editing(thread, record)
                        ),
                        can_pop_out=True,
                        can_view_office=can_view_office,
                    )
                else:
                    capabilities = CanvasCapabilities(
                        can_edit=(
                            record.editable and gateway.supports_editing(thread, record)
                        ),
                        can_pop_out=True,
                    )
                if browser_content_url and record.renderer != "office":
                    content_url = _content_url(thread_id, record)
            else:
                status = "source_changed"
        except CanvasFileError as exc:
            if exc.code == "source_changed":
                status = "source_changed"
            elif exc.code in _SNAPSHOT_FALLBACK_CODES:
                status = "unavailable"
            else:
                status = "error"

        # The workspace could not answer for these exact published bytes. If we
        # remembered them at publish time, the stage stays up read-only instead
        # of going dark — a suspended session is the common case, not a fault.
        if status == "unavailable" and db is not None:
            snapshot = await _get_snapshot_store(db).usable(
                thread_id, record.source_version
            )
            if snapshot is not None:
                status = "ready"
                content_origin = "snapshot"
                captured_at = snapshot.captured_at
                capabilities = CanvasCapabilities(can_pop_out=True)
                if browser_content_url and record.renderer != "office":
                    content_url = _content_url(thread_id, record)
    elif isinstance(record.source, WorkspaceAppSource):
        # Slice 3A exposes only callable state/status. Viewer sessions and
        # iframe URLs arrive with the isolated-origin proxy; never manufacture
        # a same-origin fallback while that boundary is absent or disabled.
        if canvas_live_preview_enabled():
            status = await _get_app_gateway(db).status_for_record(thread, record)
            if status == "ready":
                try:
                    viewer = canvas_viewer_config()
                except CanvasViewerConfigurationError:
                    viewer = None
                if viewer is not None and viewer.enabled:
                    capabilities = CanvasCapabilities(
                        can_pop_out=True,
                        can_create_viewer_session=True,
                    )
    elif isinstance(record.source, BrowserSource):
        # The stream endpoint still revalidates the concrete browser generation;
        # this presentation gate uses the same attested workspace authority as open.
        capability = browser_capability(thread)
        if capability.can_open_browser and capability.workspace_ready:
            status = "ready"
            capabilities = CanvasCapabilities(
                can_pop_out=True,
                can_take_control=True,
                can_stream_browser=True,
            )
    return build_public_canvas_representation(
        record,
        status=status,
        capabilities=capabilities,
        content_url=content_url,
        content_origin=content_origin,
        content_captured_at=captured_at,
    )


async def _require_delegated_owner(
    request: Request, db: Any, thread_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    await require_internal(request)
    delegated_id = request.headers.get("X-MCP-User-Id", "").strip()
    if not delegated_id:
        raise HTTPException(status_code=401, detail="Delegated user is required")
    user, thread = await require_thread_owner(request, db, thread_id)
    if str(user.get("id") or "") != delegated_id:
        raise HTTPException(status_code=401, detail="Delegated user does not match")
    return user, thread


@router.get(
    "/main",
    response_model=CanvasPublicState,
    responses={
        204: {"description": "No Canvas exists"},
        304: {"description": "Not modified"},
    },
)
async def get_main_canvas(thread_id: str, request: Request) -> Response:
    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    record = await _get_canvas_service(db).get(thread_id)
    if record is None:
        return _no_content_response()
    # A returning session usually finds its workspace rebuilt under a new
    # generation. If the file it published is still byte-identical, re-bind it
    # here so the Canvas comes back live and editable rather than frozen at the
    # stored copy. Deliberately only on the state read: `content_url` embeds
    # the source fingerprint, so re-pinning during a content fetch would
    # invalidate the very URL being served.
    record = await _maybe_repin(db, thread_id, thread, record)
    representation = await _represent(
        thread_id, thread, record, browser_content_url=True, db=db
    )
    await require_thread_owner(request, db, thread_id)
    latest = await _get_canvas_service(db).get(thread_id)
    if (
        latest is not None
        and latest.presentation_revision != record.presentation_revision
    ):
        record = latest
        representation = await _represent(
            thread_id, thread, record, browser_content_url=True, db=db
        )
        await require_thread_owner(request, db, thread_id)
    if _if_none_match_matches(
        request.headers.get("If-None-Match"), representation.etag
    ):
        return Response(
            status_code=304,
            headers={
                "ETag": representation.etag,
                "Cache-Control": _STATE_CACHE_CONTROL,
            },
        )
    return _state_response(representation.payload, representation.etag)


@router.put(
    "/main/awareness/{editing_session_id}",
    response_model=CanvasAwarenessResponse,
    responses={401: {}, 403: {}, 409: {}, 422: {}, 503: {}},
)
async def put_main_canvas_awareness(
    thread_id: str,
    editing_session_id: str,
    payload: CanvasAwarenessRequest,
    request: Request,
) -> CanvasAwarenessResponse:
    """Apply one owner-authenticated editor heartbeat or idle tombstone.

    The browser never sends or learns an execution lane. Both pinned and
    stateless sessions use this same database-backed courtesy signal.
    """

    if not re.fullmatch(_AWARENESS_SESSION_PATTERN, editing_session_id):
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_canvas_awareness_session",
                "message": "Canvas editing session id is invalid",
            },
        )
    db = _get_db(request)
    await require_thread_owner(request, db, thread_id)
    try:
        mutation = await mutate_canvas_awareness(
            db,
            thread_id=thread_id,
            editing_session_id=editing_session_id,
            sequence=payload.sequence,
            state=payload.state,
            path=payload.path,
            presentation_revision=payload.presentation_revision,
            source_version=payload.source_version,
            ttl_seconds=CANVAS_AWARENESS_TTL_SECONDS,
        )
    except CanvasAwarenessConflict as exc:
        _raise_awareness_conflict(exc)
    return CanvasAwarenessResponse(
        applied=mutation.applied,
        sender_id=mutation.sender_id,
        sequence=mutation.sequence,
        state=mutation.state,
        expires_at=mutation.expires_at,
    )


@router.get(
    "/main/awareness/stream",
    response_class=StreamingResponse,
    responses={401: {}, 403: {}, 404: {}},
)
async def stream_main_canvas_awareness(
    thread_id: str, request: Request
) -> StreamingResponse:
    """Stream complete, unjournaled Canvas editor snapshots for both lanes.

    Frames are named ``canvas_awareness`` events and deliberately carry no
    SSE ``id`` line: reconnect reads a fresh complete DB snapshot, so no event
    cursor or session-journal sequence allocation is involved.
    """

    db = _get_db(request)
    await require_thread_owner(request, db, thread_id)

    async def event_stream():
        yield ": open\n\n"
        previous_signature: tuple[tuple[str, str, str, int, str, int], ...] | None = (
            None
        )
        next_reauth = time.monotonic() + _CANVAS_AWARENESS_STREAM_REAUTH_S
        next_keepalive = time.monotonic() + _CANVAS_AWARENESS_STREAM_KEEPALIVE_S
        next_cleanup = time.monotonic()
        try:
            while True:
                if await request.is_disconnected():
                    return
                now = time.monotonic()
                if now >= next_reauth:
                    # A long-lived SSE response does not retain authorization
                    # forever. Failure closes it; EventSource reconnects with
                    # the browser's current BFF cookie.
                    await require_thread_owner(request, db, thread_id)
                    next_reauth = now + _CANVAS_AWARENESS_STREAM_REAUTH_S
                if now >= next_cleanup:
                    try:
                        await cleanup_canvas_awareness(
                            db, limit=CANVAS_AWARENESS_CLEANUP_LIMIT
                        )
                    except Exception as exc:
                        # Cleanup is only a storage bound. It must not hide the
                        # current courtesy snapshot from an attached editor.
                        logger.warning("Canvas awareness cleanup failed: %s", exc)
                    next_cleanup = now + _CANVAS_AWARENESS_STREAM_CLEANUP_S

                editors = await fetch_canvas_awareness_snapshot(db, thread_id=thread_id)
                signature = tuple(editor.signature() for editor in editors)
                if previous_signature is None or signature != previous_signature:
                    body = json.dumps(
                        {
                            "canvas_id": "main",
                            "editors": [editor.public_dict() for editor in editors],
                        },
                        separators=(",", ":"),
                    )
                    yield f"event: canvas_awareness\ndata: {body}\n\n"
                    previous_signature = signature
                    next_keepalive = now + _CANVAS_AWARENESS_STREAM_KEEPALIVE_S
                elif now >= next_keepalive:
                    # A comment is transport keepalive only. It is separate
                    # from the DB snapshot contract and has no resumable id.
                    yield ": keepalive\n\n"
                    next_keepalive = now + _CANVAS_AWARENESS_STREAM_KEEPALIVE_S
                await asyncio.sleep(_CANVAS_AWARENESS_STREAM_POLL_S)
        except asyncio.CancelledError:
            return
        except Exception as exc:
            logger.warning(
                "Canvas awareness stream closed (thread=%s): %s",
                thread_id,
                exc,
            )
            return

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache, no-store",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@router.delete(
    "/main",
    response_model=CanvasPublicState,
    responses={204: {"description": "Absent/already cleared"}, 412: {}, 428: {}},
)
async def clear_main_canvas(thread_id: str, request: Request) -> Response:
    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    service = _get_canvas_service(db)
    current = await service.get(thread_id)
    if current is None or current.source is None:
        return _no_content_response()
    visible = await _represent(
        thread_id, thread, current, browser_content_url=True, db=db
    )

    def visible_builder(record: CanvasRecord):
        content_url = (
            _content_url(thread_id, record)
            if visible.state.content_url is not None
            and isinstance(record.source, WorkspaceFileSource)
            else None
        )
        return build_public_canvas_representation(
            record,
            status=visible.state.status,
            capabilities=visible.state.capabilities,
            content_url=content_url,
            content_origin=visible.state.content_origin,
            content_captured_at=visible.state.content_captured_at,
        )

    await require_thread_owner(request, db, thread_id)
    try:
        mutation = await service.clear(
            thread_id,
            expected_etag=request.headers.get("If-Match"),
            representation_builder=visible_builder,
        )
    except CanvasPreconditionRequired as exc:
        raise HTTPException(
            status_code=428,
            detail={"code": "canvas_precondition_required", "message": str(exc)},
        ) from exc
    except CanvasPreconditionFailed as exc:
        raise HTTPException(
            status_code=412,
            detail={"code": "canvas_precondition_failed", "message": str(exc)},
        ) from exc
    if not mutation.changed:
        return _no_content_response()
    assert mutation.record is not None
    representation = build_public_canvas_representation(mutation.record)
    return _state_response(representation.payload, representation.etag)


@router.post(
    "/main/view-attachments",
    responses={401: {}, 403: {}, 409: {}, 412: {}, 428: {}, 503: {}},
)
async def create_main_canvas_view_attachment(
    thread_id: str, request: Request
) -> Response:
    """Create one iframe-only bootstrap from exact authorized Canvas state."""

    parent_session_id = _required_parent_session_id(request)
    _require_canvas_parent_fetch_metadata(request)
    try:
        await _require_empty_refresh_body(request)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    db = _get_db(request)
    user, thread = await require_thread_owner(request, db, thread_id)
    record = await _get_canvas_service(db).get(thread_id)
    if record is None or not isinstance(record.source, WorkspaceAppSource):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "canvas_not_live_app",
                "message": "The current Canvas is not a live workspace application",
            },
        )
    visible = await _represent(
        thread_id, thread, record, browser_content_url=True, db=db
    )
    expected = request.headers.get("If-Match")
    if expected is None:
        raise HTTPException(
            status_code=428,
            detail={
                "code": "canvas_precondition_required",
                "message": "If-Match is required to attach a Canvas viewer",
            },
        )
    if not secrets.compare_digest(strong_state_precondition(expected), visible.etag):
        raise HTTPException(
            status_code=412,
            detail={
                "code": "canvas_precondition_failed",
                "message": "Canvas state ETag is stale",
            },
        )
    if not visible.state.capabilities.can_create_viewer_session:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "canvas_viewer_unavailable",
                "message": "Canvas live viewer is unavailable",
            },
        )
    await require_thread_owner(request, db, thread_id)
    try:
        grant = await _get_viewer_service(db).create_attachment(
            user_id=str(user["id"]),
            thread_id=thread_id,
            parent_session_id=parent_session_id,
            embedding_origin=request.headers.get("Origin"),
            expected_record=record,
        )
    except CanvasViewerError as exc:
        _raise_viewer_error(exc)
    return JSONResponse(
        grant.public_dict(), headers={"Cache-Control": "private, no-store"}
    )


@router.post(
    "/main/view-attachments/{attachment_id}/authorize",
    responses={401: {}, 403: {}, 409: {}, 422: {}, 503: {}},
)
async def authorize_main_canvas_view_attachment(
    thread_id: str,
    attachment_id: UUID,
    payload: CanvasBootstrapAuthorizeRequest,
    request: Request,
) -> Response:
    """Bind one gateway challenge to this exact authenticated BFF session."""

    parent_session_id = _required_parent_session_id(request)
    db = _get_db(request)
    user, _ = await require_thread_owner(request, db, thread_id)
    try:
        authorization = await _get_viewer_service(db).authorize_bootstrap(
            attachment_id=attachment_id,
            user_id=str(user["id"]),
            thread_id=thread_id,
            parent_session_id=parent_session_id,
            embedding_origin=request.headers.get("Origin"),
            challenge=payload.challenge,
            ready_receipt=payload.ready_receipt,
            bridge_nonce=payload.bridge_nonce,
        )
    except CanvasViewerError as exc:
        _raise_viewer_error(exc)
    return JSONResponse(
        authorization.public_dict(),
        headers={"Cache-Control": "private, no-store"},
    )


@router.post(
    "/main/view-attachments/{attachment_id}/renew",
    responses={401: {}, 403: {}, 409: {}, 503: {}},
)
async def renew_main_canvas_view_attachment(
    thread_id: str, attachment_id: UUID, request: Request
) -> Response:
    parent_session_id = _required_parent_session_id(request)
    try:
        await _require_empty_refresh_body(request)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    db = _get_db(request)
    user, _ = await require_thread_owner(request, db, thread_id)
    try:
        renewal = await _get_viewer_service(db).renew_attachment(
            attachment_id=attachment_id,
            user_id=str(user["id"]),
            thread_id=thread_id,
            parent_session_id=parent_session_id,
        )
    except CanvasViewerError as exc:
        _raise_viewer_error(exc)
    return JSONResponse(
        renewal.public_dict(), headers={"Cache-Control": "private, no-store"}
    )


@router.delete(
    "/main/view-attachments/{attachment_id}",
    status_code=204,
    responses={401: {}, 403: {}, 503: {}},
)
async def close_main_canvas_view_attachment(
    thread_id: str, attachment_id: UUID, request: Request
) -> Response:
    parent_session_id = _required_parent_session_id(request)
    try:
        await _require_empty_refresh_body(request)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    db = _get_db(request)
    user, _ = await require_thread_owner(request, db, thread_id)
    try:
        await _get_viewer_service(db).close_attachment(
            attachment_id=attachment_id,
            user_id=str(user["id"]),
            thread_id=thread_id,
            parent_session_id=parent_session_id,
        )
    except CanvasViewerError as exc:
        _raise_viewer_error(exc)
    return Response(status_code=204, headers={"Cache-Control": "private, no-store"})


@router.post(
    "/main/office-session",
    responses={401: {}, 403: {}, 409: {}, 412: {}, 428: {}, 503: {}},
)
async def create_main_canvas_office_session(
    thread_id: str,
    request: Request,
) -> Response:
    """Mint one form-post WOPI session from exact current Office Canvas state."""

    _required_parent_session_id(request)
    try:
        await _require_empty_refresh_body(request)
        config = _get_collabora_config().require_enabled()
        config.require_cockpit_origin(request.headers.get("Origin"))
    except CanvasFileError as exc:
        _raise_file_error(exc)
    except CanvasOfficeError as exc:
        _raise_office_error(exc)

    db = _get_db(request)
    user, thread = await require_thread_owner(request, db, thread_id)
    service = _get_canvas_service(db)
    record = await service.get(thread_id)
    if (
        record is None
        or record.renderer != "office"
        or not isinstance(record.source, WorkspaceFileSource)
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "canvas_not_office",
                "message": "The current Canvas is not an Office document",
            },
        )

    visible = await _represent(
        thread_id,
        thread,
        record,
        browser_content_url=False,
        db=db,
    )
    expected = request.headers.get("If-Match")
    if expected is None:
        raise HTTPException(
            status_code=428,
            detail={
                "code": "canvas_precondition_required",
                "message": "If-Match is required to open an Office Canvas session",
            },
        )
    if not secrets.compare_digest(strong_state_precondition(expected), visible.etag):
        raise HTTPException(
            status_code=412,
            detail={
                "code": "canvas_precondition_failed",
                "message": "Canvas state ETag is stale",
            },
        )
    if (
        visible.state.status != "ready"
        or not visible.state.capabilities.can_view_office
        or (record.editable and not visible.state.capabilities.can_edit)
    ):
        raise HTTPException(
            status_code=503,
            detail={
                "code": "canvas_office_unavailable",
                "message": "Office document viewing or editing is currently unavailable",
            },
        )

    try:
        urlsrc = await _get_office_discovery().get_urlsrc(
            PurePosixPath(record.source.path).suffix
        )
    except CanvasOfficeError as exc:
        _raise_office_error(exc)

    # Discovery and workspace reads can race a clear/replacement or membership
    # change. Re-admit and bind the token to the exact current path immediately
    # before minting it.
    user, fresh_thread = await require_thread_owner(request, db, thread_id)
    current = await service.get(thread_id)
    if (
        current is None
        or current.presentation_revision != record.presentation_revision
        or current.source_fingerprint != record.source_fingerprint
        or current.source_version != record.source_version
        or current.renderer != "office"
        or current.editable is not record.editable
        or not isinstance(current.source, WorkspaceFileSource)
        or current.source.path != record.source.path
        or (
            current.editable
            and not _get_file_gateway(db).supports_editing(fresh_thread, current)
        )
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "canvas_presentation_changed",
                "message": "Canvas presentation changed while opening Office",
            },
        )
    try:
        grant = _get_wopi_token_service(db).mint(
            user_id=str(user["id"]),
            thread_id=thread_id,
            path=current.source.path,
            write_flag=current.editable,
        )
    except CanvasOfficeError as exc:
        _raise_office_error(exc)
    return JSONResponse(
        _build_office_session_payload(
            urlsrc=urlsrc,
            wopi_base_url=config.wopi_base_url,
            grant=grant,
        ),
        headers={"Cache-Control": "private, no-store"},
    )


def _verify_content_identity(
    record: CanvasRecord | None,
    *,
    presentation_revision: int,
    source_fingerprint: str,
    source_version: str,
) -> CanvasRecord:
    if record is None or record.source is None:
        raise CanvasFileError(409, "canvas_cleared", "Canvas is cleared")
    if not isinstance(record.source, WorkspaceFileSource):
        raise CanvasFileError(409, "canvas_replaced", "Canvas source was replaced")
    if source_fingerprint != record.source_fingerprint:
        raise CanvasFileError(409, "canvas_replaced", "Canvas source was replaced")
    if presentation_revision != record.presentation_revision:
        raise CanvasFileError(
            409,
            "canvas_presentation_changed",
            "Canvas presentation changed; reload its current state",
        )
    if source_version != record.source_version:
        raise CanvasFileError(
            409,
            "canvas_presentation_changed",
            "Canvas content version changed; reload its current state",
        )
    return record


def _parse_single_range(value: str, size: int) -> tuple[int, int]:
    if size <= 0 or not value.startswith("bytes=") or "," in value:
        raise CanvasFileError(416, "invalid_range", "Only one byte range is supported")
    spec = value[6:].strip()
    match = re.fullmatch(r"([0-9]*)-([0-9]*)", spec)
    if match is None or not any(match.groups()):
        raise CanvasFileError(416, "invalid_range", "Byte range is invalid")
    start_raw, end_raw = match.groups()
    try:
        if not start_raw:
            suffix = int(end_raw)
            if suffix <= 0:
                raise ValueError
            start, end = max(0, size - suffix), size - 1
        else:
            start = int(start_raw)
            end = int(end_raw) if end_raw else size - 1
            if start < 0 or end < start or start >= size:
                raise ValueError
            end = min(end, size - 1)
    except ValueError as exc:
        raise CanvasFileError(
            416, "invalid_range", "Byte range is unsatisfiable"
        ) from exc
    return start, end


def _if_range_allows(value: str | None, file: ValidatedCanvasFile) -> bool:
    if value is None:
        return True
    etag = f'"{file.source_version}"'
    value = value.strip()
    if value.startswith('"') or value.startswith("W/"):
        return value == etag
    if file.last_modified is None:
        return False
    try:
        candidate = parsedate_to_datetime(value)
    except (TypeError, ValueError, OverflowError):
        return False
    if candidate.tzinfo is None:
        return False
    return file.last_modified.replace(microsecond=0) <= candidate.replace(microsecond=0)


def _content_headers(file: ValidatedCanvasFile, *, length: int) -> dict[str, str]:
    disposition = (
        "attachment" if file.renderer in {"html", "html-interactive"} else "inline"
    )
    headers = {
        "ETag": f'"{file.source_version}"',
        "Content-Type": file.media_type,
        "Content-Length": str(length),
        "Accept-Ranges": "bytes",
        "Content-Disposition": (
            f"{disposition}; filename*=UTF-8''{quote(file.filename, safe='')}"
        ),
        "Cache-Control": _CONTENT_CACHE_CONTROL,
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer",
    }
    if file.last_modified is not None:
        headers["Last-Modified"] = format_datetime(file.last_modified, usegmt=True)
    if file.renderer in {"html", "html-interactive"}:
        headers["Content-Security-Policy"] = (
            "default-src 'none'; script-src 'none'; style-src 'none'; "
            "img-src 'none'; connect-src 'none'; form-action 'none'; "
            "base-uri 'none'; object-src 'none'; sandbox"
        )
    return headers


async def _maybe_repin(
    db: Any, thread_id: str, thread: dict[str, Any], record: CanvasRecord
) -> CanvasRecord:
    """Re-bind a stale file Canvas to the current workspace, if the bytes match.

    Cheap to skip: the staleness test is a metadata comparison, so a healthy
    Canvas never touches the workspace here. Only a genuinely stale generation
    pays for a read, and only once per (generation, revision) — a file that has
    really changed must not turn every state GET into an SFTP round-trip.
    """

    source = record.source
    if not isinstance(source, WorkspaceFileSource) or not record.source_version:
        return record
    try:
        generation = current_workspace_generation(thread)
    except CanvasFileError:
        # No workspace bound right now. Nothing to re-pin against; the snapshot
        # fallback in `_represent` covers the user.
        return record
    if generation == source.workspace_generation:
        return record

    attempt = (
        thread_id,
        str(generation),
        record.presentation_revision,
        record.source_version,
    )
    if attempt in _REPIN_ATTEMPTED:
        return record

    gateway = _get_file_gateway(db)
    # Distinguishing these two matters for whether the attempt is worth
    # repeating. A workspace that is still restoring reads as "unreadable" for
    # a while, and treating that as final would strand the Canvas on its stored
    # copy until the next publish or a process restart.
    definitively_different = False

    async def verifier(
        locked: CanvasRecord, locked_thread: dict[str, Any]
    ) -> UUID | None:
        nonlocal definitively_different
        locked_source = locked.source
        if not isinstance(locked_source, WorkspaceFileSource):
            return None
        try:
            current, file = await gateway.validate_for_presentation(
                locked_thread,
                locked_source.path,
                requested_renderer=locked.renderer,
                alt_text=locked.alt_text,
            )
        except CanvasFileError:
            # Gone, unreadable, or not yet restored. Retryable.
            return None
        if file.source_version != locked.source_version:
            # The file really did change. `source_changed` is the honest
            # answer and it already has a "load current version" affordance.
            # Settled, so stop re-reading it on every state GET.
            definitively_different = True
            return None
        return current

    try:
        mutation = await _get_canvas_service(db).repin_workspace_generation(
            thread_id,
            expected_source_version=record.source_version,
            verifier=verifier,
        )
    except Exception:
        logger.exception(
            "Canvas re-pin failed for thread=%s — falling back to the stored copy",
            thread_id,
        )
        return record
    if definitively_different:
        _remember_repin_attempt(attempt)
    return mutation.record if mutation.changed and mutation.record else record


def _remember_repin_attempt(attempt: tuple[str, str, int, str]) -> None:
    # Bounded, per-replica, and deliberately not durable: a restart retrying a
    # hopeless re-pin costs one file read, while an unbounded set would not.
    if len(_REPIN_ATTEMPTED) >= _REPIN_ATTEMPT_CAP:
        _REPIN_ATTEMPTED.pop(next(iter(_REPIN_ATTEMPTED)), None)
    _REPIN_ATTEMPTED[attempt] = True


def _snapshot_as_file(snapshot: CanvasSnapshot, data: bytes) -> ValidatedCanvasFile:
    """Present stored bytes through the same shape as a live workspace read.

    Everything downstream — headers, CSP, disposition, ETag, ranges — then has
    exactly one code path, so a snapshot response cannot drift from the live
    one. The renderer and media type are the ones the bytes were validated
    for at publish time; they are not re-derived from a snapshot row.
    """

    return ValidatedCanvasFile(
        path=snapshot.path,
        data=data,
        media_type=snapshot.media_type,
        renderer=snapshot.renderer,  # type: ignore[arg-type]
        source_version=snapshot.source_version,
        last_modified=snapshot.last_modified,
    )


def _file_mutation_response(
    thread_id: str,
    record: CanvasRecord,
    *,
    can_edit: bool,
) -> Response:
    representation = build_public_canvas_representation(
        record,
        status="ready",
        capabilities=CanvasCapabilities(can_edit=can_edit, can_pop_out=True),
        content_url=_content_url(thread_id, record),
        content_origin="workspace",
    )
    assert record.source_version is not None
    return _state_response(
        representation.payload,
        representation.etag,
        extra_headers={
            "X-Canvas-Content-ETag": f'"{record.source_version}"',
        },
    )


@router.api_route("/main/content", methods=["GET", "HEAD"])
async def get_main_canvas_content(
    thread_id: str,
    request: Request,
    presentation_revision: int = Query(ge=1),
    source_fingerprint: str = Query(pattern=r"^sha256:[0-9a-f]{64}$"),
    source_version: str = Query(pattern=r"^sha256:[0-9a-f]{64}$"),
    ngsw_bypass: Literal["true"] = Query(alias="ngsw-bypass"),
) -> Response:
    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    response_lease: CanvasResponseLease | None = None
    content_origin = "workspace"
    try:
        try:
            service = _get_canvas_service(db)
            record = _verify_content_identity(
                await service.get(thread_id),
                presentation_revision=presentation_revision,
                source_fingerprint=source_fingerprint,
                source_version=source_version,
            )
            if record.renderer == "office":
                raise CanvasFileError(
                    409,
                    "canvas_office_content_unavailable",
                    "Office bytes are available only through the WOPI read path",
                )
            # Reserve a bounded response slot before retaining workspace bytes.
            # Its lifetime transfers to the response for non-empty GET bodies.
            response_lease = await acquire_canvas_response_lease()
            try:
                file = await _get_file_gateway(db).materialize_current(thread, record)
                if file.source_version != record.source_version:
                    raise CanvasFileError(
                        409,
                        "source_changed",
                        "Workspace bytes changed; ask the agent to refresh the Canvas",
                    )
            except CanvasFileError as exc:
                # The workspace cannot answer for these exact bytes. Serve the
                # copy taken when they were published, if we have it. The
                # identity triple above was already verified against the live
                # row, so a snapshot is never addressable on its own.
                if exc.code not in _SNAPSHOT_FALLBACK_CODES:
                    raise
                loaded = await _get_snapshot_store(db).load(
                    thread_id, record.source_version
                )
                if loaded is None:
                    raise
                snapshot, snapshot_bytes = loaded
                file = _snapshot_as_file(snapshot, snapshot_bytes)
                content_origin = "snapshot"
            # The file read can be expensive. Re-check the authoritative Canvas row
            # after it so a concurrent clear/replace cannot release bytes under the
            # old renderer identity.
            _verify_content_identity(
                await service.get(thread_id),
                presentation_revision=presentation_revision,
                source_fingerprint=source_fingerprint,
                source_version=source_version,
            )
            # Authorization may be revoked while SFTP/rclone is materializing.
            # Re-admit immediately before any content response (including 304).
            await require_thread_owner(request, db, thread_id)
        except CanvasFileError:
            raise

        etag = f'"{file.source_version}"'
        headers = _content_headers(file, length=len(file.data))
        headers["X-Canvas-Content-Origin"] = content_origin
        if _if_none_match_matches(request.headers.get("If-None-Match"), etag):
            # A 304 has no response body. Reusing the representation's
            # Content-Length makes Uvicorn correctly reject the framing as a
            # short body, so retain validators/metadata but omit that field.
            not_modified_headers = {
                key: value
                for key, value in headers.items()
                if key.lower() != "content-length"
            }
            return Response(status_code=304, headers=not_modified_headers)

        status_code = 200
        content = file.data
        range_header = request.headers.get("Range")
        if range_header and _if_range_allows(request.headers.get("If-Range"), file):
            try:
                start, end = _parse_single_range(range_header, len(file.data))
            except CanvasFileError:
                return Response(
                    status_code=416,
                    headers={
                        **headers,
                        "Content-Length": "0",
                        "Content-Range": f"bytes */{len(file.data)}",
                    },
                )
            content = file.data[start : end + 1]
            status_code = 206
            headers["Content-Length"] = str(len(content))
            headers["Content-Range"] = f"bytes {start}-{end}/{len(file.data)}"

        if request.method == "HEAD":
            return Response(content=b"", status_code=status_code, headers=headers)
        assert response_lease is not None
        response = _LeasedContentResponse(
            content=content,
            status_code=status_code,
            headers=headers,
            lease=response_lease,
        )
        response_lease = None
        return response
    except CanvasFileError as exc:
        _raise_file_error(exc)
    finally:
        if response_lease is not None:
            response_lease.release()


@router.put("/main/content", response_model=CanvasPublicState)
async def put_main_canvas_content(
    thread_id: str,
    request: Request,
    presentation_revision: int = Query(ge=1),
    source_fingerprint: str = Query(pattern=r"^sha256:[0-9a-f]{64}$"),
    source_version: str = Query(pattern=r"^sha256:[0-9a-f]{64}$"),
    ngsw_bypass: Literal["true"] = Query(alias="ngsw-bypass"),
) -> Response:
    """Conditionally replace one editable text Canvas source."""

    del ngsw_bypass
    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    try:
        expected_source_version, expected_revision = _required_edit_preconditions(
            request
        )
        service = _get_canvas_service(db)
        record = _verify_content_identity(
            await service.get(thread_id),
            presentation_revision=presentation_revision,
            source_fingerprint=source_fingerprint,
            source_version=source_version,
        )
        if record.renderer == "office":
            raise CanvasFileError(
                409,
                "canvas_office_content_unavailable",
                "Office bytes are writable only through the WOPI PutFile path",
            )
        if expected_revision != record.presentation_revision:
            raise CanvasEditError(
                409,
                "canvas_presentation_changed",
                "Canvas presentation changed; reload its current state",
            )
        if not record.editable:
            raise CanvasEditError(
                409,
                "canvas_not_editable",
                "The current Canvas presentation is not editable",
            )
        if not secrets.compare_digest(
            expected_source_version, str(record.source_version)
        ):
            raise CanvasEditError(
                412,
                "canvas_content_precondition_failed",
                "Canvas content changed; reload before saving",
            )
    except CanvasFileError as exc:
        _raise_file_error(exc)
    except CanvasEditError as exc:
        _raise_edit_error(exc)

    try:
        candidate = await _read_bounded_edit_body(request)
        gateway = _get_file_gateway(db)
        await gateway.validate_edit_candidate(record, candidate)
        if not gateway.supports_editing(thread, record):
            raise CanvasFileError(
                409,
                "canvas_editing_unavailable",
                "This workspace is not currently writable through Canvas",
            )
    except CanvasFileError as exc:
        _raise_file_error(exc)

    # Body spooling and candidate validation are complete. Re-admit immediately
    # before taking the cross-replica coordinator and Canvas row lock.
    _, fresh_thread = await require_thread_owner(request, db, thread_id)
    locked_gateway = _get_file_gateway()

    saved: ValidatedCanvasFile | None = None

    async def write_locked(current: CanvasRecord, locked_thread: dict[str, Any]) -> str:
        nonlocal saved
        result = await locked_gateway.replace_current(
            locked_thread,
            current,
            candidate,
            expected_source_version=expected_source_version,
        )
        saved = result
        return result.source_version

    try:
        # A VM save holds the durable VM write lease (a no-op for a container)
        # outside the Canvas and thread row locks edit_file takes.
        async with vm_canvas_write_authority(fresh_thread, store=db):
            mutation = await service.edit_file(
                thread_id,
                expected_presentation_revision=expected_revision,
                expected_source_fingerprint=source_fingerprint,
                expected_source_version=expected_source_version,
                expected_thread_user_id=str(fresh_thread.get("user_id") or ""),
                writer=write_locked,
            )
    except CanvasFileError as exc:
        _raise_file_error(exc)
    except CanvasEditError as exc:
        _raise_edit_error(exc)

    await require_thread_owner(request, db, thread_id)
    assert mutation.record is not None
    if saved is not None:
        await _get_snapshot_store(db).capture(thread_id, saved)
    return _file_mutation_response(
        thread_id,
        mutation.record,
        can_edit=(
            mutation.record.editable
            and gateway.supports_editing(fresh_thread, mutation.record)
        ),
    )


@router.post("/main/refresh", response_model=CanvasPublicState)
async def refresh_main_canvas(thread_id: str, request: Request) -> Response:
    """Validate and adopt the current bytes of the selected workspace file."""

    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    expected_etag = request.headers.get("If-Match")
    if expected_etag is None:
        raise HTTPException(
            status_code=428,
            detail={
                "code": "canvas_precondition_required",
                "message": "If-Match is required to refresh a Canvas",
            },
        )
    expected_etag = strong_state_precondition(expected_etag)
    if not re.fullmatch(r'"canvas:[0-9]+:[0-9a-f]{64}"', expected_etag):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_canvas_precondition",
                "message": "If-Match must contain one strong Canvas state ETag",
            },
        )
    try:
        await _require_empty_refresh_body(request)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    service = _get_canvas_service(db)
    current = await service.get(thread_id)
    if current is None or current.source is None:
        _raise_edit_error(CanvasEditError(409, "canvas_cleared", "Canvas is cleared"))
    if not isinstance(current.source, WorkspaceFileSource):
        _raise_edit_error(
            CanvasEditError(
                409, "canvas_not_file", "The current Canvas is not file-backed"
            )
        )

    visible = await _represent(
        thread_id, thread, current, browser_content_url=True, db=db
    )

    def visible_builder(record: CanvasRecord):
        content_url = (
            _content_url(thread_id, record)
            if visible.state.content_url is not None
            else None
        )
        return build_public_canvas_representation(
            record,
            status=visible.state.status,
            capabilities=visible.state.capabilities,
            content_url=content_url,
            content_origin=visible.state.content_origin,
            content_captured_at=visible.state.content_captured_at,
        )

    _, fresh_thread = await require_thread_owner(request, db, thread_id)
    locked_gateway = _get_file_gateway()

    refreshed: ValidatedCanvasFile | None = None

    async def refresh_locked(
        record: CanvasRecord, locked_thread: dict[str, Any]
    ) -> str:
        nonlocal refreshed
        file = await locked_gateway.materialize_for_refresh(locked_thread, record)
        refreshed = file
        return file.source_version

    try:
        mutation = await service.refresh_file(
            thread_id,
            expected_etag=expected_etag,
            expected_thread_user_id=str(fresh_thread.get("user_id") or ""),
            refresher=refresh_locked,
            representation_builder=visible_builder,
        )
    except CanvasPreconditionRequired as exc:
        raise HTTPException(
            status_code=428,
            detail={"code": "canvas_precondition_required", "message": str(exc)},
        ) from exc
    except CanvasPreconditionFailed as exc:
        raise HTTPException(
            status_code=412,
            detail={"code": "canvas_precondition_failed", "message": str(exc)},
        ) from exc
    except CanvasFileError as exc:
        _raise_file_error(exc)
    except CanvasEditError as exc:
        _raise_edit_error(exc)

    await require_thread_owner(request, db, thread_id)
    assert mutation.record is not None
    if refreshed is not None:
        await _get_snapshot_store(db).capture(thread_id, refreshed)
    return _file_mutation_response(
        thread_id,
        mutation.record,
        can_edit=(
            mutation.record.editable
            and locked_gateway.supports_editing(fresh_thread, mutation.record)
        ),
    )


@router.post("/main/reset-origin", response_model=CanvasPublicState)
async def reset_main_canvas_origin(thread_id: str, request: Request) -> Response:
    """Rotate the current live application's isolated browser origin."""

    db = _get_db(request)
    _, thread = await require_thread_owner(request, db, thread_id)
    expected_etag = request.headers.get("If-Match")
    if expected_etag is None:
        raise HTTPException(
            status_code=428,
            detail={
                "code": "canvas_precondition_required",
                "message": "If-Match is required to reset a Canvas origin",
            },
        )
    expected_etag = strong_state_precondition(expected_etag)
    if not re.fullmatch(r'"canvas:[0-9]+:[0-9a-f]{64}"', expected_etag):
        raise HTTPException(
            status_code=400,
            detail={
                "code": "invalid_canvas_precondition",
                "message": "If-Match must contain one strong Canvas state ETag",
            },
        )
    try:
        await _require_empty_refresh_body(request)
    except CanvasFileError as exc:
        _raise_file_error(exc)

    service = _get_canvas_service(db)
    current = await service.get(thread_id)
    if current is None or current.source is None:
        _raise_edit_error(CanvasEditError(409, "canvas_cleared", "Canvas is cleared"))
    if not isinstance(current.source, WorkspaceAppSource):
        _raise_edit_error(
            CanvasEditError(
                409,
                "canvas_not_live_app",
                "The current Canvas is not a live workspace application",
            )
        )

    visible = await _represent(
        thread_id, thread, current, browser_content_url=True, db=db
    )

    def visible_builder(record: CanvasRecord):
        return build_public_canvas_representation(
            record,
            status=visible.state.status,
            capabilities=visible.state.capabilities,
            content_origin=visible.state.content_origin,
            content_captured_at=visible.state.content_captured_at,
        )

    _, fresh_thread = await require_thread_owner(request, db, thread_id)
    try:
        mutation = await service.reset_origin(
            thread_id,
            expected_etag=expected_etag,
            expected_thread_user_id=str(fresh_thread.get("user_id") or ""),
            representation_builder=visible_builder,
        )
    except CanvasPreconditionRequired as exc:
        raise HTTPException(
            status_code=428,
            detail={"code": "canvas_precondition_required", "message": str(exc)},
        ) from exc
    except CanvasPreconditionFailed as exc:
        raise HTTPException(
            status_code=412,
            detail={"code": "canvas_precondition_failed", "message": str(exc)},
        ) from exc
    except CanvasEditError as exc:
        _raise_edit_error(exc)

    await require_thread_owner(request, db, thread_id)
    assert mutation.record is not None
    representation = visible_builder(mutation.record)
    return _state_response(representation.payload, representation.etag)


@internal_router.get("/main", response_model=CanvasPublicState)
async def internal_get_main_canvas(thread_id: str, request: Request) -> Response:
    db = _get_db(request)
    _, thread = await _require_delegated_owner(request, db, thread_id)
    record = await _get_canvas_service(db).get(thread_id)
    if record is None:
        return _no_content_response()
    representation = await _represent(
        thread_id, thread, record, browser_content_url=False, db=db
    )
    await _require_delegated_owner(request, db, thread_id)
    latest = await _get_canvas_service(db).get(thread_id)
    if (
        latest is not None
        and latest.presentation_revision != record.presentation_revision
    ):
        representation = await _represent(
            thread_id, thread, latest, browser_content_url=False, db=db
        )
        await _require_delegated_owner(request, db, thread_id)
    return _state_response(representation.payload, representation.etag)


@internal_router.post("/main/set", response_model=CanvasPublicState)
async def internal_set_main_canvas(
    thread_id: str, request: Request, body: CanvasSetRequest
) -> Response:
    db = _get_db(request)
    _, thread = await _require_delegated_owner(request, db, thread_id)

    if body.source_type == "browser":

        async def generation_resolver() -> dict[str, Any]:
            current = await db.get_thread(thread_id)
            return dict(current) if current else {}

        try:
            prepared = await prepare_browser_canvas(
                thread,
                initial_baton="agent",
                generation_resolver=generation_resolver,
            )
            # Startup can outlive the delegated admission or the workspace
            # binding. Re-authorize and re-evaluate the positive capability
            # immediately before the idempotent durable transition.
            _, fresh_thread = await _require_delegated_owner(request, db, thread_id)
            require_browser_capability(
                browser_capability(fresh_thread),
                require_ready=True,
            )
            mutation = await commit_browser_canvas(
                db,
                thread_id,
                prepared,
                title=body.title or "Shared browser",
            )
        except BrowserCanvasError as exc:
            _raise_browser_error(exc)

        assert mutation.record is not None
        _, visible_thread = await _require_delegated_owner(request, db, thread_id)
        representation = await _represent(
            thread_id,
            visible_thread,
            mutation.record,
            browser_content_url=False,
            db=db,
        )
        return _state_response(
            representation.payload,
            representation.etag,
            extra_headers={
                "X-Canvas-Mutation-Changed": str(mutation.changed).lower(),
            },
        )

    if body.source_type == "workspace_port":
        assert body.port is not None
        if not canvas_live_preview_enabled():
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "canvas_live_preview_disabled",
                    "message": "Canvas live preview is not enabled",
                },
            )

        app_gateway = _get_app_gateway(db)
        try:
            validated = await app_gateway.validate_for_presentation(
                thread,
                body.port,
                entry_path=body.entry_path or "/",
            )
        except CanvasAppError as exc:
            _raise_app_error(exc)

        # Remote readiness can take long enough for the delegated owner,
        # deployment gate, or workspace generation to change. Re-admit and
        # re-bind immediately before committing the durable pointer.
        _, fresh_thread = await _require_delegated_owner(request, db, thread_id)
        if not canvas_live_preview_enabled():
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "canvas_live_preview_disabled",
                    "message": "Canvas live preview is not enabled",
                },
            )
        try:
            app_gateway.revalidate_for_commit(fresh_thread, validated)
        except CanvasAppError as exc:
            _raise_app_error(exc)

        mutation = await _get_canvas_service(db).set(
            thread_id,
            CanvasSetInput(
                source=validated.source,
                title=body.title or "Workspace application",
                renderer="auto",
                editable=False,
                alt_text=None,
                source_version=None,
                new_app=body.new_app,
            ),
        )
        assert mutation.record is not None
        representation = build_public_canvas_representation(
            mutation.record,
            status=validated.status,
            capabilities=CanvasCapabilities(),
            content_url=None,
        )
        return _state_response(
            representation.payload,
            representation.etag,
            extra_headers={"X-Canvas-Mutation-Changed": "true"},
        )

    assert body.path is not None
    gateway = _get_file_gateway(db)
    try:
        generation, file = await gateway.validate_for_presentation(
            thread,
            body.path,
            requested_renderer=body.renderer,
            alt_text=body.alt_text,
        )
        if file.renderer == "office":
            await _require_office_view(file.path)
        if body.editable and (
            file.renderer
            not in {"markdown", "text", "html", "html-interactive", "office"}
            or not gateway.supports_editing(thread)
        ):
            raise CanvasFileError(
                422,
                "canvas_editing_unsupported",
                "This Canvas source or workspace does not support editing",
            )
    except CanvasFileError as exc:
        _raise_file_error(exc)

    title = body.title or PurePosixPath(file.path).name[:200]
    # The delegated user/thread relationship may be revoked during the remote
    # file validation above. Do not commit after that authorization expires.
    _, fresh_thread = await _require_delegated_owner(request, db, thread_id)
    try:
        if current_workspace_generation(fresh_thread) != generation:
            raise CanvasFileError(
                409,
                "workspace_generation_changed",
                "The workspace changed while the Canvas file was being validated",
            )
        if file.renderer == "office":
            await _require_office_view(file.path)
        if body.editable and not gateway.supports_editing(fresh_thread):
            raise CanvasFileError(
                422,
                "canvas_editing_unsupported",
                "This workspace no longer supports Canvas editing",
            )
    except CanvasFileError as exc:
        _raise_file_error(exc)
    mutation = await _get_canvas_service(db).set(
        thread_id,
        CanvasSetInput(
            source=WorkspaceFileSource(path=file.path, workspace_generation=generation),
            title=title,
            renderer=file.renderer,
            editable=body.editable,
            alt_text=body.alt_text.strip() if body.alt_text else None,
            source_version=file.source_version,
        ),
    )
    assert mutation.record is not None
    # Remember exactly these published bytes so the presentation outlives the
    # workspace pod. Best-effort by contract: a snapshot failure must never
    # turn a successful `set_canvas` into a failed tool call.
    await _get_snapshot_store(db).capture(thread_id, file)
    representation = build_public_canvas_representation(
        mutation.record,
        status="ready",
        capabilities=CanvasCapabilities(
            can_edit=(
                mutation.record.editable
                and gateway.supports_editing(fresh_thread, mutation.record)
            ),
            can_pop_out=True,
            can_view_office=mutation.record.renderer == "office",
        ),
        content_origin="workspace",
    )
    return _state_response(
        representation.payload,
        representation.etag,
        extra_headers={"X-Canvas-Mutation-Changed": "true"},
    )


@internal_router.delete("/main", response_model=CanvasPublicState)
async def internal_clear_main_canvas(thread_id: str, request: Request) -> Response:
    db = _get_db(request)
    await _require_delegated_owner(request, db, thread_id)
    mutation = await _get_canvas_service(db).clear(
        thread_id,
        expected_etag=None,
        require_precondition=False,
    )
    if mutation.record is None:
        return _no_content_response()
    representation = build_public_canvas_representation(mutation.record)
    return _state_response(
        representation.payload,
        representation.etag,
        extra_headers={
            "X-Canvas-Mutation-Changed": "true" if mutation.changed else "false"
        },
    )


__all__ = ["internal_router", "router"]
