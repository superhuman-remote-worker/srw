"""Token-authenticated WOPI host for Canvas Office documents."""

from __future__ import annotations

from datetime import datetime, timezone
import logging
from pathlib import PurePosixPath
import secrets
from tempfile import SpooledTemporaryFile
from typing import Any
from urllib.parse import quote

from fastapi import APIRouter, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse

from orchestrator.services.canvas import CanvasEditError, CanvasService
from orchestrator.services.canvas_files import (
    MAX_OFFICE_BYTES,
    CanvasFileError,
    CanvasResponseLease,
    ThreadWorkspaceFileGateway,
    ValidatedCanvasFile,
    acquire_canvas_response_lease,
    vm_canvas_write_authority,
)
from orchestrator.services.canvas_office import (
    CanvasOfficeError,
    WopiAccess,
    collabora_config,
    create_wopi_token_service,
)

router = APIRouter(prefix="/wopi", tags=["WOPI"])
logger = logging.getLogger(__name__)


class _LeasedWopiResponse(Response):
    """Retain bounded response capacity until the Office bytes finish sending."""

    def __init__(self, *args: Any, lease: CanvasResponseLease, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self._canvas_lease = lease

    async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            self._canvas_lease.release()


def _get_db(request: Request) -> Any:
    """Resolve the store of the application handling *this* request.

    Composition publishes it as ``app.state.store``; reading it here rather
    than importing ``orchestrator.main`` keeps this module free of the
    application module while still resolving per call, never at import, so
    two applications in one process each keep their own store (R1.B04
    caller-boundary closure, same shape as ``uploads.py`` in B03).
    """

    return request.app.state.store


def _get_token_service(db: Any):
    return create_wopi_token_service(db)


def _get_file_gateway(db: Any | None) -> ThreadWorkspaceFileGateway:
    return ThreadWorkspaceFileGateway(
        thread_loader=getattr(db, "get_thread", None) if db is not None else None
    )


def _get_canvas_service(db: Any) -> CanvasService:
    return CanvasService(db)


def _get_collabora_config():
    return collabora_config()


def _raise_office_error(error: CanvasOfficeError) -> None:
    raise HTTPException(
        status_code=error.status_code,
        detail={"code": error.code, "message": error.message},
    ) from error


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


def _access_token(request: Request, query_token: str | None) -> str:
    bearer: str | None = None
    authorization = request.headers.get("Authorization")
    if authorization is not None:
        scheme, separator, value = authorization.strip().partition(" ")
        if separator and scheme.lower() == "bearer" and value.strip():
            bearer = value.strip()
        else:
            raise CanvasOfficeError(
                401, "wopi_token_invalid", "A valid WOPI bearer token is required"
            )
    token = (query_token or "").strip()
    if bearer is not None and token and not secrets.compare_digest(bearer, token):
        raise CanvasOfficeError(
            401, "wopi_token_invalid", "Conflicting WOPI access tokens were supplied"
        )
    resolved = bearer or token
    if not resolved or len(resolved) > 8192:
        raise CanvasOfficeError(
            401, "wopi_token_invalid", "A WOPI access token is required"
        )
    return resolved


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return (
        value.astimezone(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


async def _authenticate(
    request: Request,
    file_id: str,
    access_token: str | None,
    *,
    require_write: bool = False,
) -> WopiAccess:
    token = _access_token(request, access_token)
    return await _get_token_service(_get_db(request)).authenticate(
        token,
        file_id=file_id,
        require_write=require_write,
    )


async def _recheck(
    request: Request,
    file_id: str,
    access_token: str | None,
    *,
    require_write: bool = False,
) -> WopiAccess:
    """Repeat live admission after the potentially slow workspace read."""

    return await _authenticate(
        request,
        file_id,
        access_token,
        require_write=require_write,
    )


def _parse_cool_timestamp(value: str) -> datetime | None:
    value = value.strip()
    if not value or len(value) > 128:
        return None
    try:
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _stored_last_modified(
    file: ValidatedCanvasFile,
    access: WopiAccess,
) -> datetime:
    return file.last_modified or access.record.updated_at


async def _read_bounded_office_body(request: Request) -> bytes:
    """Spool one PutFile body under the Office ceiling, outside Canvas locks."""

    declared: int | None = None
    raw_length = request.headers.get("Content-Length")
    if raw_length is not None:
        try:
            declared = int(raw_length)
        except ValueError as exc:
            raise CanvasFileError(
                400,
                "invalid_canvas_content",
                "Content-Length is invalid",
            ) from exc
        if declared < 0:
            raise CanvasFileError(
                400,
                "invalid_canvas_content",
                "Content-Length is invalid",
            )
        if declared > MAX_OFFICE_BYTES:
            raise CanvasFileError(
                413,
                "canvas_file_too_large",
                "Office document is too large",
            )

    spool = SpooledTemporaryFile(max_size=min(MAX_OFFICE_BYTES, 1024 * 1024))
    total = 0
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            total += len(chunk)
            if total > MAX_OFFICE_BYTES:
                raise CanvasFileError(
                    413,
                    "canvas_file_too_large",
                    "Office document is too large",
                )
            spool.write(chunk)
        if declared is not None and declared != total:
            raise CanvasFileError(
                400,
                "invalid_canvas_content",
                "Content-Length does not match the received Office content",
            )
        spool.seek(0)
        return spool.read()
    finally:
        spool.close()


def _cool_save_flag(request: Request, name: str) -> bool | None:
    value = request.headers.get(name)
    if value is None:
        return None
    return value.strip().lower() == "true"


# nosec: public WOPI CheckFileInfo — access_token gate validated in-handler
# The Office host echoes the token back on every callback; see _access_token().
@router.get("/files/{file_id}")
async def check_file_info(
    file_id: str,
    request: Request,
    access_token: str | None = Query(default=None),
) -> Response:
    """WOPI CheckFileInfo for one current Office Canvas."""

    response_lease: CanvasResponseLease | None = None
    try:
        config = _get_collabora_config().require_enabled()
        access = await _authenticate(request, file_id, access_token)
        response_lease = await acquire_canvas_response_lease()
        file = await _get_file_gateway(_get_db(request)).materialize_binary(
            access.thread,
            access.record,
        )
        access = await _recheck(request, file_id, access_token)
        owner_id = str(access.thread.get("user_id") or access.user["id"])
        modified = file.last_modified or access.record.updated_at
        payload = {
            "BaseFileName": PurePosixPath(file.path).name,
            "OwnerId": owner_id,
            "Size": len(file.data),
            "UserId": str(access.user["id"]),
            "UserFriendlyName": str(
                access.user.get("display_name") or access.user["id"]
            ),
            "LastModifiedTime": _utc_iso(modified),
            "PostMessageOrigin": config.cockpit_origin,
            "SupportsLocks": False,
            "UserCanNotWriteRelative": True,
        }
        if access.record.editable and access.claims["write_flag"] is True:
            payload["UserCanWrite"] = True
        return JSONResponse(
            payload,
            headers={"Cache-Control": "private, no-store"},
        )
    except CanvasOfficeError as exc:
        _raise_office_error(exc)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    finally:
        if response_lease is not None:
            response_lease.release()


# nosec: public WOPI PutFile — access_token gate, write scope checked in-handler
# Same token path as CheckFileInfo, with require_write=True.
@router.post("/files/{file_id}/contents")
async def put_file(
    file_id: str,
    request: Request,
    access_token: str | None = Query(default=None),
) -> Response:
    """WOPI PutFile through the shared Canvas edit coordinator."""

    try:
        _get_collabora_config().require_enabled()
        if request.headers.get("X-WOPI-Override", "").strip().upper() != "PUT":
            raise CanvasOfficeError(
                400,
                "wopi_override_invalid",
                "X-WOPI-Override: PUT is required for PutFile",
            )
        access = await _authenticate(
            request,
            file_id,
            access_token,
            require_write=True,
        )
        candidate = await _read_bounded_office_body(request)

        # A slow upload holds neither the advisory coordinator nor a Canvas row
        # lock. Re-admit it, then validate and hash the current binary bytes.
        access = await _recheck(
            request,
            file_id,
            access_token,
            require_write=True,
        )
        db = _get_db(request)
        gateway = _get_file_gateway(db)
        if not gateway.supports_editing(access.thread, access.record):
            raise CanvasOfficeError(
                403,
                "wopi_access_denied",
                "The Office Canvas workspace is not writable",
            )
        await gateway.validate_edit_candidate(access.record, candidate)
        current_file = await gateway.materialize_binary(
            access.thread,
            access.record,
        )
        access = await _recheck(
            request,
            file_id,
            access_token,
            require_write=True,
        )

        supplied_timestamp = request.headers.get("X-COOL-WOPI-Timestamp")
        if supplied_timestamp is not None:
            supplied = _parse_cool_timestamp(supplied_timestamp)
            stored = _parse_cool_timestamp(
                _utc_iso(_stored_last_modified(current_file, access))
            )
            if supplied is None or supplied != stored:
                return JSONResponse(
                    {"COOLStatusCode": 1010},
                    status_code=409,
                    headers={"Cache-Control": "private, no-store"},
                )

        logger.info(
            "Canvas Office PutFile thread=%s path=%s autosave=%s "
            "exit_save=%s modified_by_user=%s",
            access.record.thread_id,
            access.claims["path"],
            _cool_save_flag(request, "X-COOL-WOPI-IsAutosave"),
            _cool_save_flag(request, "X-COOL-WOPI-IsExitSave"),
            _cool_save_flag(request, "X-COOL-WOPI-IsModifiedByUser"),
        )

        record = access.record
        if record.source_fingerprint is None or record.source_version is None:
            raise CanvasOfficeError(
                409,
                "wopi_access_denied",
                "The Office Canvas no longer has a writable file identity",
            )
        locked_gateway = _get_file_gateway(None)
        saved_file: ValidatedCanvasFile | None = None

        async def write_locked(current, locked_thread):
            nonlocal saved_file
            saved_file = await locked_gateway.replace_current_binary(
                locked_thread,
                current,
                candidate,
                expected_source_version=record.source_version,
            )
            return saved_file.source_version

        # A VM save holds the durable VM write lease (a no-op for a container)
        # outside the Canvas and thread row locks edit_file takes.
        async with vm_canvas_write_authority(access.thread, store=db):
            mutation = await _get_canvas_service(db).edit_file(
                record.thread_id,
                expected_presentation_revision=record.presentation_revision,
                expected_source_fingerprint=record.source_fingerprint,
                expected_source_version=record.source_version,
                expected_thread_user_id=str(access.thread.get("user_id") or ""),
                writer=write_locked,
            )
        assert mutation.record is not None
        assert saved_file is not None
        modified = saved_file.last_modified or mutation.record.updated_at
        return JSONResponse(
            {"LastModifiedTime": _utc_iso(modified)},
            headers={"Cache-Control": "private, no-store"},
        )
    except CanvasOfficeError as exc:
        _raise_office_error(exc)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    except CanvasEditError as exc:
        _raise_edit_error(exc)


# nosec: public WOPI GetFile — same access_token gate as CheckFileInfo
@router.get("/files/{file_id}/contents")
async def get_file(
    file_id: str,
    request: Request,
    access_token: str | None = Query(default=None),
) -> Response:
    """WOPI GetFile through the binary-only bounded Canvas gateway."""

    response_lease: CanvasResponseLease | None = None
    try:
        _get_collabora_config().require_enabled()
        access = await _authenticate(request, file_id, access_token)
        response_lease = await acquire_canvas_response_lease()
        file = await _get_file_gateway(_get_db(request)).materialize_binary(
            access.thread,
            access.record,
        )
        await _recheck(request, file_id, access_token)
        headers = {
            "Cache-Control": "private, no-store",
            "Content-Disposition": (
                "inline; filename*=UTF-8''" + quote(file.filename, safe="")
            ),
            "Content-Length": str(len(file.data)),
            "Content-Type": file.media_type,
            "ETag": f'"{file.source_version}"',
            "X-Content-Type-Options": "nosniff",
        }
        assert response_lease is not None
        response = _LeasedWopiResponse(
            content=file.data,
            status_code=200,
            headers=headers,
            lease=response_lease,
        )
        response_lease = None
        return response
    except CanvasOfficeError as exc:
        _raise_office_error(exc)
    except CanvasFileError as exc:
        _raise_file_error(exc)
    finally:
        if response_lease is not None:
            response_lease.release()


__all__ = ["check_file_info", "get_file", "put_file", "router"]
