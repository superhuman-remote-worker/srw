"""The application's HTTP stack: response encoding, exception handlers, middleware.

``install(app)`` registers everything in the order ``orchestrator.main`` used
at import, which is load-bearing for middleware: Starlette runs the
most recently added middleware outermost. The resulting stack, outermost
first, is CorrelationId → trusted-parent anti-framing → request logging →
CORS → CSRF → the application.
"""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from urllib.parse import urlparse
from uuid import UUID

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from orchestrator.logging_config import CorrelationIdMiddleware
from orchestrator.security.csrf import CSRFMiddleware
from orchestrator.services.gitea import GiteaPathError
from shared.anti_framing import TrustedParentAntiFramingMiddleware
from shared.runtime.core.model_registry import UnknownModelError

logger = logging.getLogger(__name__)


class CustomJSONEncoder(json.JSONEncoder):
    """JSON encoder that handles PostgreSQL types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            # Ensure timestamps include UTC indicator for proper browser parsing
            if obj.tzinfo is None:
                # Naive datetime - assume UTC and add Z suffix
                return obj.isoformat() + "Z"
            else:
                # Timezone-aware - convert to UTC and use Z suffix
                utc_dt = obj.astimezone(timezone.utc)
                return utc_dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class CustomJSONResponse(JSONResponse):
    """JSON response that uses custom encoder."""

    def render(self, content: Any) -> bytes:
        return json.dumps(
            content,
            cls=CustomJSONEncoder,
            ensure_ascii=False,
            allow_nan=False,
            indent=None,
            separators=(",", ":"),
        ).encode("utf-8")


async def unknown_model_handler(
    request: Request, exc: UnknownModelError
) -> JSONResponse:
    """Translate registry misses into a helpful 400 instead of a 500.

    Fires whenever a request references a model ID that isn't in the
    admin-curated catalog. Points operators at the admin surface where they
    can register the model.
    """
    return JSONResponse(
        status_code=400,
        content={
            "detail": str(exc),
            "model_id": exc.model_id,
            "hint": (
                "Register this model under Admin → Models (anchored to a "
                "system provider key or a system endpoint from Admin → "
                "Providers), or pick an ID from /api/models."
            ),
        },
    )


async def gitea_path_error_handler(
    request: Request, exc: GiteaPathError
) -> JSONResponse:
    """Refuse a caller-shaped repository path or name with a 400, not a 500.

    ``services.gitea`` validates every path, ref and repository name before
    a request leaves the process (the client authenticates as the Gitea
    instance administrator, so an unencoded ``..`` would re-target another
    owner's repository). The refusal is about the caller's input, not a
    server fault, so surface it as such -- this covers every route that
    reaches the sink, including the MCP-facing ``/repo/file`` and
    ``/repo/contents`` proxies.
    """
    return JSONResponse(status_code=400, content={"detail": str(exc)})


async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Return a CORS-friendly 500 for any otherwise-unhandled exception.

    Without this, Starlette's outermost ServerErrorMiddleware produces a
    bare 500 that skips the CORSMiddleware on the way out. Browsers then
    drop the response (no Access-Control-Allow-Origin header) and Angular
    surfaces it as a status-0 "network failure" instead of a real 5xx,
    which makes server-side bugs look like client-side connectivity
    issues. Handling here keeps the response inside the middleware stack
    so CORS headers are attached.

    HTTPException / RequestValidationError / UnknownModelError are
    dispatched to their own handlers first, so they don't reach here.
    """
    logger.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


# Request logging middleware — replaces uvicorn's shallow access log with
# app-level logging that includes response timing and error tracebacks.
_SILENT_PATHS = {"/api/health"}
_SILENT_PREFIXES = ("/api/ide/",)  # suppress per-asset log spam from IDE proxy


async def request_logging_middleware(request: Request, call_next):
    path = request.url.path
    if path in _SILENT_PATHS or path.startswith(_SILENT_PREFIXES):
        return await call_next(request)

    method = request.method
    start = time.perf_counter()
    # request_id is bound upstream by CorrelationIdMiddleware (outermost), so it
    # tags both this access line and the route handler's logs.
    try:
        response = await call_next(request)
    except Exception:
        elapsed = (time.perf_counter() - start) * 1000
        logger.exception(
            "%s %s 500 (%dms) — unhandled exception", method, path, elapsed
        )
        return JSONResponse(
            status_code=500, content={"detail": "Internal server error"}
        )

    elapsed = (time.perf_counter() - start) * 1000
    status = response.status_code
    if status >= 500:
        logger.warning("%s %s %d (%dms)", method, path, status, elapsed)
    else:
        logger.info("%s %s %d (%dms)", method, path, status, elapsed)
    return response


# Trusted Cockpit/BFF responses must never become documents inside an untrusted
# Canvas app iframe. Register this outside route/CORS/CSRF handling so redirects,
# errors, and same-origin API responses receive the same response boundary. It
# appends (rather than replaces) any route-specific CSP.
#
# The IDE proxy is the one intentional frame-based application on this ASGI
# service (VS Code webviews). It receives a same-origin-only framing policy on
# the exact, separately hosted IDE/API authority. The same path on a Cockpit
# authority is still denied, closing the Canvas self-navigation boundary without
# breaking code-server webviews on api.<domain>.
def origin_authority(value: str) -> tuple[str, int] | None:
    try:
        parsed = urlparse(value.strip())
    except ValueError:
        return None
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.path not in {"", "/"}
        or parsed.params
        or parsed.query
        or parsed.fragment
    ):
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    try:
        hostname = parsed.hostname
        port = parsed.port
        if not hostname:
            return None
        hostname = hostname.rstrip(".").encode("idna").decode("ascii").lower()
    except (UnicodeError, ValueError):
        return None
    if not hostname:
        return None
    return hostname, port or (443 if parsed.scheme == "https" else 80)


def origin_host_headers(value: str) -> tuple[str, ...]:
    """Host spellings a proxy may preserve for one configured web origin."""

    authority = origin_authority(value)
    if authority is None:
        return ()
    hostname, port = authority
    host_literal = f"[{hostname}]" if ":" in hostname else hostname
    parsed = urlparse(value.strip())
    default_port = 443 if parsed.scheme == "https" else 80
    if port == default_port:
        return host_literal, f"{host_literal}:{port}"
    return (f"{host_literal}:{port}",)


def isolated_ide_frame_authorities() -> dict[str, tuple[str, ...]]:
    cockpit_origins = {
        "http://localhost:4200",
        "http://127.0.0.1:4200",
        "http://localhost:4000",
        "http://127.0.0.1:4000",
        os.environ.get("SRW_SPA_BASE_URL", ""),
        *os.environ.get("CORS_ORIGINS", "").split(","),
    }
    cockpit_authorities = {
        authority
        for origin in cockpit_origins
        if (authority := origin_authority(origin)) is not None
    }
    ide_origin = os.environ.get("IDE_PROXY_BASE_URL", "http://localhost:8085")
    ide_authority = origin_authority(ide_origin)
    if ide_authority is None or ide_authority in cockpit_authorities:
        return {}
    return {"/api/ide/": origin_host_headers(ide_origin)}


def install(app: FastAPI) -> None:
    """Register middleware and exception handlers in their fixed order."""

    # CSRF defense for the cookie BFF. Middleware order matters: Starlette
    # runs the OUTERMOST `add_middleware` last, so we add CSRF first and CORS
    # second. Result: incoming request → CORS preflight/origin handling →
    # CSRF check → app. That means OPTIONS preflights are still answered by
    # CORS (which is good — preflights are unauthenticated), while real
    # POST/PUT/DELETE/PATCH requests get the layered Sec-Fetch-Site +
    # X-CSRF + Origin allowlist check before they reach any handler.
    app.add_middleware(CSRFMiddleware)

    # CORS for Angular frontend (dev server on 4200, production/SSR on 4000)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:4200",
            "http://127.0.0.1:4200",
            "http://localhost:4000",
            "http://127.0.0.1:4000",
        ]
        + [o for o in os.environ.get("CORS_ORIGINS", "").split(",") if o],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
        # Canvas uses strong state/content ETags as mutation preconditions. The
        # local Cockpit dev server (4200) calls the orchestrator (8085)
        # cross-origin, so both non-secret response headers must be readable by
        # HttpClient there. Production remains same-origin behind the BFF.
        expose_headers=["ETag", "X-Canvas-Content-ETag", "X-Canvas-Mutation-Changed"],
    )

    app.add_exception_handler(UnknownModelError, unknown_model_handler)
    app.add_exception_handler(GiteaPathError, gitea_path_error_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler)

    app.middleware("http")(request_logging_middleware)

    app.add_middleware(
        TrustedParentAntiFramingMiddleware,
        same_origin_frameable_path_authorities=isolated_ide_frame_authorities(),
    )

    # request_id correlation — added last so it is OUTERMOST (wraps both the
    # access-log and anti-framing middleware above). See CorrelationIdMiddleware in
    # logging_config.
    app.add_middleware(CorrelationIdMiddleware)


__all__ = [
    "CustomJSONEncoder",
    "CustomJSONResponse",
    "install",
    "isolated_ide_frame_authorities",
    "origin_authority",
    "origin_host_headers",
]
