"""Internal inventory publication with bounds before JSON parsing and HMAC."""

from dataclasses import dataclass
import hmac
from typing import Callable, Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from shared.vm_inventory_transport import (
    ENVELOPE_ALLOWANCE,
    OPERATION,
    decode_document,
    validate_envelope,
)
from shared.vm_lifecycle_auth import configured_secret, sign_payload, verify_payload
from shared.vm_resource_inventory import (
    InventoryError,
    canonical_snapshot,
    snapshot_digest,
)


router = APIRouter(prefix="/api/internal/vm-resource-inventory")


@dataclass(frozen=True)
class Configuration:
    store_factory: Callable[[], Any]
    max_items: int
    max_bytes: int


_configuration: Configuration | None = None


def configure(*, store_factory, max_items, max_bytes):
    global _configuration
    if any(type(n) is not int or not 1 <= n < 2**63 for n in (max_items, max_bytes)):
        raise InventoryError("invalid_inventory_configuration")
    _configuration = Configuration(store_factory, max_items, max_bytes)


def configure_from_environment(db):
    """Enable only from the same explicit policy used by the controller."""
    from shared.vm_resource_inventory_settings import InventorySettings
    from orchestrator.services.vm_resource_inventory_store import (
        VMResourceInventoryStore,
    )

    global _configuration
    settings = InventorySettings.from_environment()
    if settings is None:
        _configuration = None
        return
    store = VMResourceInventoryStore(
        db,
        cluster_id=settings.cluster_id,
        namespace=settings.namespace,
        policy_digest=settings.policy_digest,
        label_keys=settings.label_keys,
        max_items=settings.max_items,
        max_bytes=settings.max_bytes,
        stale_after_seconds=settings.stale_after_seconds,
        history_limit=settings.history_limit,
    )
    configure(
        store_factory=lambda: store,
        max_items=settings.max_items,
        max_bytes=settings.max_bytes,
    )


def _plain(reason, status):
    return JSONResponse({"detail": reason}, status_code=status)


@router.post("/publish")
async def publish(request: Request):
    config = _configuration
    try:
        secret = configured_secret()
    except ValueError:
        secret = None
    if config is None or secret is None:
        return _plain("inventory_disabled", 503)
    if request.headers.get("content-encoding", "identity").lower() != "identity":
        return _plain("unsupported_encoding", 415)
    limit = config.max_bytes + ENVELOPE_ALLOWANCE
    try:
        declared = request.headers.get("content-length")
        if declared is not None:
            if len(declared) > 20 or not declared.isascii() or not declared.isdecimal():
                return _plain("invalid_inventory_transport", 400)
            if int(declared) > limit:
                return _plain("byte_limit", 413)
        raw = bytearray()
        async for chunk in request.stream():
            if len(chunk) > limit - len(raw):
                return _plain("byte_limit", 413)
            raw.extend(chunk)
        value = decode_document(bytes(raw), max_bytes=limit)
        auth = validate_envelope(
            value, payload_fields={"snapshot", "digest"}, direction="request"
        )
        snapshot = canonical_snapshot(
            value["snapshot"], max_items=config.max_items, max_bytes=config.max_bytes
        )
        digest = value["digest"]
        if (
            not isinstance(digest, str)
            or len(digest) != 71
            or not hmac.compare_digest(snapshot_digest(snapshot), digest)
        ):
            raise InventoryError("inventory_digest_changed")
    except (ValueError, TypeError, UnicodeError):
        return _plain("invalid_inventory_transport", 400)
    if not verify_payload(
        value, direction="request", operation=OPERATION, secret=secret
    ):
        return _plain("unauthenticated", 401)
    correlation = auth["request_id"]
    try:
        receipt = await config.store_factory().publish(snapshot=snapshot, digest=digest)
        result, status = {**receipt, "complete": snapshot["complete"]}, 200
    except InventoryError:
        result, status = {"accepted": False, "reason": "inventory_refused"}, 409
    except Exception:
        result, status = {"accepted": False, "reason": "inventory_unavailable"}, 503
    return JSONResponse(
        sign_payload(
            result,
            direction="response",
            operation=OPERATION,
            secret=secret,
            correlation_id=correlation,
        ),
        status_code=status,
    )
