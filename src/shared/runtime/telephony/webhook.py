"""webhook — verify and parse OpenAI's incoming-call webhook.

OpenAI signs webhooks with Standard Webhooks: the ``webhook-signature`` header
holds one or more space-separated ``v1,<base64>`` signatures, each an
HMAC-SHA256 over ``{webhook-id}.{webhook-timestamp}.{raw body}`` keyed with the
base64-decoded part of the ``whsec_`` secret. Several signatures appear during
a secret rotation; any match is valid. A timestamp outside the tolerance window
is refused so a captured delivery cannot be replayed.

Verify against the raw request body before parsing it. ``webhook-id`` is the
idempotency key: OpenAI redelivers until it sees a 2xx, so callers deduplicate
on it.

Standard library only.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple

DEFAULT_TOLERANCE_S = 300

INCOMING_CALL_EVENT = "live.transport.incoming"
# The deprecated name: no ``data.type``; still delivered to old subscriptions.
LEGACY_INCOMING_CALL_EVENT = "live.call.incoming"


class WebhookVerificationError(ValueError):
    """The delivery is not provably from OpenAI (or is stale). Answer 400/401."""


def _secret_key(secret: str) -> bytes:
    raw = (secret or "").strip()
    if raw.startswith("whsec_"):
        raw = raw[len("whsec_") :]
    try:
        key = base64.b64decode(raw, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise WebhookVerificationError("webhook secret is not valid base64") from exc
    if not key:
        raise WebhookVerificationError("webhook secret is empty")
    return key


def _header(headers: Mapping[str, str], name: str) -> Optional[str]:
    value = headers.get(name)
    if value is None:
        lowered = name.lower()
        for key, candidate in headers.items():
            if key.lower() == lowered:
                return candidate
    return value


def sign_webhook(secret: str, webhook_id: str, timestamp: int, body: bytes) -> str:
    """Return the ``webhook-signature`` header value for a delivery.

    Used by tests and the fake GPT-Live server; production only verifies.
    """

    signed = f"{webhook_id}.{int(timestamp)}.".encode() + body
    digest = hmac.new(_secret_key(secret), signed, hashlib.sha256).digest()
    return "v1," + base64.b64encode(digest).decode()


def verify_webhook(
    secret: str,
    headers: Mapping[str, str],
    body: bytes,
    *,
    tolerance_s: int = DEFAULT_TOLERANCE_S,
    now: Optional[float] = None,
) -> Mapping[str, Any]:
    """Check the signature and freshness of one delivery and return its JSON.

    Raises :class:`WebhookVerificationError` on any failure; the caller must
    not act on an unverified body.
    """

    webhook_id = _header(headers, "webhook-id")
    timestamp_raw = _header(headers, "webhook-timestamp")
    signature_header = _header(headers, "webhook-signature")
    if not webhook_id or not timestamp_raw or not signature_header:
        raise WebhookVerificationError(
            "missing webhook-id, webhook-timestamp or webhook-signature"
        )
    try:
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise WebhookVerificationError("webhook-timestamp is not an integer") from exc
    current = time.time() if now is None else now
    if abs(current - timestamp) > tolerance_s:
        raise WebhookVerificationError(
            "webhook-timestamp is outside the tolerance window"
        )

    expected = sign_webhook(secret, webhook_id, timestamp, body).split(",", 1)[1]
    matched = False
    for candidate in signature_header.split():
        version, _, value = candidate.partition(",")
        if version != "v1" or not value:
            continue
        if hmac.compare_digest(value.encode(), expected.encode()):
            matched = True
            break
    if not matched:
        raise WebhookVerificationError("no webhook signature matched")

    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise WebhookVerificationError("webhook body is not JSON") from exc
    if not isinstance(payload, dict):
        raise WebhookVerificationError("webhook body is not a JSON object")
    return payload


@dataclass(frozen=True)
class IncomingCall:
    """An incoming SIP call waiting for an accept or reject decision."""

    webhook_id: str
    event_id: Optional[str]
    event_type: str
    session_id: str
    sip_headers: Tuple[Tuple[str, str], ...]
    created_at: Optional[int] = None

    def sip_header(self, name: str) -> Optional[str]:
        """First value of a SIP header, case-insensitive. Untrusted metadata."""

        lowered = name.lower()
        for key, value in self.sip_headers:
            if key.lower() == lowered:
                return value
        return None


def parse_incoming_call(
    payload: Mapping[str, Any], webhook_id: str
) -> Optional[IncomingCall]:
    """Return the incoming call a verified payload announces, else ``None``.

    ``None`` means "not a call for us" (another event type, or a non-SIP
    transport); answer 2xx and ignore it. A call event without a session id is
    malformed and raises ``ValueError`` (answer 400).
    """

    event_type = str(payload.get("type") or "")
    if event_type not in (INCOMING_CALL_EVENT, LEGACY_INCOMING_CALL_EVENT):
        return None
    data = payload.get("data")
    if not isinstance(data, Mapping):
        raise ValueError(f"{event_type} without a data object")
    transport_type = data.get("type")
    if event_type == INCOMING_CALL_EVENT and transport_type not in (None, "sip"):
        return None
    session_id = data.get("session_id") or data.get("call_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError(f"{event_type} without data.session_id")
    headers: list[Tuple[str, str]] = []
    for item in data.get("sip_headers") or ():
        if isinstance(item, Mapping):
            name, value = item.get("name"), item.get("value")
            if isinstance(name, str) and isinstance(value, str):
                headers.append((name, value))
    created_at = payload.get("created_at")
    return IncomingCall(
        webhook_id=webhook_id,
        event_id=payload.get("id") if isinstance(payload.get("id"), str) else None,
        event_type=event_type,
        session_id=session_id,
        sip_headers=tuple(headers),
        created_at=created_at if isinstance(created_at, int) else None,
    )


def sip_uri_user(value: Optional[str]) -> Optional[str]:
    """``"Name" <sip:+4960521829990@host;tag=x>`` → ``+4960521829990``."""

    if not value:
        return None
    text = value
    if "<" in text and ">" in text:
        text = text[text.index("<") + 1 : text.index(">")]
    for scheme in ("sips:", "sip:", "tel:"):
        if text.lower().startswith(scheme):
            text = text[len(scheme) :]
            break
    user = text.split("@", 1)[0].split(";", 1)[0].strip()
    return user or None
