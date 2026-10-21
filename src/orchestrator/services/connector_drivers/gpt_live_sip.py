"""``srw.gpt-live-sip/v1``: a phone line (live_session_jobs.md §5.1).

The control-plane half of the phone-line driver. Its connector names a
``live_voice`` catalog model and the line's limits; the server derives the
engine's host and port from the model's endpoint, which become the pod's
only egress. Its optional ``webhook_secret`` verifies the provider's
incoming-session webhook (SRW's own webhook path). Its standing service pod
runs the ``image_reference`` the chart names (SRW's ``srw-driver-gpt-live-sip``)
and gets the engine key from the lease exchange with its own identity; an
agent's lease only authenticates the job's voice subagent to the pod, so no
upstream credential is ever exchanged for it.

Installed only when ``connectors.drivers.gptLiveSip.enabled`` names an image.
"""

from __future__ import annotations

import base64
import binascii
import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlsplit

from fastapi import HTTPException

from orchestrator.services.connector_drivers.base import (
    BindContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    ValidationContext,
    payload_entry,
)
from shared.connectors.builtin import GPT_LIVE_SIP_SPEC

_WEBHOOK_SECRET = re.compile(r"\Awhsec_[A-Za-z0-9+/=]{16,512}\Z")
_TRANSFER = re.compile(r"\A(sip|tel):[^\s]{1,500}\Z")
_DERIVED = ("api_host", "api_port")
_CONFIG_KEYS = {
    "model",
    "max_duration_s",
    "no_job_fallback_s",
    "transfer_target",
    "greeting_delay_ms",
    "greeting_retry_ms",
}
#: Bounds the pod's own config parser enforces (drivers/gpt-live-sip/config.go).
_GREETING_BOUNDS = {"greeting_delay_ms": 10000, "greeting_retry_ms": 30000}


def _webhook_secret_ok(secret: Any) -> bool:
    """A ``whsec_`` secret the webhook check can use: its key must decode as
    base64 (``shared.runtime.live.webhook``), or every delivery is a 401."""
    if not isinstance(secret, str) or not _WEBHOOK_SECRET.fullmatch(secret):
        return False
    try:
        return bool(base64.b64decode(secret[len("whsec_") :], validate=True))
    except (binascii.Error, ValueError):
        return False


def _int_in(value: Any, low: int, high: int) -> bool:
    return (
        isinstance(value, int) and not isinstance(value, bool) and low <= value <= high
    )


class GptLiveSipDriver(DatasourceDriver):
    def __init__(self, image_reference: str) -> None:
        super().__init__(GPT_LIVE_SIP_SPEC)
        #: The image every pod of this driver runs (tag follows, digest pins).
        self.image_reference = image_reference

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if draft.connection_url:
            raise HTTPException(400, "A phone line connector has no URL")
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            secret = credentials.get("webhook_secret")
            if set(credentials) - {"webhook_secret"} or (
                secret is not None and not _webhook_secret_ok(secret)
            ):
                raise HTTPException(
                    400,
                    "A phone line's credentials are {webhook_secret}: the "
                    "provider's webhook signing secret (whsec_…)",
                )
        config = draft.config
        if config is None and existing is None:
            raise HTTPException(400, "A phone line needs a live voice model")
        if config is not None:
            config = {k: v for k, v in dict(config).items() if k not in _DERIVED}
            unknown = set(config) - _CONFIG_KEYS
            if unknown:
                raise HTTPException(
                    400, f"Unknown phone line settings: {', '.join(sorted(unknown))}"
                )
            if "max_duration_s" in config and not _int_in(
                config["max_duration_s"], 60, 14400
            ):
                raise HTTPException(400, "max_duration_s is 60 to 14400 seconds")
            if "no_job_fallback_s" in config and not _int_in(
                config["no_job_fallback_s"], 1, 120
            ):
                raise HTTPException(400, "no_job_fallback_s is 1 to 120 seconds")
            for key, high in _GREETING_BOUNDS.items():
                if key in config and not _int_in(config[key], 0, high):
                    raise HTTPException(400, f"{key} is 0 to {high} milliseconds")
            target = config.get("transfer_target")
            if target is not None and (
                not isinstance(target, str) or not _TRANSFER.fullmatch(target)
            ):
                raise HTTPException(400, "transfer_target is a sip: or tel: URI")
            model = config.get("model")
            if not isinstance(model, str) or not model.strip():
                raise HTTPException(400, "A phone line needs a live voice model")
            config["model"] = model.strip()
            config.update(await self._engine_host(config["model"], ctx))
        return NormalizedConnector(
            connection_url=None, config=config, credentials=credentials
        )

    async def _engine_host(self, model: str, ctx: ValidationContext) -> dict[str, Any]:
        """The pod's egress: the host and port of the model's endpoint."""
        from orchestrator.services.live.exchange import live_engine

        store = getattr(ctx, "store", None)
        if store is None:
            raise HTTPException(503, "The model catalog is unavailable")
        engine = await live_engine(store, model)
        if engine is None:
            raise HTTPException(
                400,
                f"{model!r} is no usable live_voice model in the catalog "
                "(Admin → Models: a live_voice row on an endpoint with a key)",
            )
        parts = urlsplit(engine["api_base"])
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise HTTPException(400, "The live voice model's endpoint has no host")
        port = parts.port or (443 if parts.scheme == "https" else 80)
        return {"api_host": parts.hostname, "api_port": port}

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # Never the webhook secret: the lease service fills ``credentials``
        # with the job's lease token for the line's pod.
        return payload_entry(
            row,
            credentials={},
            read_only=False,
            connection_url=None,
            fields={"datasource_id": str(row["id"])},
        )

    def lease_upstream(self, row: Mapping[str, Any]) -> dict[str, Any]:
        # An agent's lease authenticates it to the pod; the pod gets the
        # engine key with its own identity (/v1/live/credential).
        raise ValueError("A phone line's lease exchanges no upstream credential")
