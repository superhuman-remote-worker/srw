"""The job the harness runs, read from SRW's ``POST /job/start`` body.

The orchestrator sends the frozen config blob as ``resolved_config`` (model under
``agent.llm``, workspace under ``agent.workspace.remote``); ``config_override`` is
``None`` on the wire whenever the blob is sent and is read only as the fallback.
Standard library only.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from .orchestrator import PinnedEcho
from .workspace import WorkspaceAccess

EFFORT_LEVELS = ("low", "medium", "high", "xhigh", "max")
ANTHROPIC_PROTOCOL = "anthropic-messages"


class JobRejected(ValueError):
    """The start request cannot be run by this harness."""


@dataclass(frozen=True)
class ModelRoute:
    model: str
    provider: str | None
    client_protocol: str | None
    base_url: str | None
    api_key: str | None = field(repr=False)


@dataclass(frozen=True)
class Limits:
    max_turns: int = 200
    max_budget_usd: float = 20.0
    timeout_seconds: int = 3600


@dataclass(frozen=True)
class JobSpec:
    job_id: str
    prompt: str
    instructions: str | None
    model: ModelRoute
    workspace: WorkspaceAccess
    pinned: PinnedEcho | None
    recipient: dict | None = field(repr=False)
    limits: Limits
    effort: str | None


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None


def parse_job_start(
    body: Mapping[str, Any], *, key_path_override: str | None = None
) -> JobSpec:
    resolved = _mapping(body.get("resolved_config"))
    agent = _mapping(resolved.get("agent"))
    override = _mapping(body.get("config_override"))
    llm = _mapping(agent.get("llm")) or _mapping(override.get("llm"))
    remote = _mapping(_mapping(agent.get("workspace")).get("remote")) or _mapping(
        _mapping(override.get("workspace")).get("remote")
    )

    host, root = _text(remote.get("host")), _text(remote.get("workspace_path"))
    user = _text(remote.get("username"))
    key_path = key_path_override or _text(remote.get("key_path"))
    port = remote.get("port", 22)
    if not (host and root and user and key_path):
        raise JobRejected("the job carries no workspace")
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise JobRejected("the job's workspace has an invalid port")
    model = _text(llm.get("model"))
    if not model:
        raise JobRejected("the job carries no model")

    effort = llm.get("reasoning_level")
    pin_fields = (
        body.get("pinned_delivery_id"),
        body.get("pinned_projection_digest"),
        body.get("pinned_delivery_proof"),
    )
    recipient = body.get("recipient")
    return JobSpec(
        job_id=str(body.get("job_id") or ""),
        prompt=str(body.get("description") or ""),
        instructions=_text(body.get("instructions")),
        model=ModelRoute(
            model=model,
            provider=_text(llm.get("provider")),
            client_protocol=_text(llm.get("client_protocol")),
            base_url=_text(llm.get("base_url")),
            api_key=_text(llm.get("api_key")),
        ),
        workspace=WorkspaceAccess(
            host=host,
            port=port,
            user=user,
            root=root,
            private_key_file=key_path,
            host_key_fingerprint=_text(body.get("workspace_ssh_host_key_fingerprint")),
        ),
        pinned=PinnedEcho(*pin_fields) if any(pin_fields) else None,
        recipient=dict(recipient) if isinstance(recipient, Mapping) else None,
        limits=Limits(),
        effort=effort if effort in EFFORT_LEVELS else None,
    )


def require_anthropic_route(route: ModelRoute) -> None:
    if route.client_protocol != ANTHROPIC_PROTOCOL:
        raise JobRejected(
            f"this harness speaks {ANTHROPIC_PROTOCOL}; the job's model is "
            f"{route.provider}/{route.model}, {route.client_protocol}"
        )
    if not route.api_key and not route.base_url:
        raise JobRejected("the job's model has no credential")
