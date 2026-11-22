"""Authoritative capability and Canvas actions for the shared browser."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from orchestrator.services import resolve_ssh_key_path
from orchestrator.services.browser_stream_broker import (
    BrowserStreamUnavailable,
    exec_stream_info,
)
from orchestrator.services.browser_stream_config import (
    BrowserStreamConfigurationError,
    browser_stream_config,
)
from orchestrator.services.canvas import (
    BrowserSource,
    CanvasMutation,
    CanvasPreconditionFailed,
    CanvasService,
    CanvasSetInput,
)
from orchestrator.services.canvas_ssh import (
    CanvasSSHError,
    GenerationResolver,
    RemoteWorkspaceTarget,
    asyncssh,
    attest_vm_workspace_target,
    bound_workspace_generation,
    canvas_workspace_generation,
    require_same_remote_workspace,
    resolve_canvas_workspace_target,
    resolve_remote_workspace_target,
    vm_canvas_identity,
    vm_canvas_route_available,
)
from orchestrator.services.shared_browser_vm import (
    request_vm_browser_wake,
    thread_uses_vm_workspace,
    vm_browser_wake_state,
    vm_ready_but_coming_up,
)
from orchestrator.services.ssh_helpers import orchestrator_can_reach
from orchestrator.services.workspace_binding import remote_canvas_presentation_available
from shared.backend_kinds import LITE_BACKENDS
from shared.workspace_contract import vm_mode_from_env

BrowserCapabilityReason = Literal[
    "feature_disabled",
    "workspace_required",
    "workspace_unattested",
    "workspace_unroutable",
    "transport_unavailable",
]


class BrowserCapabilityResponse(BaseModel):
    """Closed owner-facing pre-source capability without private identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    feature_enabled: bool
    can_open_browser: bool
    workspace_ready: bool
    reason: BrowserCapabilityReason | None


class BrowserCanvasError(Exception):
    """Typed shared-browser action failure safe for an HTTP boundary."""

    def __init__(self, status_code: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


@dataclass(frozen=True, slots=True)
class PreparedBrowser:
    """Private browser identity paired to the workspace target that minted it."""

    browser_generation: UUID
    workspace_target: RemoteWorkspaceTarget


def _mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return value if isinstance(value, dict) else {}


def _thread_backend(thread: dict[str, Any]) -> str:
    """Resolve current metadata plus the legacy direct fixture field."""

    metadata = _mapping(thread.get("metadata"))
    direct = metadata.get("workspace_backend")
    if isinstance(direct, str) and direct:
        return direct
    config_override = _mapping(metadata.get("config_override"))
    workspace = _mapping(config_override.get("workspace"))
    backend = workspace.get("backend")
    return backend if isinstance(backend, str) and backend else "sandbox"


def _active_workspace_context(metadata: dict[str, Any]) -> dict[str, Any]:
    vm = _mapping(metadata.get("vm"))
    if vm.get("status") == "ready":
        return vm
    return _mapping(metadata.get("workspace_container"))


def _capability(
    *,
    feature_enabled: bool,
    can_open_browser: bool,
    workspace_ready: bool,
    reason: BrowserCapabilityReason | None,
) -> BrowserCapabilityResponse:
    return BrowserCapabilityResponse(
        feature_enabled=feature_enabled,
        can_open_browser=can_open_browser,
        workspace_ready=workspace_ready,
        reason=reason,
    )


def _vm_browser_capability(
    thread: dict[str, Any], transport_available: bool
) -> BrowserCapabilityResponse:
    """Capability of a VM Session, from the durable row only (no network).

    The order is the answer a user should see first: a VM the orchestrator
    cannot route to (the tailnet, ``VM_MODE=external``) is ``unroutable``
    before anything else, so it never waits on an SSH timeout. On the same
    cluster a VM that is coming up, or asleep on its disk under idle release,
    is openable but not ready, like a cold container: open waits for it (and
    wakes a sleeping one, see shared_browser_vm). Any other VM that is not
    ready, or whose identity the controller has not authenticated, is
    ``unattested``. The positive answer still needs a fresh controller proof at
    every operation; this gate only decides what to offer.
    """

    vm = _mapping(_mapping(thread.get("metadata")).get("vm"))
    mode = vm_mode_from_env()
    # Reported ready but not usable yet (not yet probed by the provisioner, or
    # woken but not yet marked ready): still coming up, so open waits.
    ready = vm.get("status") == "ready" and not vm_ready_but_coming_up(vm)
    if not ready:
        if mode == "external":
            # DEFERRED, not forgotten: tailnet VM routing for the Canvas and
            # the shared browser (see canvas_ssh.vm_canvas_route_available).
            return _capability(
                feature_enabled=True,
                can_open_browser=False,
                workspace_ready=False,
                reason="workspace_unroutable",
            )
        if mode == "same-cluster" and vm_browser_wake_state(thread) is not None:
            if not transport_available:
                return _capability(
                    feature_enabled=True,
                    can_open_browser=False,
                    workspace_ready=False,
                    reason="transport_unavailable",
                )
            return _capability(
                feature_enabled=True,
                can_open_browser=True,
                workspace_ready=False,
                reason=None,
            )
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=False,
            reason="workspace_unattested",
        )
    host = vm.get("ssh_host")
    if not isinstance(host, str) or not vm_canvas_route_available(
        host, can_reach=orchestrator_can_reach
    ):
        # A ready VM the orchestrator cannot reach: the tailnet. Routing it is
        # DEFERRED, not forgotten (canvas_ssh.vm_canvas_route_available).
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unroutable",
        )
    if vm_canvas_identity(vm) is None:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unattested",
        )
    try:
        resolve_canvas_workspace_target(thread, canvas_workspace_generation(thread))
    except CanvasSSHError:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unattested",
        )
    if not transport_available:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="transport_unavailable",
        )
    return _capability(
        feature_enabled=True,
        can_open_browser=True,
        workspace_ready=True,
        reason=None,
    )


async def wake_vm_workspace_for_browser(
    db: Any, thread: dict[str, Any], *, user_id: str
) -> None:
    """Open browser on a VM that is not ready: join or start its wake.

    The VM mirror of the container's cold open, which kicks provisioning and
    answers 202. A VM that nothing will bring up answers
    ``workspace_unattested`` instead of a wait that cannot end.
    """

    if not await request_vm_browser_wake(db, thread, user_id=user_id):
        raise BrowserCanvasError(
            409,
            "workspace_unattested",
            "The VM workspace cannot be started for the shared browser",
        )


async def _require_current_vm_target(
    target: RemoteWorkspaceTarget, *, changed_code: str, changed_message: str
) -> None:
    """Fresh controller proof for a VM target before a durable Canvas write."""

    if target.vm_runtime is None:
        return
    verdict = await attest_vm_workspace_target(target)
    if verdict == "current":
        return
    if verdict == "changed":
        raise BrowserCanvasError(409, changed_code, changed_message)
    raise BrowserCanvasError(
        503,
        "transport_unavailable",
        "The VM workspace runtime could not be verified",
    )


def browser_capability(thread: dict[str, Any]) -> BrowserCapabilityResponse:
    """Evaluate the exact owner-facing ability to create or recover a browser."""

    enabled = os.getenv("CANVAS_SHARED_BROWSER_ENABLED", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    if not enabled:
        return _capability(
            feature_enabled=False,
            can_open_browser=False,
            workspace_ready=False,
            reason="feature_disabled",
        )
    try:
        browser_stream_config()
        transport_configured = True
    except BrowserStreamConfigurationError:
        transport_configured = False
    transport_available = (
        transport_configured and asyncssh is not None and bool(resolve_ssh_key_path())
    )

    backend = _thread_backend(thread)
    # A ready VM is the workspace the Canvas resolver selects whatever the
    # declared backend says (an upgraded Session keeps declaring its old
    # tier), so it is checked before the lite refusal; so is a VM asleep on
    # its disk, which open wakes.
    if thread_uses_vm_workspace(thread):
        return _vm_browser_capability(thread, transport_available)
    if backend in LITE_BACKENDS:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=False,
            reason="workspace_required",
        )

    metadata = _mapping(thread.get("metadata"))
    context = _active_workspace_context(metadata)
    ready = context.get("status") == "ready"
    if backend not in {"sandbox", "container", "vm", "remote"}:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=ready,
            reason="workspace_unattested",
        )
    if not ready:
        if backend in {"sandbox", "container"}:
            if not transport_available:
                return _capability(
                    feature_enabled=True,
                    can_open_browser=False,
                    workspace_ready=False,
                    reason="transport_unavailable",
                )
            return _capability(
                feature_enabled=True,
                can_open_browser=True,
                workspace_ready=False,
                reason=None,
            )
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=False,
            reason="workspace_unattested",
        )

    if not remote_canvas_presentation_available(metadata, context):
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unattested",
        )
    try:
        target = resolve_remote_workspace_target(
            thread,
            bound_workspace_generation(thread),
        )
    except CanvasSSHError:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unattested",
        )
    if not orchestrator_can_reach(target.host):
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="workspace_unroutable",
        )
    if not transport_available:
        return _capability(
            feature_enabled=True,
            can_open_browser=False,
            workspace_ready=True,
            reason="transport_unavailable",
        )
    return _capability(
        feature_enabled=True,
        can_open_browser=True,
        workspace_ready=True,
        reason=None,
    )


def require_browser_capability(
    capability: BrowserCapabilityResponse,
    *,
    require_ready: bool,
) -> None:
    if capability.can_open_browser and (
        capability.workspace_ready or not require_ready
    ):
        return
    reason = capability.reason
    if reason == "feature_disabled":
        raise BrowserCanvasError(404, reason, "Shared browser is not enabled")
    if reason == "workspace_required":
        raise BrowserCanvasError(
            409,
            reason,
            "The shared browser requires a full workspace",
        )
    if reason == "workspace_unattested":
        raise BrowserCanvasError(
            409,
            reason,
            "The workspace is not attested for shared-browser access",
        )
    if reason == "workspace_unroutable":
        raise BrowserCanvasError(
            503,
            reason,
            "The workspace is not reachable from the browser broker",
        )
    if reason == "transport_unavailable":
        raise BrowserCanvasError(
            503,
            reason,
            "The shared-browser transport is unavailable",
        )
    raise BrowserCanvasError(
        503,
        "workspace_unavailable",
        "The workspace is still being prepared",
    )


async def prepare_browser_canvas(
    thread: dict[str, Any],
    *,
    initial_baton: Literal["agent", "user"],
    generation_resolver: GenerationResolver,
) -> PreparedBrowser:
    """Start/reuse the current browser without committing public Canvas state."""

    capability = browser_capability(thread)
    require_browser_capability(capability, require_ready=True)
    if initial_baton not in {"agent", "user"}:
        raise ValueError("initial_baton must be 'agent' or 'user'")
    try:
        # One resolver for both kinds: the bound container, or the ready VM by
        # its authenticated identity. exec_stream_info attests a VM itself.
        target = resolve_canvas_workspace_target(
            thread,
            canvas_workspace_generation(thread),
        )
    except CanvasSSHError as exc:
        raise BrowserCanvasError(
            409,
            "workspace_generation_changed",
            "The workspace changed while the browser was starting",
        ) from exc
    try:
        info = await exec_stream_info(
            thread,
            initial_baton=initial_baton,
            generation_resolver=generation_resolver,
        )
    except BrowserStreamUnavailable as exc:
        raise BrowserCanvasError(
            exc.status,
            "transport_unavailable",
            exc.detail,
        ) from exc
    raw_generation = info.get("generation")
    try:
        generation = UUID(raw_generation)
    except (AttributeError, TypeError, ValueError) as exc:
        raise BrowserCanvasError(
            502,
            "browser_identity_invalid",
            "browser-exec returned an invalid browser identity",
        ) from exc
    if raw_generation != str(generation):
        raise BrowserCanvasError(
            502,
            "browser_identity_invalid",
            "browser-exec returned an invalid browser identity",
        )
    return PreparedBrowser(
        browser_generation=generation,
        workspace_target=target,
    )


async def commit_browser_canvas(
    db: Any,
    thread_id: str,
    prepared: PreparedBrowser,
    *,
    title: str,
    expected_presentation_revision: int | None = None,
) -> CanvasMutation:
    """Revalidate the selected target and idempotently stage its browser."""

    current = await db.get_thread(thread_id)
    if current is None:
        raise BrowserCanvasError(404, "thread_unavailable", "Thread is unavailable")
    capability = browser_capability(dict(current))
    require_browser_capability(capability, require_ready=True)
    try:
        require_same_remote_workspace(dict(current), prepared.workspace_target)
    except CanvasSSHError as exc:
        raise BrowserCanvasError(
            409,
            "workspace_generation_changed",
            "The workspace changed while the browser was starting",
        ) from exc
    # The row check above cannot see a VM replaced behind an unchanged row;
    # the controller can. Containers keep their row-only check.
    await _require_current_vm_target(
        prepared.workspace_target,
        changed_code="workspace_generation_changed",
        changed_message="The workspace changed while the browser was starting",
    )

    normalized_title = title.strip() or "Shared browser"
    try:
        return await CanvasService(db).set_if_changed(
            thread_id,
            CanvasSetInput(
                source=BrowserSource(
                    browser_generation=prepared.browser_generation,
                ),
                title=normalized_title,
                renderer="auto",
                editable=False,
                alt_text=None,
                source_version=None,
                new_app=False,
            ),
            expected_presentation_revision=expected_presentation_revision,
        )
    except CanvasPreconditionFailed as exc:
        raise BrowserCanvasError(
            409,
            "canvas_presentation_changed",
            "The Canvas presentation changed while the browser was starting",
        ) from exc


__all__ = [
    "BrowserCanvasError",
    "BrowserCapabilityReason",
    "BrowserCapabilityResponse",
    "PreparedBrowser",
    "_thread_backend",
    "browser_capability",
    "commit_browser_canvas",
    "prepare_browser_canvas",
    "require_browser_capability",
    "wake_vm_workspace_for_browser",
]
