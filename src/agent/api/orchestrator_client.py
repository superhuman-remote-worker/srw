"""HTTP client for orchestrator communication.

Handles agent registration, heartbeats, and job management with the orchestrator.
"""

import asyncio
import logging
import os
import socket
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional
from uuid import UUID

import httpx
from pydantic import BaseModel

from shared.runtime.core.product_capabilities import ProductComponent
from shared.runtime.core.runtime_provenance import component_provenance_from_environment
from shared.runtime_actor import (
    RUNTIME_ACTOR_BOOTSTRAP_HEADER,
    RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER,
    RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN,
    RUNTIME_ACTOR_REFRESH_HEADER,
    RuntimeActorContext,
)
from shared.workspace_recovery import (
    WorkspaceRecoveryCode,
    WorkspaceRecoveryDisposition,
)
from shared.subagent_parent_authority import (
    ParentExecutionAuthority,
    ParentExecutionAuthorityRefused,
    coerce_parent_execution_authority,
)
from shared.session_subagent_authority import (
    SessionParentAuthority,
    SessionParentAuthorityRefused,
    coerce_session_parent_authority,
    session_subagent_delivery_id,
)

logger = logging.getLogger(__name__)

_COMPLETION_REPORT_PAYLOAD_FIELDS = (
    "should_stop",
    "goal_achieved",
    "error",
    "freeze_data",
)

_AGENT_LOCAL_QUIESCENCE_PROTOCOLS = frozenset(
    {"workspace_process_zero_v1", "agent_runtime_zero_v1"}
)
_ATTACH_RELEASE_QUIESCENCE_PROTOCOLS = frozenset(
    {*_AGENT_LOCAL_QUIESCENCE_PROTOCOLS, "agent_attach_not_started_v1"}
)


class DuplicateThreadBinding(RuntimeError):
    """Raised when a thread-bound registration loses the provisioning race.

    The orchestrator returns HTTP 409 ("thread already bound to another live
    agent") when a second agent pod tries to register for a thread that a
    different live agent already owns. The losing pod must exit cleanly rather
    than linger: it still carries the ``srw.io/thread-id`` label, so it stays in
    the per-session Service's endpoints (which sets ``publishNotReadyAddresses``)
    and black-holes ~half the cockpit's connection attempts until reaped. See
    knowledge-history/done/persistent_thread_double_provisioning_race.md.
    """


class SessionGrantDenied(Exception):
    """The session's resolved config exceeds the runner's capability grants —
    the agent's workspace endpoint (``GET /api/agents/threads/{id}/workspace``)
    returned HTTP 403. Permanent: a rebind hits the identical denial, so the
    attach path must surface the real reason and stop, NOT misreport it as a
    transient 'workspace not provisioned' (the 5m40s ready-timeout bug). Not a
    RuntimeError so the pool-mode ``except RuntimeError`` attach handler doesn't
    swallow it. See knowledge-base/knowledge/issues/session_permission_mode_grant_denied_ready_timeout.md.
    """


class SessionEnded(Exception):
    """Typed terminal response from the workspace credential boundary."""


class ThreadConfigUpdateDenied(Exception):
    """The orchestrator rejected a live thread config update (4xx).

    Carries the response ``detail`` (e.g. the capability-grant denial reason
    from ``_enforce_session_create_grants``) so the session can surface the
    actual cause to the user instead of a generic "update rejected". Network
    failures and 5xx keep returning ``None`` — those are transient and the
    caller's fallback semantics apply (live_session_settings.md P0.3).
    """

    def __init__(self, status_code: int, detail: str) -> None:
        self.status_code = status_code
        self.detail = detail
        super().__init__(f"Thread config update denied ({status_code}): {detail}")


class ClaimBundleError(Exception):
    """Non-200 from the claim-bundle endpoint (M3 pinned contract).

    Carries the status code so the stateless turn executor can branch:
    401/403 → treat as lease lost (its token-guarded release no-ops), 404 →
    the unit vanished (drop the claim), 409 → not leased / wrong lane (drop
    and re-poll). Network errors are NOT wrapped — they propagate as httpx
    exceptions and the executor releases with backoff.
    """

    def __init__(
        self,
        status_code: int,
        detail: str = "",
        *,
        code: WorkspaceRecoveryCode | None = None,
        recovery: WorkspaceRecoveryDisposition | None = None,
    ) -> None:
        self.status_code = status_code
        self.detail = detail
        self.code = code
        self.recovery = recovery
        super().__init__(f"claim-bundle {status_code}: {detail[:200]}")


def _workspace_recovery_receipt(
    response: httpx.Response, lease_token: int
) -> WorkspaceRecoveryDisposition | None:
    """Recognize only versioned, exact-attempt receipts; unknown 409s stay generic."""
    try:
        body = response.json()
        recovery = body["recovery"]
        if (
            type(recovery["version"]) is not int
            or recovery["version"] != 1
            or recovery["action"]
            not in {"hold_committed", "paused_attention", "recovered"}
            or type(recovery["accepted_lease_token"]) is not int
            or recovery["accepted_lease_token"] != lease_token
            or type(recovery["hold_lease_token"]) is not int
        ):
            return None
        return WorkspaceRecoveryDisposition(
            code=WorkspaceRecoveryCode(body["code"]),
            action=recovery["action"],
            operation_id=UUID(recovery["operation_id"]),
            accepted_lease_token=recovery["accepted_lease_token"],
            hold_lease_token=recovery["hold_lease_token"],
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        return None


class CompletionNonTerminalReportError(Exception):
    """The orchestrator definitively refused a continue-shaped completion.

    This exact machine-coded HTTP 422 is a pre-write refusal, not an ambiguous
    transport failure.  Stateless workers must release through their ordinary
    retry/parking path without probing for durable completion acceptance.
    """

    code = "completion_non_terminal_report"

    def __init__(self, message: str = "") -> None:
        self.message = message
        super().__init__(message or self.code)


class SubagentPersistenceError(RuntimeError):
    """A strict child receipt/recovery request could not prove its result."""

    def __init__(self, operation: str, status_code: int | None = None) -> None:
        self.operation = operation
        self.status_code = status_code
        suffix = f" (HTTP {status_code})" if status_code is not None else ""
        super().__init__(f"subagent {operation} failed{suffix}")


def _raise_subagent_authority_refusal(response: Any) -> None:
    """Raise the typed stale-parent signal for the internal authority 409."""

    if getattr(response, "status_code", None) != 409:
        return
    try:
        detail = response.json().get("detail")
    except Exception:
        detail = None
    if isinstance(detail, dict) and detail.get("code") == (
        ParentExecutionAuthorityRefused.code
    ):
        raise ParentExecutionAuthorityRefused(
            str(detail.get("reason") or "remote_refusal")
        )


def _raise_session_subagent_authority_refusal(response: Any) -> None:
    """Raise the typed stale-session signal for the internal authority 409."""

    if getattr(response, "status_code", None) != 409:
        return
    try:
        detail = response.json().get("detail")
    except Exception:
        detail = None
    if isinstance(detail, dict) and detail.get("code") == (
        SessionParentAuthorityRefused.code
    ):
        raise SessionParentAuthorityRefused(
            str(detail.get("reason") or "remote_refusal")
        )


def _session_subagent_authority_for_thread(
    value: Any, parent_thread_id: str
) -> SessionParentAuthority:
    """Coerce malformed local identity into the same typed refusal as HTTP."""

    try:
        parsed = coerce_session_parent_authority(value)
    except SessionParentAuthorityRefused:
        raise
    except Exception as exc:
        raise SessionParentAuthorityRefused("invalid") from exc
    if not parsed.for_thread(parent_thread_id):
        raise SessionParentAuthorityRefused("parent_mismatch")
    return parsed


def _normalize_session_subagent_roster_row(
    value: Any,
    *,
    parent_thread_id: str,
    expected_thread_id: str | None = None,
) -> dict[str, Any]:
    """Validate the non-job identity coordinates of an internal roster row."""

    if not isinstance(value, dict):
        raise ValueError("session subagent row is not an object")
    parent = UUID(str(parent_thread_id))
    returned_parent = UUID(str(value.get("parent_thread_id")))
    child = UUID(str(value.get("thread_id") or value.get("id")))
    generation = UUID(str(value.get("runtime_generation")))
    if returned_parent != parent or value.get("parent_job_id") not in (None, ""):
        raise ValueError("session subagent row returned a different parent")
    if expected_thread_id is not None and child != UUID(str(expected_thread_id)):
        raise ValueError("session subagent row returned a different thread")
    return {
        **dict(value),
        "thread_id": str(child),
        "parent_thread_id": str(parent),
        "parent_job_id": None,
        "runtime_generation": str(generation),
    }


class VerdictRecordingError(Exception):
    """The verdict could not be durably recorded.

    Deliberately loud: a verdict that is not persisted must never be reported
    to the model as recorded, because every downstream loss path treats a
    missing verdict as approval. Raised by ``record_verification_round`` on
    any failure — network error, non-200 response — instead of the house
    best-effort convention (return None/False) used elsewhere in this client.
    """


class CompletionDecisionError(Exception):
    """The job_complete decision could not be durably journaled.

    Sibling of ``VerdictRecordingError`` for the worker's own terminating
    decision (journal-before-observe, knowledge-base/knowledge/issues/
    job_finalization_decisions_held_only_in_process_memory.md): a decision
    that is not persisted must never be reported to the model as accepted,
    because a restart would then convert "I decided" into "no decision was
    made". Raised by ``record_completion_decision`` on any failure instead of
    the house best-effort convention (return None/False).
    """


class CanvasClientError(RuntimeError):
    """A model-safe failure from a delegated Dynamic Canvas request.

    ``httpx.HTTPStatusError`` includes the full request URL in its string form.
    Persistent tool failures are returned to the model, so propagating that
    exception would disclose internal service names, routes, and thread IDs.
    Keep only a fixed public error code/message and the response status.
    """

    def __init__(
        self, code: str, message: str, *, status_code: int | None = None
    ) -> None:
        self.code = code
        self.message = message
        self.status_code = status_code
        status = f", HTTP {status_code}" if status_code is not None else ""
        super().__init__(f"Canvas request failed [{code}{status}]: {message}")


@dataclass(frozen=True, slots=True)
class CanvasSetResult:
    """Internal set response plus its non-model-visible mutation signal."""

    state: dict[str, Any]
    changed: bool


@dataclass(frozen=True, slots=True)
class CanvasClearResult:
    """Internal clear response plus its non-model-visible mutation signal."""

    state: dict[str, Any]
    changed: bool


# Server-side Canvas validation has a deliberately bounded but longer envelope
# than this client's ordinary 30-second control-plane requests: capacity queue
# waits plus pinned SFTP/rclone materialization and image validation can exceed
# 30 seconds. Keep the delegated mutation/read request alive beyond that hard
# path so the tool cannot report a timeout while the handler is still capable
# of committing the presentation. End-to-end idempotency remains the stronger
# future answer for an actual connection loss after commit.
CANVAS_REQUEST_TIMEOUT_SECONDS = 120.0


def _canonical_runtime_uuid(value: Any) -> str | None:
    """Return one canonical UUID string without truthiness coercion."""

    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return str(UUID(value.strip()))
    except (TypeError, ValueError):
        return None


_CANVAS_PUBLIC_ERROR_MESSAGES = {
    # This is deliberately a closed vocabulary. Orchestrator response bodies
    # cross an internal-to-model trust boundary, so neither an arbitrary code
    # nor an arbitrary detail.message may become a tool exception.
    "canvas_alt_text_required": "Raster images require meaningful alt text",
    "canvas_cleared": "Canvas is cleared",
    "canvas_file_not_found": "Canvas file was not found",
    "canvas_file_too_large": "Canvas content is too large",
    "canvas_image_too_large": "Canvas image is too large",
    "canvas_not_file": "Canvas is not file-backed",
    "canvas_office_unavailable": (
        "Office document viewing is not enabled or currently available"
    ),
    "canvas_precondition_failed": "Canvas state changed; inspect it and try again",
    "canvas_precondition_required": "Canvas state must be inspected before changing it",
    "canvas_presentation_changed": "Canvas presentation changed; inspect it and try again",
    "canvas_port_reserved": "Canvas application port is reserved",
    "canvas_regular_file_required": "Canvas sources must be regular files",
    "canvas_replaced": "Canvas source was replaced; inspect it and try again",
    "canvas_symlink_rejected": "Canvas paths may not contain symlinks",
    "invalid_canvas_image": "Canvas image is invalid or unsafe",
    "invalid_canvas_entry_path": "Canvas application entry path is invalid",
    "invalid_canvas_path": "File path is invalid",
    "invalid_canvas_port": "Canvas application port is invalid",
    "mime_renderer_mismatch": "The requested renderer is incompatible with the file",
    "source_changed": "Workspace content changed; publish the Canvas again",
    "unsupported_canvas_file": "File type is not supported by Canvas",
    "workspace_generation_changed": "The workspace changed; publish the Canvas again",
    "workspace_unavailable": "The workspace is unavailable",
}


def _canvas_response_error(response: httpx.Response) -> CanvasClientError:
    """Map an HTTP response to a stable error without retaining its request URL."""

    status_code = response.status_code
    if status_code >= 500:
        return CanvasClientError(
            "canvas_service_unavailable",
            "Canvas service is temporarily unavailable",
            status_code=status_code,
        )

    default_code = {
        400: "invalid_canvas_request",
        401: "canvas_not_authorized",
        403: "canvas_not_authorized",
        404: "canvas_not_found",
        409: "canvas_conflict",
        413: "canvas_file_too_large",
        422: "invalid_canvas_request",
        429: "canvas_rate_limited",
    }.get(status_code, "canvas_request_failed")
    default_message = {
        401: "Canvas authorization failed",
        403: "Canvas authorization failed",
        404: "Canvas resource was not found",
        409: "Canvas state changed; inspect it and try again",
        413: "Canvas content is too large",
        429: "Canvas is temporarily rate limited",
    }.get(status_code, "Canvas request was rejected")

    code = default_code
    message = default_message
    try:
        payload = response.json()
    except Exception:
        payload = None
    detail = payload.get("detail") if isinstance(payload, dict) else None
    if isinstance(detail, dict):
        candidate_code = detail.get("code")
        if isinstance(candidate_code, str) and candidate_code in (
            _CANVAS_PUBLIC_ERROR_MESSAGES
        ):
            code = candidate_code
            message = _CANVAS_PUBLIC_ERROR_MESSAGES[candidate_code]

    return CanvasClientError(code, message, status_code=status_code)


class UploadedFileInfo(BaseModel):
    """Metadata for a single uploaded file."""

    name: str
    size: int
    mime_type: str


class UploadInfo(BaseModel):
    """Information about an upload."""

    upload_id: str
    upload_type: str
    files: list[UploadedFileInfo]
    created_at: str


def get_agent_ip() -> str:
    """Auto-detect agent IP address.

    First checks AGENT_POD_IP environment variable, then falls back to
    socket-based detection.

    Returns:
        IP address as string
    """
    # Check environment variable first
    env_ip = os.getenv("AGENT_POD_IP")
    if env_ip:
        return env_ip

    # Fall back to socket-based detection
    try:
        # Create a socket and connect to an external address to determine local IP
        # This doesn't actually send data, just determines the route
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except Exception:
        # Final fallback to localhost
        return "127.0.0.1"


def get_hostname() -> str:
    """Get hostname for agent identification.

    Returns AGENT_HOSTNAME env var if set, otherwise system hostname.
    """
    return os.getenv("AGENT_HOSTNAME") or socket.gethostname()


class OrchestratorClient:
    """HTTP client for communication with the orchestrator service.

    Handles:
    - Agent registration on startup
    - Periodic heartbeats to report status
    - Graceful deregistration on shutdown
    """

    def __init__(
        self,
        orchestrator_url: str,
        pod_ip: str,
        pod_port: int,
        hostname: str,
        config_name: str,
        pid: Optional[int] = None,
        user_id: Optional[str] = None,
    ):
        """Initialize the orchestrator client.

        Args:
            orchestrator_url: Base URL of orchestrator (e.g., http://localhost:8085)
            pod_ip: IP address where this agent can be reached
            pod_port: Port where this agent's API is running
            hostname: Hostname for identification
            config_name: Agent configuration name (e.g., "creator", "validator")
            pid: Optional process ID
            user_id: Originating user UUID. When set, the client attaches
                ``X-MCP-User-Id`` so the orchestrator's
                ``_get_user_from_mcp_headers`` path can resolve the user
                on routes guarded by ``require_approved_user`` /
                ``require_job_access``. Worker-mode and lifecycle calls
                (register, heartbeat) leave this unset and continue to
                authenticate as anonymous-internal via X-Internal-Key.
        """
        self.orchestrator_url = orchestrator_url.rstrip("/")
        self.pod_ip = pod_ip
        self.pod_port = pod_port
        self.hostname = hostname
        self.config_name = config_name
        self.pid = pid or os.getpid()
        self.user_id = user_id

        self.agent_id: Optional[str] = None
        self.dispatch_process_generation: Optional[str] = None
        self.runtime_actor: RuntimeActorContext | None = None
        # Exact authority for one pinned session life.  This is deliberately
        # distinct from the process/agent id: a pool agent can serve the same
        # thread again after End -> Resume, and the reciprocal pair alone then
        # has an ABA shape.  Workspace credentials, lifecycle writes, attach
        # rollback and protected staging all echo this generation.
        self.session_runtime_generation: str | None = None
        self.session_runtime_attach_token: str | None = None
        self.pinned_runtime_generation_contract = False
        self.heartbeat_interval: int = 60  # Default, may be updated by orchestrator

        self._client: Optional[httpx.AsyncClient] = None
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._stop_heartbeat = asyncio.Event()
        self._runtime_actor_maintenance_lock = asyncio.Lock()
        self._runtime_actor_maintenance_failures = 0
        self._runtime_actor_retry_at = 0.0

    async def connect(self) -> None:
        """Initialize the HTTP client.

        Attaches ``X-Internal-Key`` to every request when ``MCP_INTERNAL_KEY``
        is set in the agent's env. The orchestrator's Track B (P4b) gates
        check this header on agent-internal endpoints (register, heartbeat,
        job-complete, etc.) and on the dual-callable job mutation paths
        (cancel/pause/resume/approve/subjob-merge/messages-send). Without
        the key the agent's calls would be rejected as anonymous external
        traffic.

        When the client was constructed with a ``user_id``, ``X-MCP-User-Id``
        is also attached so the orchestrator can resolve the originating
        user on require_approved_user / require_job_access endpoints.
        """
        if self._client is None:
            headers: dict[str, str] = {}
            internal_key = os.getenv("MCP_INTERNAL_KEY", "")
            if internal_key:
                headers["X-Internal-Key"] = internal_key
            if self.user_id:
                headers["X-MCP-User-Id"] = self.user_id
            self._client = httpx.AsyncClient(timeout=30.0, headers=headers)

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    def adopt_session_runtime_identity(
        self,
        generation: Any,
        attach_token: Any = None,
        *,
        contract_advertised: bool = True,
    ) -> bool:
        """Install one exact pinned-session authority on this client.

        The method validates before mutating so a malformed attach cannot
        erase or partially replace a still-current identity.
        """

        canonical_generation = _canonical_runtime_uuid(generation)
        canonical_token = (
            _canonical_runtime_uuid(attach_token) if attach_token is not None else None
        )
        if canonical_generation is None or (
            attach_token is not None and canonical_token is None
        ):
            return False
        self.session_runtime_generation = canonical_generation
        self.session_runtime_attach_token = canonical_token
        self.pinned_runtime_generation_contract = bool(contract_advertised)
        return True

    def clear_session_runtime_identity(
        self,
        *,
        expected_generation: str | None = None,
        expected_attach_token: str | None = None,
    ) -> bool:
        """Clear only the exact identity captured by a teardown/rollback."""

        if (
            expected_generation is not None
            and self.session_runtime_generation != expected_generation
        ):
            return False
        if (
            expected_attach_token is not None
            and self.session_runtime_attach_token != expected_attach_token
        ):
            return False
        self.session_runtime_generation = None
        self.session_runtime_attach_token = None
        self.pinned_runtime_generation_contract = False
        return True

    async def register(
        self,
        agent_mode: str = "worker",
        thread_id: str | None = None,
    ) -> bool:
        """Register this agent with the orchestrator.

        Args:
            agent_mode: "worker" (default, dispatch pool) or "persistent" (interactive session)
            thread_id: Thread UUID for persistent mode

        Returns:
            True if registration succeeded, False on a transient/other failure.

        Raises:
            DuplicateThreadBinding: on a 409 for a thread-bound registration
                (another live agent already owns the thread).
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/register"
        product_provenance = component_provenance_from_environment(
            os.environ,
            ProductComponent.AGENT,
            include_common=True,
        )
        provisioned_generation_raw = os.environ.get(
            "SESSION_RUNTIME_GENERATION", ""
        ).strip()
        provisioned_generation = (
            _canonical_runtime_uuid(provisioned_generation_raw)
            if provisioned_generation_raw
            else None
        )
        if provisioned_generation_raw and provisioned_generation is None:
            logger.error(
                "Refusing registration: provisioned session runtime generation "
                "is malformed"
            )
            return False

        payload = {
            "config_name": self.config_name,
            "pod_ip": self.pod_ip,
            "pod_port": self.pod_port,
            "hostname": self.hostname,
            "pid": self.pid,
            "agent_mode": agent_mode,
            "thread_id": thread_id,
            "session_runtime_generation": provisioned_generation,
            "build_sha": os.environ.get("BUILD_SHA", ""),
            "product_provenance": product_provenance.model_dump(mode="json"),
            # Injected via Kubernetes downward API by agent_provisioner; empty
            # outside of K8s (local dev). The orchestrator persists this on
            # the agents row so the session router can construct K8s
            # ownerReferences on per-session Service/Ingress resources.
            "pod_uid": os.environ.get("POD_UID", ""),
        }

        try:
            registration_headers: dict[str, str] = {}
            bootstrap = os.environ.get("SRW_RUNTIME_ACTOR_BOOTSTRAP", "").strip()
            if thread_id and bootstrap:
                registration_headers[RUNTIME_ACTOR_BOOTSTRAP_HEADER] = bootstrap
            response = await self._client.post(
                url,
                json=payload,
                headers=registration_headers or None,
            )
        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for registration: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during registration: {e}")
            return False

        if response.status_code == 200:
            data = response.json()
            contract = bool(
                type(data.get("pinned_runtime_generation_contract")) is int
                and data["pinned_runtime_generation_contract"] == 1
            )
            delivered_generation_raw = data.get("session_runtime_generation")
            delivered_generation = (
                _canonical_runtime_uuid(delivered_generation_raw)
                if delivered_generation_raw is not None
                else None
            )
            delivered_attach_token_raw = data.get("session_runtime_attach_token")
            delivered_attach_token = (
                _canonical_runtime_uuid(delivered_attach_token_raw)
                if delivered_attach_token_raw is not None
                else None
            )
            if delivered_generation_raw is not None and delivered_generation is None:
                logger.error(
                    "Refusing registration: orchestrator returned a malformed "
                    "session runtime generation"
                )
                return False
            if (
                delivered_attach_token_raw is not None
                and delivered_attach_token is None
            ):
                logger.error(
                    "Refusing registration: orchestrator returned a malformed "
                    "session runtime attach token"
                )
                return False
            if (
                thread_id is not None
                and contract
                and (delivered_generation is None or delivered_attach_token is None)
            ):
                logger.error(
                    "Refusing thread-bound registration: advertised runtime "
                    "generation contract omitted its exact generation or "
                    "attach token"
                )
                return False
            if (
                provisioned_generation is not None
                and delivered_generation != provisioned_generation
            ):
                logger.error(
                    "Refusing thread-bound registration: runtime generation "
                    "does not match the provisioned authority"
                )
                return False
            self.agent_id = data.get("agent_id")
            self.dispatch_process_generation = data.get("dispatch_process_generation")
            self.pinned_runtime_generation_contract = contract
            self.session_runtime_generation = delivered_generation
            self.session_runtime_attach_token = delivered_attach_token
            self.adopt_runtime_actor(
                RuntimeActorContext.from_payload(data.get("runtime_actor"))
            )
            self.heartbeat_interval = data.get("heartbeat_interval_seconds", 60)
            logger.info(
                f"Registered with orchestrator as agent {self.agent_id}, "
                f"heartbeat interval: {self.heartbeat_interval}s"
            )
            return True

        # A thread-bound registration that loses the provisioning race gets a
        # 409 ("thread already bound to another live agent"). Surface it as a
        # typed signal so the dedicated-mode startup path can exit cleanly
        # instead of lingering as an orphan that pollutes the per-session
        # Service endpoints. Worker/pool/dual registrations carry no thread_id
        # and never receive this 409.
        if response.status_code == 409 and thread_id is not None:
            try:
                detail = response.json().get("detail")
            except Exception:
                detail = None
            if isinstance(detail, dict) and detail.get("code") == "session_ended":
                raise SessionEnded("session ended before agent registration")
            logger.error(
                f"Orchestrator refused thread-bound registration for thread "
                f"{thread_id} (409): {response.text}"
            )
            raise DuplicateThreadBinding(response.text)

        logger.error(
            f"Failed to register with orchestrator: {response.status_code} - {response.text}"
        )
        return False

    async def create_thread(
        self,
        config_name: str = "session_base",
        permission_mode: str = "supervised",
        title: str = "Local Session",
    ) -> str | None:
        """Create a thread in the orchestrator DB (agent-facing, no auth).

        Returns:
            Thread UUID string, or None on failure.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads"
        payload = {
            "config_name": config_name,
            "permission_mode": permission_mode,
            "title": title,
        }

        try:
            response = await self._client.post(url, json=payload)
            if response.status_code == 200:
                data = response.json()
                thread_id = data.get("thread_id")
                logger.info(f"Created thread via orchestrator: {thread_id}")
                return thread_id
            else:
                logger.error(
                    f"Failed to create thread: {response.status_code} - {response.text}"
                )
                return None
        except Exception as e:
            logger.error(f"Failed to create thread: {e}")
            return None

    async def create_subagent_thread(
        self,
        job_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
        handle: str,
        subagent_type: str,
        subagent_id: Optional[str] = None,
        parent_tool_call_id: Optional[str] = None,
        parent_thread_id: Optional[str] = None,
        isolation: str = "shared",
        write_policy: str = "none",
        owned_paths: Optional[list[str]] = None,
        brief_description: str = "",
        parent_iteration: Optional[int] = None,
        fork: bool = False,
        run_in_background: bool = False,
        initial_status: str = "running",
    ) -> Optional[dict[str, str]]:
        """Create the ``threads`` row of a subagent child of ``job_id`` (U3 B.1).

        ``POST /api/agents/jobs/{job_id}/subagents`` — internal (X-Internal-Key),
        never the session creation route: the orchestrator derives the row's
        owner and project from the job and provisions nothing. ``subagent_id``
        becomes the row id when given, so the child's audit rows and its
        thread share one identity. Returns the thread id and the database-owned
        runtime generation, or ``None`` on any failure (the ledger then keeps
        no durable state for that child).
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/jobs/{job_id}/subagents"
        payload: dict[str, Any] = {
            "parent_authority": coerce_parent_execution_authority(
                parent_authority
            ).to_wire(),
            "handle": handle,
            "subagent_type": subagent_type,
            "subagent_id": subagent_id,
            "parent_tool_call_id": parent_tool_call_id,
            "parent_thread_id": parent_thread_id,
            "isolation": isolation,
            "write_policy": write_policy,
            "owned_paths": list(owned_paths or []),
            "brief_description": brief_description,
            "parent_iteration": parent_iteration,
            "fork": bool(fork),
            "run_in_background": bool(run_in_background),
            "initial_status": initial_status,
        }
        try:
            response = await self._client.post(url, json=payload)
            _raise_subagent_authority_refusal(response)
            if response.status_code == 200:
                data = response.json()
                thread_id = data.get("thread_id")
                runtime_generation = data.get("runtime_generation")
                try:
                    returned_thread_id = UUID(str(thread_id))
                    expected_thread_id = (
                        UUID(str(subagent_id)) if subagent_id is not None else None
                    )
                    if (
                        expected_thread_id is not None
                        and returned_thread_id != expected_thread_id
                    ):
                        raise ValueError(
                            "subagent create receipt returned a different thread id"
                        )
                    lease = {
                        "thread_id": str(returned_thread_id),
                        "runtime_generation": str(UUID(str(runtime_generation))),
                    }
                except (ValueError, TypeError, AttributeError):
                    if run_in_background:
                        raise SubagentPersistenceError(
                            "background-create-payload", int(response.status_code)
                        )
                    logger.error(
                        "Subagent create returned no valid generation for job %s",
                        job_id,
                    )
                    return None
                logger.info(
                    "Created subagent thread %s for job %s (%s)",
                    lease["thread_id"],
                    job_id,
                    handle,
                )
                return lease
            if run_in_background:
                raise SubagentPersistenceError(
                    "background-create", int(response.status_code)
                )
            logger.error(
                "Failed to create subagent thread for job %s: %s - %s",
                job_id,
                response.status_code,
                response.text,
            )
            return None
        except (ParentExecutionAuthorityRefused, SubagentPersistenceError):
            if run_in_background:
                raise
            logger.error(
                "Failed to create subagent thread for foreground job %s",
                job_id,
                exc_info=True,
            )
            return None
        except Exception as e:
            if run_in_background:
                raise SubagentPersistenceError("background-create") from e
            logger.error(f"Failed to create subagent thread for job {job_id}: {e}")
            return None

    async def list_live_subagent_threads(
        self,
        job_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> list[dict[str, Any]]:
        """Return generation-bearing queued/running children of a worker job."""
        if not self._client:
            await self.connect()
        url = f"{self.orchestrator_url}/api/agents/jobs/{job_id}/subagents/live"
        try:
            response = await self._client.post(
                url,
                json={
                    "parent_authority": coerce_parent_execution_authority(
                        parent_authority
                    ).to_wire()
                },
            )
            _raise_subagent_authority_refusal(response)
            if response.status_code != 200:
                raise SubagentPersistenceError("live-list", int(response.status_code))
            rows = response.json().get("subagents")
            if not isinstance(rows, list):
                raise SubagentPersistenceError("live-list-payload")
            return [dict(row) for row in rows]
        except (ParentExecutionAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("live-list") from exc

    async def get_subagent_thread(
        self,
        job_id: str,
        thread_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
    ) -> Optional[dict[str, Any]]:
        """Read one worker child under its parent job."""
        if not self._client:
            await self.connect()
        url = f"{self.orchestrator_url}/api/agents/jobs/{job_id}/subagents/{thread_id}"
        try:
            response = await self._client.post(
                url,
                json={
                    "parent_authority": coerce_parent_execution_authority(
                        parent_authority
                    ).to_wire()
                },
            )
            _raise_subagent_authority_refusal(response)
            if response.status_code == 200:
                return dict(response.json())
            if response.status_code == 404:
                return None
            raise SubagentPersistenceError("exact-read", int(response.status_code))
        except (ParentExecutionAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("exact-read") from exc

    async def reopen_subagent_thread(
        self,
        job_id: str,
        thread_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
        runtime_generation: str,
    ) -> Optional[dict[str, Any]]:
        """Rotate an ended child to a queued successor generation."""
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/jobs/{job_id}/subagents/"
            f"{thread_id}/reopen"
        )
        try:
            response = await self._client.post(
                url,
                json={
                    "runtime_generation": runtime_generation,
                    "parent_authority": coerce_parent_execution_authority(
                        parent_authority
                    ).to_wire(),
                },
            )
            data = response.json()
            _raise_subagent_authority_refusal(response)
            if response.status_code == 200:
                return dict(data)
            if response.status_code == 409 and isinstance(data.get("detail"), dict):
                return dict(data["detail"])
            raise SubagentPersistenceError("reopen", int(response.status_code))
        except (ParentExecutionAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("reopen") from exc

    async def terminalize_subagent_thread(
        self,
        job_id: str,
        thread_id: str,
        *,
        parent_authority: ParentExecutionAuthority,
        runtime_generation: str,
        delivery_id: str,
        message: str,
        timestamp: str,
        subagent_status: str,
        outcome: Optional[str] = None,
        turns: Optional[int] = None,
        tokens: Optional[int] = None,
        report_path: Optional[str] = None,
        error: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        """End one exact run and atomically enqueue its worker report."""
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/jobs/{job_id}/subagents/"
            f"{thread_id}/terminal"
        )
        payload = {
            "parent_authority": coerce_parent_execution_authority(
                parent_authority
            ).to_wire(),
            "runtime_generation": runtime_generation,
            "delivery_id": delivery_id,
            "message": message,
            "timestamp": timestamp,
            "subagent_status": subagent_status,
            "outcome": outcome,
            "turns": turns,
            "tokens": tokens,
            "report_path": report_path,
            "error": error,
        }
        try:
            response = await self._client.post(url, json=payload)
            data = response.json()
            _raise_subagent_authority_refusal(response)
            if response.status_code == 200:
                return dict(data)
            if response.status_code == 409 and isinstance(data.get("detail"), dict):
                return dict(data["detail"])
            raise SubagentPersistenceError("terminalize", int(response.status_code))
        except (ParentExecutionAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("terminalize") from exc

    async def create_session_subagent_thread(
        self,
        parent_thread_id: str,
        *,
        parent_authority: SessionParentAuthority,
        handle: str,
        subagent_type: str,
        subagent_id: Optional[str] = None,
        parent_tool_call_id: Optional[str] = None,
        parent_input_message_id: Optional[str] = None,
        parent_ai_message_id: Optional[str] = None,
        isolation: str = "shared",
        write_policy: str = "none",
        owned_paths: Optional[list[str]] = None,
        brief_description: str = "",
        parent_iteration: Optional[int] = None,
        fork: bool = False,
        run_in_background: bool = False,
        initial_status: str = "running",
    ) -> Optional[dict[str, str]]:
        """Create a child derived from one exact parent-session execution."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        if run_in_background and parsed.execution_lane == "stateless":
            raise SessionParentAuthorityRefused("stateless_background_unsupported")
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads/{parent_thread_id}/subagents"
        payload: dict[str, Any] = {
            "parent_authority": parsed.to_wire(),
            "handle": handle,
            "subagent_type": subagent_type,
            "subagent_id": subagent_id,
            "parent_tool_call_id": parent_tool_call_id,
            "parent_input_message_id": parent_input_message_id,
            "parent_ai_message_id": parent_ai_message_id,
            "isolation": isolation,
            "write_policy": write_policy,
            "owned_paths": list(owned_paths or []),
            "brief_description": brief_description,
            "parent_iteration": parent_iteration,
            "fork": bool(fork),
            "run_in_background": bool(run_in_background),
            "initial_status": initial_status,
        }
        try:
            response = await self._client.post(url, json=payload)
            _raise_session_subagent_authority_refusal(response)
            if response.status_code != 200:
                if run_in_background:
                    raise SubagentPersistenceError(
                        "session-background-create", int(response.status_code)
                    )
                logger.error(
                    "Failed to create session subagent for thread %s: HTTP %s",
                    parent_thread_id,
                    response.status_code,
                )
                return None
            data = response.json()
            returned_thread_id = UUID(str(data.get("thread_id")))
            generation = UUID(str(data.get("runtime_generation")))
            expected_thread_id = (
                UUID(str(subagent_id)) if subagent_id is not None else None
            )
            if (
                expected_thread_id is not None
                and returned_thread_id != expected_thread_id
            ):
                raise ValueError(
                    "session subagent create receipt returned a different thread id"
                )
            return {
                "thread_id": str(returned_thread_id),
                "runtime_generation": str(generation),
            }
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            if run_in_background:
                raise SubagentPersistenceError(
                    "session-background-create-payload"
                ) from exc
            logger.error(
                "Failed to create foreground session subagent for thread %s",
                parent_thread_id,
                exc_info=True,
            )
            return None

    async def list_live_session_subagent_threads(
        self,
        parent_thread_id: str,
        *,
        parent_authority: SessionParentAuthority,
    ) -> list[dict[str, Any]]:
        """Return generation-bearing session child recovery candidates."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/threads/"
            f"{parent_thread_id}/subagents/live"
        )
        try:
            response = await self._client.post(
                url, json={"parent_authority": parsed.to_wire()}
            )
            _raise_session_subagent_authority_refusal(response)
            if response.status_code != 200:
                raise SubagentPersistenceError(
                    "session-live-list", int(response.status_code)
                )
            rows = response.json().get("subagents")
            if not isinstance(rows, list) or not all(
                isinstance(row, dict) for row in rows
            ):
                raise SubagentPersistenceError("session-live-list-payload")
            normalized = [
                _normalize_session_subagent_roster_row(
                    row, parent_thread_id=parent_thread_id
                )
                for row in rows
            ]
            if len({row["thread_id"] for row in normalized}) != len(normalized):
                raise SubagentPersistenceError("session-live-list-payload")
            return normalized
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("session-live-list") from exc

    async def get_session_subagent_thread(
        self,
        parent_thread_id: str,
        thread_id: str,
        *,
        parent_authority: SessionParentAuthority,
    ) -> Optional[dict[str, Any]]:
        """Read one exact child under its session parent."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        child = str(UUID(str(thread_id)))
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/threads/{parent_thread_id}/"
            f"subagents/{child}"
        )
        try:
            response = await self._client.post(
                url, json={"parent_authority": parsed.to_wire()}
            )
            _raise_session_subagent_authority_refusal(response)
            if response.status_code == 404:
                return None
            if response.status_code != 200:
                raise SubagentPersistenceError(
                    "session-exact-read", int(response.status_code)
                )
            return _normalize_session_subagent_roster_row(
                response.json(),
                parent_thread_id=parent_thread_id,
                expected_thread_id=child,
            )
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("session-exact-read-payload") from exc

    async def get_session_subagent_thread_by_call(
        self,
        parent_thread_id: str,
        parent_tool_call_id: str,
        *,
        parent_authority: SessionParentAuthority,
    ) -> Optional[dict[str, Any]]:
        """Resolve a session's durable delegation call without job semantics."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        call_id = str(parent_tool_call_id or "").strip()
        if not call_id:
            return None
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/threads/{parent_thread_id}/"
            "subagents/by-call"
        )
        try:
            response = await self._client.post(
                url,
                json={
                    "parent_authority": parsed.to_wire(),
                    "parent_tool_call_id": call_id,
                },
            )
            _raise_session_subagent_authority_refusal(response)
            if response.status_code == 404:
                return None
            if response.status_code != 200:
                raise SubagentPersistenceError(
                    "session-by-call", int(response.status_code)
                )
            return _normalize_session_subagent_roster_row(
                response.json(), parent_thread_id=parent_thread_id
            )
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("session-by-call-payload") from exc

    async def reopen_session_subagent_thread(
        self,
        parent_thread_id: str,
        thread_id: str,
        *,
        parent_authority: SessionParentAuthority,
        runtime_generation: str,
    ) -> Optional[dict[str, Any]]:
        """Rotate one ended session child to a queued successor generation."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        child = str(UUID(str(thread_id)))
        generation = str(UUID(str(runtime_generation)))
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/threads/{parent_thread_id}/"
            f"subagents/{child}/reopen"
        )
        try:
            response = await self._client.post(
                url,
                json={
                    "parent_authority": parsed.to_wire(),
                    "runtime_generation": generation,
                },
            )
            _raise_session_subagent_authority_refusal(response)
            data = response.json()
            if response.status_code == 409 and isinstance(data.get("detail"), dict):
                return dict(data["detail"])
            if response.status_code != 200:
                raise SubagentPersistenceError(
                    "session-reopen", int(response.status_code)
                )
            if data.get("result") != "reopened":
                raise SubagentPersistenceError("session-reopen-payload")
            if UUID(str(data.get("thread_id"))) != UUID(child):
                raise SubagentPersistenceError("session-reopen-payload")
            successor = str(UUID(str(data.get("runtime_generation"))))
            return {**dict(data), "thread_id": child, "runtime_generation": successor}
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("session-reopen") from exc

    async def terminalize_session_subagent_thread(
        self,
        parent_thread_id: str,
        thread_id: str,
        *,
        parent_authority: SessionParentAuthority,
        runtime_generation: str,
        subagent_status: str,
        run_in_background: bool,
        message: Optional[str] = None,
        outcome: Optional[str] = None,
        turns: Optional[int] = None,
        tokens: Optional[int] = None,
        report_path: Optional[str] = None,
        error: Optional[str] = None,
        foreground_orphan_recovery: bool = False,
    ) -> Optional[dict[str, Any]]:
        """End one child and let the server derive any durable delivery id."""

        parsed = _session_subagent_authority_for_thread(
            parent_authority, parent_thread_id
        )
        if run_in_background and parsed.execution_lane == "stateless":
            raise SessionParentAuthorityRefused("stateless_background_unsupported")
        child = str(UUID(str(thread_id)))
        generation = str(UUID(str(runtime_generation)))
        expected_delivery = str(session_subagent_delivery_id(child, generation))
        if not self._client:
            await self.connect()
        url = (
            f"{self.orchestrator_url}/api/agents/threads/{parent_thread_id}/"
            f"subagents/{child}/terminal"
        )
        payload = {
            "parent_authority": parsed.to_wire(),
            "runtime_generation": generation,
            "subagent_status": subagent_status,
            # The stable id is deliberately not accepted from the driver.  The
            # server derives it from (child, generation) inside the terminal tx.
            "delivery_id": None,
            "message": (
                message if run_in_background or foreground_orphan_recovery else None
            ),
            "outcome": outcome,
            "turns": turns,
            "tokens": tokens,
            "report_path": report_path,
            "error": error,
            "foreground_orphan_recovery": foreground_orphan_recovery,
        }
        try:
            response = await self._client.post(url, json=payload)
            _raise_session_subagent_authority_refusal(response)
            data = response.json()
            if response.status_code == 409 and isinstance(data.get("detail"), dict):
                return dict(data["detail"])
            if response.status_code != 200:
                raise SubagentPersistenceError(
                    "session-terminalize", int(response.status_code)
                )
            if data.get("result") not in {
                "applied",
                "idempotent",
                "already_delivered",
            }:
                raise SubagentPersistenceError("session-terminalize-payload")
            if UUID(str(data.get("thread_id"))) != UUID(child) or UUID(
                str(data.get("runtime_generation"))
            ) != UUID(generation):
                raise SubagentPersistenceError("session-terminalize-payload")
            delivery_id = data.get("delivery_id")
            if data.get("result") == "already_delivered":
                if delivery_id is not None:
                    raise SubagentPersistenceError("session-terminalize-payload")
                return {
                    **dict(data),
                    "thread_id": child,
                    "runtime_generation": generation,
                }
            if run_in_background or foreground_orphan_recovery:
                if str(UUID(str(delivery_id))) != expected_delivery:
                    raise SubagentPersistenceError(
                        "session-terminalize-delivery-payload"
                    )
                if not str(data.get("delivery_state") or "").strip():
                    raise SubagentPersistenceError(
                        "session-terminalize-delivery-payload"
                    )
            elif delivery_id is not None:
                raise SubagentPersistenceError("session-terminalize-payload")
            return {
                **dict(data),
                "thread_id": child,
                "runtime_generation": generation,
            }
        except (SessionParentAuthorityRefused, SubagentPersistenceError):
            raise
        except Exception as exc:
            raise SubagentPersistenceError("session-terminalize") from exc

    async def save_thread_message(
        self,
        thread_id: str,
        role: str,
        content: str | None = None,
        tool_calls: list | None = None,
        turn_number: int | None = None,
        metrics: dict | None = None,
        tool_call_id: str | None = None,
        thinking: str | None = None,
        reasoning: object | None = None,
        tool_results: object | None = None,
        provider: str | None = None,
        provider_raw: object | None = None,
        additional_kwargs: dict | None = None,
        response_metadata: dict | None = None,
    ) -> bool:
        """Save a message to thread history via orchestrator REST. Fire-and-forget safe.

        ``tool_call_id`` is set only for role='tool' rows; ``thinking`` for
        role='ai' rows that carry reasoning content. The component columns
        (reasoning, tool_results, provider, provider_raw, additional_kwargs,
        response_metadata) are optional and were added in migration 0019.
        """
        if not self._client:
            return False

        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/messages"
        payload = {
            "role": role,
            "content": content,
            "tool_calls": tool_calls,
            "turn_number": turn_number,
            "metrics": metrics,
            "tool_call_id": tool_call_id,
            "thinking": thinking,
            "reasoning": reasoning,
            "tool_results": tool_results,
            "provider": provider,
            "provider_raw": provider_raw,
            "additional_kwargs": additional_kwargs,
            "response_metadata": response_metadata,
        }

        try:
            response = await self._client.post(url, json=payload)
            return response.status_code == 200
        except Exception as e:
            logger.warning(f"Failed to save thread message (non-fatal): {e}")
            return False

    async def request_thread_vm_upgrade(
        self, thread_id: str, cpu_cores: int = 8, memory: str = "16Gi"
    ) -> bool:
        """Request VM provisioning for a persistent thread (upgrade from container).

        Returns:
            True if accepted, False on failure.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/upgrade-to-vm"
        payload = {"cpu_cores": cpu_cores, "memory": memory}

        try:
            response = await self._client.post(url, json=payload)
            if response.status_code == 200:
                logger.info(f"VM upgrade requested for thread {thread_id}")
                return True
            else:
                logger.error(
                    f"VM upgrade request failed: {response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.error(f"VM upgrade request error: {e}")
            return False

    async def abort_thread_vm_upgrade(self, thread_id: str) -> bool:
        """Tear down a thread's VM after a failed/timed-out upgrade.

        Called by the live upgrade handler when ``_poll_vm_ready`` gives up, so a
        half-provisioned VM (+ its DataVolume + CDI importer pod) doesn't leak
        with nobody attached. Idempotent server-side; clears ``metadata.vm`` so a
        later retry isn't blocked by the provisioning-in-progress guard
        (workspace_tier_upgrade.md Q7).

        Returns:
            True if the teardown request was accepted, False otherwise.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/abort-vm-upgrade"
        try:
            response = await self._client.post(url)
            if response.status_code == 200:
                logger.info(f"VM upgrade aborted/torn down for thread {thread_id}")
                return True
            logger.error(
                f"VM upgrade abort failed: {response.status_code} - {response.text}"
            )
            return False
        except Exception as e:
            logger.error(f"VM upgrade abort error: {e}")
            return False

    async def request_thread_workspace_upgrade(
        self, thread_id: str, target_tier: str = "sandbox"
    ) -> bool:
        """Provision a real workspace container for a lite thread (upgrade from
        ``virtual``/``none`` to the ``sandbox`` tier).

        The session-side analogue of ``request_thread_vm_upgrade``: kicks off
        container provisioning, after which the caller polls
        ``get_thread_workspace`` until ready and hot-swaps the backend in place
        (workspace_tier_upgrade.md §4.2). The ``vm`` tier keeps its own
        operator-gated ``request_thread_vm_upgrade`` path.

        Returns:
            True if accepted (or already in progress), False on failure.
        """
        if not self._client:
            await self.connect()

        url = (
            f"{self.orchestrator_url}"
            f"/api/agents/threads/{thread_id}/upgrade-to-workspace"
        )
        payload = {"target_tier": target_tier}

        try:
            response = await self._client.post(url, json=payload)
            if response.status_code == 200:
                logger.info(
                    f"Workspace upgrade ({target_tier}) requested for thread "
                    f"{thread_id}"
                )
                return True
            else:
                logger.error(
                    f"Workspace upgrade request failed: "
                    f"{response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.error(f"Workspace upgrade request error: {e}")
            return False

    async def get_thread_workspace(
        self, thread_id: str, *, raise_on_denied: bool = False
    ) -> dict | None:
        """Poll workspace container status for a thread.

        Returns:
            Workspace status dict {status, pod_ip, pod_name, namespace},
            or None on failure.

        With ``raise_on_denied=True``, a 403 (capability-grant denial — the only
        403 this endpoint raises; ``require_internal`` uses 401) raises
        :class:`SessionGrantDenied` carrying the violation instead of collapsing
        to ``None``, so the attach path can fail with the real reason rather than
        the misleading 'No workspace container provisioned'. Other failures stay
        ``None`` (transient — keep polling).
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/workspace"
        try:
            # New pinned runtimes bind credential delivery to their exact
            # reciprocal thread/agent ownership. Older orchestrators ignore
            # this additive header; compatibility mode still permits old
            # agents until REQUIRE_PINNED_STATUS_IDENTITY is enabled.
            headers: dict[str, str] = {}
            if self.agent_id:
                headers["X-Agent-ID"] = self.agent_id
            if self.session_runtime_generation:
                headers["X-Session-Runtime-Generation"] = (
                    self.session_runtime_generation
                )
            if self.session_runtime_attach_token:
                headers["X-Session-Runtime-Attach-Token"] = (
                    self.session_runtime_attach_token
                )
            response = await self._client.get(url, headers=headers or None)
            if response.status_code == 200:
                return response.json()
            if raise_on_denied and response.status_code == 403:
                detail = "capability grants"
                try:
                    detail = response.json().get("detail") or detail
                except Exception:
                    pass
                raise SessionGrantDenied(detail)
            if response.status_code == 409:
                try:
                    detail = response.json().get("detail")
                except Exception:
                    detail = None
                if isinstance(detail, dict) and detail.get("code") == "session_ended":
                    raise SessionEnded("session ended before workspace attach")
            return None
        except (SessionGrantDenied, SessionEnded):
            raise
        except Exception as e:
            logger.debug(f"Failed to get thread workspace: {e}")
            return None

    async def get_claim_bundle(self, unit_id: str, lease_token: int) -> dict:
        """Fetch the resolved attach payload for a claimed run_queue unit.

        M3 pinned contract (stateless_agents.md §5.6 — credentials are
        delivered only against proof of the CURRENT lease):
        ``GET {orchestrator}/internal/units/{unit_id}/claim-bundle?lease_token=N``,
        authenticated exactly like every other internal call on this client
        (``X-Internal-Key`` attached by :meth:`connect`). The 200 body carries
        ``unit_id``/``thread_id``/``unit_kind``/``execution_lane``, the
        ``watermarks`` pair, and the ``attach`` object — the existing
        ``/session/attach`` body, fed to ``_attach_session`` unchanged.

        Raises :class:`ClaimBundleError` on any non-200; network errors
        propagate as httpx exceptions (the caller treats both as bundle
        failure and releases its claim, token-guarded).
        """
        if not self._client:
            await self.connect()
        url = f"{self.orchestrator_url}/internal/units/{unit_id}/claim-bundle"
        response = await self._client.get(
            url,
            params={
                "lease_token": int(lease_token),
                "pod_name": os.environ.get("POD_NAME")
                or os.environ.get("HOSTNAME", ""),
                "pod_uid": os.environ.get("POD_UID", ""),
            },
        )
        if response.status_code == 200:
            return response.json()
        detail = ""
        try:
            detail = response.text or ""
        except Exception:
            pass
        recovery = (
            _workspace_recovery_receipt(response, lease_token)
            if response.status_code == 409
            else None
        )
        raise ClaimBundleError(
            response.status_code,
            detail,
            code=recovery.code if recovery else None,
            recovery=recovery,
        )

    async def report_workspace_recovery(
        self,
        unit_id: str,
        lease_token: int,
        *,
        code: WorkspaceRecoveryCode,
        request_id: UUID | str,
    ) -> WorkspaceRecoveryDisposition:
        """Report an observation using a caller-retained idempotency identity."""
        if not self._client:
            await self.connect()
        response = await self._client.post(
            f"{self.orchestrator_url}/internal/units/{unit_id}/workspace-recovery",
            json={
                "lease_token": int(lease_token),
                "request_id": str(request_id),
                "code": code.value,
                "pod_name": os.environ.get("POD_NAME")
                or os.environ.get("HOSTNAME", ""),
                "pod_uid": os.environ.get("POD_UID", ""),
            },
            timeout=15.0,
        )
        receipt = _workspace_recovery_receipt(response, lease_token)
        if response.status_code != 200 or receipt is None:
            raise ClaimBundleError(response.status_code, response.text)
        return receipt

    async def get_workspace_recovery_disposition(
        self,
        unit_id: str,
        lease_token: int,
    ) -> WorkspaceRecoveryDisposition | None:
        """Read the exact non-executable receipt after an ambiguous response."""
        if not self._client:
            await self.connect()
        response = await self._client.get(
            f"{self.orchestrator_url}/internal/units/{unit_id}/workspace-recovery-disposition",
            params={"lease_token": int(lease_token)},
            timeout=15.0,
        )
        if response.status_code == 404:
            return None
        receipt = _workspace_recovery_receipt(response, lease_token)
        if response.status_code != 200 or receipt is None:
            raise ClaimBundleError(response.status_code, response.text)
        return receipt

    async def get_thread_canvas(self, thread_id: str) -> dict[str, Any] | None:
        """Fetch the delegated user's logical ``main`` Canvas state.

        ``204`` means the Canvas has never been created. HTTP and authorization
        failures are deliberately raised so a tool call cannot misrepresent
        them as an empty stage.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        url = (
            f"{self.orchestrator_url}/api/internal/persistent/threads/{thread_id}"
            "/canvases/main"
        )
        try:
            response = await self._client.get(
                url, timeout=CANVAS_REQUEST_TIMEOUT_SECONDS
            )
        except httpx.RequestError:
            raise CanvasClientError(
                "canvas_service_unavailable",
                "Canvas service is temporarily unavailable",
            ) from None
        if response.status_code == 204:
            return None
        if not response.is_success:
            raise _canvas_response_error(response)
        try:
            data = response.json()
        except Exception:
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            ) from None
        if not isinstance(data, dict):
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            )
        return data

    async def set_thread_canvas(
        self, thread_id: str, payload: dict[str, Any]
    ) -> CanvasSetResult:
        """Set ``main`` through the internal delegated-user Canvas adapter.

        The mutation signal is transport-only. Its absence means ``true`` for
        compatibility with older orchestrator replicas whose file/app set
        endpoint always changed state; a present value is a closed contract.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        url = (
            f"{self.orchestrator_url}/api/internal/persistent/threads/{thread_id}"
            "/canvases/main/set"
        )
        try:
            response = await self._client.post(
                url,
                json=payload,
                timeout=CANVAS_REQUEST_TIMEOUT_SECONDS,
            )
        except httpx.RequestError:
            raise CanvasClientError(
                "canvas_service_unavailable",
                "Canvas service is temporarily unavailable",
            ) from None
        if not response.is_success:
            raise _canvas_response_error(response)
        try:
            data = response.json()
        except Exception:
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            ) from None
        if not isinstance(data, dict):
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            )
        mutation_header = response.headers.get("X-Canvas-Mutation-Changed")
        if mutation_header is None:
            changed = True
        elif mutation_header == "true":
            changed = True
        elif mutation_header == "false":
            changed = False
        else:
            raise CanvasClientError(
                "invalid_canvas_response",
                "Canvas service returned an invalid response",
            )
        return CanvasSetResult(state=data, changed=changed)

    async def clear_thread_canvas(self, thread_id: str) -> CanvasClearResult | None:
        """Clear ``main`` through the internal delegated-user Canvas adapter.

        ``204`` means no Canvas row has ever existed. An already-cleared row is
        returned with ``changed=False`` so the tool can expose its revisioned
        logical state without emitting a duplicate transition invalidation.
        """
        if not self._client:
            await self.connect()
        assert self._client is not None

        url = (
            f"{self.orchestrator_url}/api/internal/persistent/threads/{thread_id}"
            "/canvases/main"
        )
        try:
            response = await self._client.delete(
                url, timeout=CANVAS_REQUEST_TIMEOUT_SECONDS
            )
        except httpx.RequestError:
            raise CanvasClientError(
                "canvas_service_unavailable",
                "Canvas service is temporarily unavailable",
            ) from None
        if response.status_code == 204:
            return None
        if not response.is_success:
            raise _canvas_response_error(response)
        try:
            data = response.json()
        except Exception:
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            ) from None
        if not isinstance(data, dict):
            raise CanvasClientError(
                "invalid_canvas_response", "Canvas service returned an invalid response"
            )
        return CanvasClearResult(
            state=data,
            changed=(
                response.headers.get("X-Canvas-Mutation-Changed", "true").lower()
                == "true"
            ),
        )

    async def request_job_workspace_upgrade(
        self, job_id: str, target_tier: str = "sandbox"
    ) -> bool:
        """Provision a real workspace container for a lite (``virtual``/``none``)
        worker job, in place — the worker analogue of
        ``request_thread_workspace_upgrade`` (workspace_tier_upgrade.md §4.3 W2).

        The job stays ``processing`` (no pause, no re-dispatch): the same running
        agent provisions, then polls ``get_job_workspace_status`` until ready and
        swaps its ``WorkspaceManager`` backend in place, re-``ainvoke``-ing from
        the local checkpoint. Idempotent server-side.

        Returns:
            True if accepted (or already in progress), False on failure.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/provision-workspace"
        payload = {"target_tier": target_tier}

        try:
            response = await self._client.post(url, json=payload)
            if response.status_code == 200:
                logger.info(
                    f"Workspace upgrade ({target_tier}) requested for job {job_id}"
                )
                return True
            else:
                logger.error(
                    f"Job workspace upgrade request failed: "
                    f"{response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.error(f"Job workspace upgrade request error: {e}")
            return False

    async def get_job_workspace_status(self, job_id: str) -> dict | None:
        """Poll workspace container connection details for a running job.

        The job-side analogue of ``get_thread_workspace`` — returns the
        ``context.workspace_container`` block (status + pod connection fields)
        the agent's ``_poll_job_workspace_ready`` consumes to build the upgraded
        ``RemoteBackend`` (workspace_tier_upgrade.md §4.3 W1).

        Returns:
            Workspace status dict {status, pod_ip, pod_port, pod_name,
            namespace, ssh_key_path}, or None on failure.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/workspace-status"
        try:
            response = await self._client.get(url)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.debug(f"Failed to get job workspace status: {e}")
            return None

    async def get_job_brief(self, job_id: str) -> dict | None:
        """Fetch the job's task-brief fields (internal ``/brief`` endpoint).

        ``JobResumeRequest`` carries no description/required_deliverables/
        kickoff_message, so a resumed agent backfills them from here before
        serving the virtual ``task_brief.md``
        (knowledge-base/knowledge/issues/fresh_job_dispatched_as_resume_skips_seeding.md).

        Returns:
            Dict {description, required_deliverables, kickoff_message}, or
            None on failure.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/brief"
        try:
            response = await self._client.get(url)
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            logger.debug(f"Failed to get job brief: {e}")
            return None

    async def get_thread_lifecycle(self, thread_id: str) -> dict | None:
        """Fetch minimal lifecycle fields for the agent's status watchdog.

        Returns:
            ``{status, agent_id, ended_at}`` on success; ``None`` on any
            failure (caller treats that as "skip this poll cycle").
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/lifecycle"
        try:
            headers: dict[str, str] = {}
            if self.agent_id:
                headers["X-Agent-ID"] = self.agent_id
            if self.session_runtime_generation:
                headers["X-Session-Runtime-Generation"] = (
                    self.session_runtime_generation
                )
            if self.session_runtime_attach_token:
                headers["X-Session-Runtime-Attach-Token"] = (
                    self.session_runtime_attach_token
                )
            response = await self._client.get(url, headers=headers or None)
            if response.status_code == 200:
                return response.json()
            if response.status_code in {403, 404, 409}:
                # This internal endpoint is bound to the exact runtime
                # identity. A definitive ownership/lifecycle refusal means
                # the polling pod is stale; distinguish it from a transient
                # transport failure so the watchdog exits instead of holding
                # mounts/tools forever.
                return {"status": "runtime_moved", "authority_refused": True}
            return None
        except Exception as e:
            logger.debug(f"Failed to get thread lifecycle: {e}")
            return None

    async def deregister(self) -> bool:
        """Deregister this agent from the orchestrator.

        Returns:
            True if deregistration succeeded, False otherwise
        """
        if not self.agent_id:
            logger.warning("Cannot deregister: agent_id not set")
            return False

        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/{self.agent_id}"

        try:
            response = await self._client.delete(url)

            if response.status_code == 200:
                logger.info(f"Deregistered agent {self.agent_id} from orchestrator")
                self.agent_id = None
                self.dispatch_process_generation = None
                self.clear_session_runtime_identity()
                return True
            else:
                logger.error(
                    f"Failed to deregister from orchestrator: {response.status_code} - {response.text}"
                )
                return False

        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for deregistration: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error during deregistration: {e}")
            return False

    async def update_thread_status(
        self,
        thread_id: str,
        status: str,
        *,
        pinned_agent_id: Optional[str] = None,
        session_runtime_generation: Optional[str] = None,
        session_runtime_attach_token: Optional[str] = None,
        retirement_disposition: Optional[str] = None,
        retirement_permanent: Optional[bool] = None,
        session_runtime_retirement_token: Optional[str] = None,
        local_runtime_quiesced: bool = False,
        local_quiescence_protocol: Optional[str] = None,
        workspace_generation: Optional[str] = None,
        workspace_runtime_incarnation: Optional[str] = None,
    ) -> bool:
        """Update thread status via orchestrator REST.

        Args:
            thread_id: Thread UUID
            status: New status ('active' or 'ended'). When 'ended', the
                orchestrator routes through end_thread() so ended_at is stamped.

        Returns:
            True if update succeeded, False otherwise
        """
        if not self._client:
            return False
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/status"
        try:
            body = {"status": status}
            if retirement_disposition is not None:
                if status not in {"ending", "ended"} or retirement_disposition not in {
                    "ended",
                    "suspended",
                }:
                    return False
                body["retirement_disposition"] = retirement_disposition
            if retirement_permanent is not None:
                if (
                    status not in {"ending", "ended"}
                    or type(retirement_permanent) is not bool
                ):
                    return False
                body["retirement_permanent"] = retirement_permanent
            retirement_token = None
            if session_runtime_retirement_token is not None:
                retirement_token = _canonical_runtime_uuid(
                    session_runtime_retirement_token
                )
                if retirement_token is None or status != "ended":
                    return False
                body["session_runtime_retirement_token"] = retirement_token
            if local_runtime_quiesced:
                if (
                    status != "ended"
                    or retirement_token is None
                    or local_quiescence_protocol
                    not in _AGENT_LOCAL_QUIESCENCE_PROTOCOLS
                ):
                    return False
                body["local_runtime_quiesced"] = True
                body["local_quiescence_protocol"] = local_quiescence_protocol
                canonical_workspace_generation = (
                    _canonical_runtime_uuid(workspace_generation)
                    if workspace_generation is not None
                    else None
                )
                canonical_workspace_incarnation = (
                    _canonical_runtime_uuid(workspace_runtime_incarnation)
                    if workspace_runtime_incarnation is not None
                    else None
                )
                if (canonical_workspace_generation is None) != (
                    canonical_workspace_incarnation is None
                ):
                    return False
                if workspace_generation is not None and (
                    canonical_workspace_generation is None
                    or canonical_workspace_incarnation is None
                ):
                    return False
                if canonical_workspace_generation is not None:
                    body["workspace_generation"] = canonical_workspace_generation
                    body["workspace_runtime_incarnation"] = (
                        canonical_workspace_incarnation
                    )
                if (
                    local_quiescence_protocol == "workspace_process_zero_v1"
                    and canonical_workspace_generation is None
                ) or (
                    local_quiescence_protocol == "agent_runtime_zero_v1"
                    and canonical_workspace_generation is not None
                ):
                    return False
            elif (
                local_quiescence_protocol is not None
                or workspace_generation is not None
                or workspace_runtime_incarnation is not None
            ):
                return False
            if pinned_agent_id is not None:
                body["agent_id"] = pinned_agent_id
                pod_uid = str(os.environ.get("POD_UID") or "").strip()
                if pod_uid:
                    body["pod_uid"] = pod_uid
                if not self.dispatch_process_generation:
                    return False
                body["process_generation"] = self.dispatch_process_generation
                generation = _canonical_runtime_uuid(
                    session_runtime_generation or self.session_runtime_generation
                )
                if self.pinned_runtime_generation_contract and generation is None:
                    return False
                if generation is not None:
                    body["session_runtime_generation"] = generation
                attach_token = _canonical_runtime_uuid(
                    session_runtime_attach_token or self.session_runtime_attach_token
                )
                if (
                    session_runtime_attach_token is not None
                    or self.session_runtime_attach_token is not None
                ) and attach_token is None:
                    return False
                if attach_token is not None:
                    body["session_runtime_attach_token"] = attach_token
            r = await self._client.put(url, json=body)
            if r.status_code != 200:
                return False
            if not local_runtime_quiesced:
                return True
            # A generic 200 is not a local-quiescence receipt. A typed terminal
            # settlement normally lets the old runtime clear its exact fence.
            # Permanent dedicated-agent teardown has one additional exact
            # handoff: after the server durably accepts local quiescence it may
            # authorize this caller to exit so an owner retry can delete the
            # now-unmounted agent PVC. The token echo prevents a generic or
            # stale ``ending`` response from being mistaken for that handoff.
            try:
                payload = r.json()
            except Exception:
                return False
            terminal_settlement = bool(
                isinstance(payload, dict)
                and payload.get("status")
                in {"ended", "suspended", "deleted", "settled_or_superseded"}
                and payload.get("retirement_disposition") == retirement_disposition
                and payload.get("retirement_permanent") is retirement_permanent
            )
            exact_exit_handoff = bool(
                isinstance(payload, dict)
                and retirement_permanent is True
                and payload.get("status") == "ending"
                and payload.get("retiring_agent_exit_authorized") is True
                and payload.get("retirement_disposition") == retirement_disposition
                and payload.get("retirement_permanent") is True
                and _canonical_runtime_uuid(
                    payload.get("session_runtime_retirement_token")
                )
                == retirement_token
            )
            return terminal_settlement or exact_exit_handoff
        except Exception as e:
            logger.warning(f"Thread status update failed (non-fatal): {e}")
            return False

    async def begin_thread_retirement(
        self,
        thread_id: str,
        *,
        pinned_agent_id: str,
        session_runtime_generation: str,
        session_runtime_attach_token: str,
        retirement_disposition: str,
        retirement_permanent: bool = False,
    ) -> dict[str, Any] | None:
        """Authorize one exact pinned retirement and return its opaque token.

        Unlike ordinary status writes, Begin is not complete merely because
        HTTP returned 200. The response must echo the exact immutable
        disposition and contain a canonical server-minted retirement token;
        callers retain that token across local quiescence and lost-response
        settlement retries.
        """

        if (
            not self._client
            or not self.dispatch_process_generation
            or retirement_disposition not in {"ended", "suspended"}
            or type(retirement_permanent) is not bool
        ):
            return None
        generation = _canonical_runtime_uuid(session_runtime_generation)
        attach_token = _canonical_runtime_uuid(session_runtime_attach_token)
        if generation is None or attach_token is None or not pinned_agent_id:
            return None
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/status"
        body = {
            "status": "ending",
            "agent_id": pinned_agent_id,
            "process_generation": self.dispatch_process_generation,
            "session_runtime_generation": generation,
            "session_runtime_attach_token": attach_token,
            "retirement_disposition": retirement_disposition,
            "retirement_permanent": retirement_permanent,
        }
        pod_uid = str(os.environ.get("POD_UID") or "").strip()
        if pod_uid:
            body["pod_uid"] = pod_uid
        try:
            response = await self._client.put(url, json=body)
            if response.status_code != 200:
                return None
            payload = response.json()
            retirement_token = _canonical_runtime_uuid(
                payload.get("session_runtime_retirement_token")
                if isinstance(payload, dict)
                else None
            )
            if (
                not isinstance(payload, dict)
                or payload.get("status") != "ending"
                or payload.get("retirement_disposition") != retirement_disposition
                or payload.get("retirement_permanent") is not retirement_permanent
                or retirement_token is None
            ):
                return None
            return {
                "status": "ending",
                "retirement_disposition": retirement_disposition,
                "retirement_permanent": retirement_permanent,
                "session_runtime_retirement_token": retirement_token,
            }
        except Exception as exc:
            logger.warning(
                "Thread retirement begin failed (non-fatal): %s", type(exc).__name__
            )
            return None

    async def get_thread_retirement_outcome(
        self,
        thread_id: str,
        *,
        pinned_agent_id: str,
        session_runtime_generation: str,
        session_runtime_attach_token: str,
        session_runtime_retirement_token: str,
        retirement_disposition: str,
        retirement_permanent: bool,
    ) -> dict[str, Any] | None:
        """Read back only the exact append-only retirement outcome.

        This is deliberately not a broad lifecycle inference.  A moved
        generation, cleared binding, or generic 409 does not prove that this
        runtime's local-quiescence receipt settled.  The server returns a
        positive result only for the exact G/attach/T/disposition/permanent
        ledger identity captured before local cleanup.
        """

        if (
            not self._client
            or not pinned_agent_id
            or retirement_disposition not in {"ended", "suspended"}
            or type(retirement_permanent) is not bool
        ):
            return None
        generation = _canonical_runtime_uuid(session_runtime_generation)
        attach_token = _canonical_runtime_uuid(session_runtime_attach_token)
        retirement_token = _canonical_runtime_uuid(session_runtime_retirement_token)
        if generation is None or attach_token is None or retirement_token is None:
            return None
        headers = {
            "X-Agent-ID": pinned_agent_id,
            "X-Session-Runtime-Generation": generation,
            "X-Session-Runtime-Attach-Token": attach_token,
            "X-Session-Runtime-Retirement-Token": retirement_token,
            "X-Retirement-Disposition": retirement_disposition,
            "X-Retirement-Permanent": ("true" if retirement_permanent else "false"),
        }
        url = (
            f"{self.orchestrator_url}/api/agents/threads/{thread_id}/retirement-outcome"
        )
        try:
            response = await self._client.get(url, headers=headers)
            if response.status_code != 200:
                return None
            payload = response.json()
        except Exception as exc:
            logger.warning(
                "Thread retirement outcome read failed (non-fatal): %s",
                type(exc).__name__,
            )
            return None
        if (
            not isinstance(payload, dict)
            or payload.get("retirement_disposition") != retirement_disposition
            or payload.get("retirement_permanent") is not retirement_permanent
        ):
            return None
        status = payload.get("status")
        if status == "ending":
            return {
                "status": "ending",
                "retirement_disposition": retirement_disposition,
                "retirement_permanent": retirement_permanent,
            }
        if (
            status != "settled_or_superseded"
            or payload.get("outcome") not in {"settled", "deleted"}
            or not isinstance(payload.get("settled_at"), str)
            or not payload["settled_at"].strip()
        ):
            return None
        return {
            "status": "settled_or_superseded",
            "retirement_disposition": retirement_disposition,
            "retirement_permanent": retirement_permanent,
            "outcome": payload["outcome"],
            "settled_at": payload["settled_at"],
        }

    async def file_officer_wake(
        self, thread_id: str, minutes: int, reason: str
    ) -> bool:
        """File an officer session's durable timer wake (centurion.md §4).

        The orchestrator upserts the pending ``timer`` outbox row
        (``fire_at = now + minutes``); its wake drain injects the timer wake
        when due. Failure is non-fatal — the officer watchdog files
        ``sleep_max`` whenever no timer row is pending.

        Returns:
            True if the orchestrator accepted the filing.
        """
        if not self._client:
            return False
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/officer/wake"
        try:
            r = await self._client.post(
                url, json={"minutes": int(minutes), "reason": reason or ""}
            )
            return r.status_code in (200, 201)
        except Exception as e:
            logger.warning(f"Officer wake filing failed (non-fatal): {e}")
            return False

    async def suspend_thread(
        self,
        thread_id: str,
        *,
        pinned_agent_id: str | None = None,
        session_runtime_generation: str | None = None,
        session_runtime_attach_token: str | None = None,
        session_runtime_retirement_token: str | None = None,
        local_runtime_quiesced: bool = False,
        local_quiescence_protocol: str | None = None,
        workspace_generation: str | None = None,
        workspace_runtime_incarnation: str | None = None,
    ) -> bool:
        """Request a clean drain-suspend of a thread (drift-drain path).

        The orchestrator snapshots the workspace to S3, tears down the
        workspace + agent pods, and flips the thread to 'suspended' so the
        next user input resumes on a fresh agent. Generous timeout — the
        snapshot of a large workspace takes a while and this call is the
        last thing the pod does before exiting.

        Returns:
            True only if the orchestrator confirmed the suspend.
        """
        if not self._client:
            return False
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/suspend"
        try:
            agent_id = pinned_agent_id or self.agent_id
            generation = _canonical_runtime_uuid(
                session_runtime_generation or self.session_runtime_generation
            )
            token = _canonical_runtime_uuid(
                session_runtime_attach_token or self.session_runtime_attach_token
            )
            headers: dict[str, str] = {}
            if agent_id:
                headers["X-Agent-ID"] = agent_id
            if generation:
                headers["X-Session-Runtime-Generation"] = generation
            if token:
                headers["X-Session-Runtime-Attach-Token"] = token
            retirement_token = _canonical_runtime_uuid(session_runtime_retirement_token)
            if local_runtime_quiesced:
                if (
                    retirement_token is None
                    or local_quiescence_protocol
                    not in _AGENT_LOCAL_QUIESCENCE_PROTOCOLS
                ):
                    return False
                headers["X-Session-Runtime-Retirement-Token"] = retirement_token
                headers["X-Session-Local-Quiesced"] = "true"
                headers["X-Session-Local-Quiescence-Protocol"] = (
                    local_quiescence_protocol
                )
                canonical_workspace_generation = (
                    _canonical_runtime_uuid(workspace_generation)
                    if workspace_generation is not None
                    else None
                )
                canonical_workspace_incarnation = (
                    _canonical_runtime_uuid(workspace_runtime_incarnation)
                    if workspace_runtime_incarnation is not None
                    else None
                )
                if (canonical_workspace_generation is None) != (
                    canonical_workspace_incarnation is None
                ):
                    return False
                if workspace_generation is not None and (
                    canonical_workspace_generation is None
                    or canonical_workspace_incarnation is None
                ):
                    return False
                if canonical_workspace_generation is not None:
                    headers["X-Workspace-Generation"] = canonical_workspace_generation
                    headers["X-Workspace-Runtime-Incarnation"] = (
                        canonical_workspace_incarnation
                    )
                if (
                    local_quiescence_protocol == "workspace_process_zero_v1"
                    and canonical_workspace_generation is None
                ) or (
                    local_quiescence_protocol == "agent_runtime_zero_v1"
                    and canonical_workspace_generation is not None
                ):
                    return False
            elif (
                session_runtime_retirement_token is not None
                or local_quiescence_protocol is not None
                or workspace_generation is not None
                or workspace_runtime_incarnation is not None
            ):
                return False
            r = await self._client.post(
                url,
                timeout=300.0,
                headers=headers or None,
            )
            if r.status_code != 200:
                logger.warning(f"Thread suspend rejected: {r.status_code} - {r.text}")
                return False
            return bool(r.json().get("suspended"))
        except Exception as e:
            logger.warning(f"Thread suspend request failed: {e}")
            return False

    async def bind_pod_runtime_actor(self, thread_id: str) -> bool:
        """Exchange this pod's thread-less bootstrap for the attached session.

        Dedicated session pods get their actor inside ``register()`` because
        the orchestrator knew the thread when it minted the pod. A warm pool
        pod registers thread-less and learns its session later, so it has to
        ask again — presenting the same kind of pod-unique secret, which the
        orchestrator cross-checks against its own ``agents.thread_id`` binding.

        Returns False when this pod holds no pod bootstrap (an older image, or
        a pod the provisioner did not mint) or the exchange is refused. The
        caller must treat that as a failed attach rather than continuing: a
        session that runs without actor identity looks healthy and then denies
        every machine-tag write several tool calls later.
        """
        if not self._client or not self.agent_id:
            return False
        bootstrap = os.environ.get("SRW_RUNTIME_ACTOR_POD_BOOTSTRAP", "").strip()
        if not bootstrap:
            logger.warning(
                "No pod runtime actor bootstrap in env — this pod cannot take "
                "a session that needs actor identity"
            )
            return False
        url = (
            f"{self.orchestrator_url}/api/agents/{self.agent_id}/runtime-actor/session"
        )
        try:
            response = await self._client.post(
                url,
                json={"thread_id": str(thread_id)},
                headers={RUNTIME_ACTOR_BOOTSTRAP_HEADER: bootstrap},
            )
        except Exception as e:
            logger.warning(f"Pod runtime actor exchange failed: {e}")
            return False
        if response.status_code != 200:
            logger.warning(
                "Pod runtime actor exchange refused (%s): %s",
                response.status_code,
                response.text[:200],
            )
            return False
        self.adopt_runtime_actor(
            RuntimeActorContext.from_payload(response.json().get("runtime_actor"))
        )
        logger.info(f"Runtime actor bound for thread {thread_id}")
        return True

    def adopt_runtime_actor(self, actor: RuntimeActorContext | None) -> None:
        """Bind the exact actor object shared by heartbeat and session tools."""

        self.runtime_actor = actor
        self._runtime_actor_maintenance_failures = 0
        self._runtime_actor_retry_at = 0.0

    def clear_runtime_actor(self) -> None:
        """Drop the actor when this pod stops serving its session.

        A pool pod goes back into the idle pool and may be handed a different
        thread — and a different project — next. Keeping the old actor would
        leave a credential scoped to the previous session lying in memory.
        """
        self.adopt_runtime_actor(None)

    async def maintain_runtime_actor(self, *, force: bool = False) -> tuple[bool, str]:
        """Renew/recover the hidden Officer actor with bounded local backoff.

        Heartbeats call this before the refresh idle wall; the persistent turn
        gate calls it with ``force=True`` before any provider request. Identity
        is never supplied in the body: the opaque refresh bearer selects a
        grant and the server re-derives its Post/thread/agent authority.
        """

        actor = self.runtime_actor
        if actor is None:
            return False, "server-derived actor context is missing"
        if actor.caller_kind != "officer":
            return True, "non-Officer grant keeps its existing lifecycle"
        if not actor.refresh_credential:
            return False, "runtime actor refresh credential is missing"
        renew_before = int(
            os.environ.get("RUNTIME_ACTOR_OFFICER_RENEW_BEFORE_SECONDS", "21600")
        )
        if not force and not actor.refresh_needs_renewal(skew_seconds=renew_before):
            return True, "Officer runtime grant is inside its renewal window"
        now = asyncio.get_running_loop().time()
        if now < self._runtime_actor_retry_at:
            return False, "Officer runtime authorization retry is backed off"

        async with self._runtime_actor_maintenance_lock:
            actor = self.runtime_actor
            if actor is None or not actor.refresh_credential:
                return False, "runtime actor refresh credential is missing"
            now = asyncio.get_running_loop().time()
            if now < self._runtime_actor_retry_at:
                return False, "Officer runtime authorization retry is backed off"
            if not self._client:
                await self.connect()
            assert self._client is not None
            try:
                refresh_headers = {
                    RUNTIME_ACTOR_REFRESH_HEADER: actor.refresh_credential
                }
                if force:
                    # ``force`` is used only by the persistent loop's
                    # before-turn authorization callback. This hidden phase
                    # assertion lets the dark verification plan wait for the
                    # post-persistence/pre-provider boundary; it is not
                    # identity or authorization input.
                    refresh_headers[RUNTIME_ACTOR_MAINTENANCE_PHASE_HEADER] = (
                        RUNTIME_ACTOR_MAINTENANCE_PHASE_PRE_TURN
                    )
                response = await self._client.post(
                    f"{self.orchestrator_url}/api/runtime-actors/refresh",
                    headers=refresh_headers,
                )
                if response.status_code != 200:
                    code = f"http-{response.status_code}"
                    try:
                        detail = response.json().get("detail")
                        if isinstance(detail, dict) and isinstance(
                            detail.get("code"), str
                        ):
                            code = detail["code"]
                    except Exception:
                        pass
                    raise RuntimeError(f"refresh denied ({code})")
                payload = response.json().get("runtime_actor")
                if not actor.apply_refreshed_payload(payload):
                    raise RuntimeError("refresh response changed identity")
            except Exception as exc:
                self._runtime_actor_maintenance_failures += 1
                base = max(
                    1,
                    int(os.environ.get("RUNTIME_ACTOR_RETRY_BASE_SECONDS", "60")),
                )
                ceiling = max(
                    base,
                    int(os.environ.get("RUNTIME_ACTOR_RETRY_MAX_SECONDS", "900")),
                )
                delay = min(
                    ceiling,
                    base * (2 ** min(self._runtime_actor_maintenance_failures - 1, 8)),
                )
                self._runtime_actor_retry_at = now + delay
                # Never include response bodies or credentials in this log.
                reason = type(exc).__name__
                logger.error(
                    "Officer runtime authorization maintenance failed (%s); "
                    "planning is suppressed for %ss",
                    type(exc).__name__,
                    delay,
                )
                return False, reason[:160]

            self._runtime_actor_maintenance_failures = 0
            self._runtime_actor_retry_at = 0.0
            return True, "Officer runtime authorization maintained"

    async def release_thread_agent(
        self,
        thread_id: str,
        *,
        session_runtime_generation: str | None = None,
        session_runtime_attach_token: str | None = None,
        agent_pod_uid: str | None = None,
        local_runtime_quiesced: bool = False,
        local_quiescence_protocol: str | None = None,
        workspace_generation: str | None = None,
        workspace_runtime_incarnation: str | None = None,
    ) -> bool:
        """Clear threads.agent_id when this agent's session attach fails.

        Lets the orchestrator dispatch the next WS reconnect to a healthy
        agent instead of re-targeting this (now session-less) one.
        """
        if not self._client or not self.agent_id:
            return False
        generation = _canonical_runtime_uuid(
            session_runtime_generation or self.session_runtime_generation
        )
        attach_token = _canonical_runtime_uuid(
            session_runtime_attach_token or self.session_runtime_attach_token
        )
        if generation is None or attach_token is None:
            logger.warning(
                "Cannot release thread binding without its exact runtime "
                "generation and attach token"
            )
            return False
        pod_uid = str(agent_pod_uid or "").strip()
        if (
            not pod_uid
            or local_runtime_quiesced is not True
            or local_quiescence_protocol not in _ATTACH_RELEASE_QUIESCENCE_PROTOCOLS
        ):
            logger.warning(
                "Cannot release delivered thread binding without exact local "
                "quiescence proof"
            )
            return False
        canonical_workspace_generation = (
            _canonical_runtime_uuid(workspace_generation)
            if workspace_generation is not None
            else None
        )
        canonical_workspace_incarnation = (
            _canonical_runtime_uuid(workspace_runtime_incarnation)
            if workspace_runtime_incarnation is not None
            else None
        )
        if (canonical_workspace_generation is None) != (
            canonical_workspace_incarnation is None
        ):
            return False
        if workspace_generation is not None and (
            canonical_workspace_generation is None
            or canonical_workspace_incarnation is None
        ):
            return False
        if (
            local_quiescence_protocol == "workspace_process_zero_v1"
            and canonical_workspace_generation is None
        ) or (
            local_quiescence_protocol == "agent_runtime_zero_v1"
            and canonical_workspace_generation is not None
        ):
            return False
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/release-agent"
        try:
            # The server CASes this exact agent/thread pair.  A delayed
            # background attach failure must never clear a successor agent's
            # binding after the thread has already been reassigned.
            body: dict[str, Any] = {
                "agent_id": self.agent_id,
                "session_runtime_generation": generation,
                "session_runtime_attach_token": attach_token,
                "agent_pod_uid": pod_uid,
                "local_runtime_quiesced": True,
                "local_quiescence_protocol": local_quiescence_protocol,
            }
            if canonical_workspace_generation is not None:
                body["workspace_generation"] = canonical_workspace_generation
                body["workspace_runtime_incarnation"] = canonical_workspace_incarnation
            r = await self._client.post(url, json=body)
            if r.status_code != 200:
                return False
            try:
                payload = r.json()
            except Exception:
                return False
            return bool(
                isinstance(payload, dict)
                and payload.get("status")
                in {
                    "released",
                    "already_detached",
                    "retirement_acknowledged",
                }
            )
        except Exception as e:
            logger.warning(f"Thread→agent release failed (non-fatal): {e}")
            return False

    async def update_thread_config(
        self,
        thread_id: str,
        config_override: dict[str, Any],
        datasource_ids: Optional[list[str]] = None,
        *,
        snapshot_generation: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        """Persist runtime config changes for a thread.

        Args:
            thread_id: Thread UUID
            config_override: Partial config dict to deep-merge
                             (e.g. ``{"llm": {"model": "..."}}``)
            datasource_ids: Desired FULL datasource selection (live_session_
                            settings.md Slice B). ``None`` = no change;
                            ``[]`` = detach all. The orchestrator authorizes,
                            grant-checks the derived tool flip, and persists
                            ``metadata.datasource_ids``; the caller then
                            re-fetches ``get_thread_workspace`` for the
                            enriched datasource payloads.

        Returns:
            The orchestrator-enriched ``config_override`` (with resolved
            ``base_url``/``api_key`` for endpoint-backed models) on success,
            or ``None`` on transient failure (network, 5xx). Callers must use
            the returned dict — not the input — when rebuilding the LLM,
            otherwise custom endpoint requests fall through to api.openai.com.

        Raises:
            ThreadConfigUpdateDenied: on a 4xx response — a deliberate
                rejection (grant denial, invalid override, unknown thread)
                whose ``detail`` must reach the user, not a transient failure
                to retry or fall back from.
        """
        if not self._client:
            return None
        url = f"{self.orchestrator_url}/api/agents/threads/{thread_id}/config"
        payload: dict[str, Any] = {
            "config_override": config_override,
            "snapshot_patch_protocol": 1,
            "snapshot_generation": snapshot_generation,
        }
        if datasource_ids is not None:
            payload["datasource_ids"] = [str(v) for v in datasource_ids]
        try:
            r = await self._client.patch(url, json=payload)
            if r.status_code != 200:
                if 400 <= r.status_code < 500:
                    try:
                        detail = r.json().get("detail")
                    except Exception:
                        detail = None
                    if not isinstance(detail, str):
                        detail = str(detail) if detail else (r.text or "")[:500]
                    raise ThreadConfigUpdateDenied(r.status_code, detail)
                logger.warning(
                    f"Thread config update rejected: {r.status_code} - {r.text[:200]}"
                )
                return None
            data = r.json()
            enriched = data.get("config_override")
            return enriched if isinstance(enriched, dict) else config_override
        except ThreadConfigUpdateDenied:
            raise
        except Exception as e:
            logger.warning(f"Thread config update failed (non-fatal): {e}")
            return None

    async def heartbeat(
        self,
        status: str,
        job_id: Optional[str] = None,
        metrics: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Send a heartbeat to the orchestrator.

        Args:
            status: Agent status (booting, ready, working, completed, failed)
            job_id: Current job ID if working
            metrics: Optional metrics dict (memory_mb, cpu_percent, tokens_processed)

        Returns:
            The orchestrator's JSON response body on success (truthy);
            ``None`` on failure. The response carries any pending
            ``intents`` (drain, version-upgrade hints) the agent should
            react to. Callers that only care about success can still
            check the return value as truthy/falsy.
        """
        if not self.agent_id:
            logger.warning("Cannot send heartbeat: agent_id not set")
            return None

        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/agents/{self.agent_id}/heartbeat"
        payload: dict[str, Any] = {"status": status}

        if self.session_runtime_generation is not None:
            payload["session_runtime_generation"] = self.session_runtime_generation
        if self.session_runtime_attach_token is not None:
            payload["session_runtime_attach_token"] = self.session_runtime_attach_token

        if job_id:
            payload["current_job_id"] = job_id
        if metrics:
            payload["metrics"] = metrics

        try:
            response = await self._client.post(url, json=payload)

            if response.status_code == 200:
                logger.debug(f"Heartbeat sent: status={status}, job_id={job_id}")
                try:
                    return response.json() or {}
                except Exception:
                    return {}
            elif response.status_code == 404:
                # Agent not found - might have been cleaned up, try to re-register
                logger.warning(
                    "Agent not found during heartbeat, attempting re-registration"
                )
                if await self.register():
                    # Retry heartbeat after re-registration
                    return await self.heartbeat(status, job_id, metrics)
                return None
            else:
                logger.error(
                    f"Failed to send heartbeat: {response.status_code} - {response.text}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for heartbeat: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during heartbeat: {e}")
            return None

    async def run_heartbeat_loop(
        self,
        get_status: Callable[[], str],
        get_job_id: Callable[[], Optional[str]],
        get_metrics: Callable[[], Optional[dict[str, Any]]],
        on_response: Optional[
            Callable[[dict[str, Any]], Optional[Awaitable[None]]]
        ] = None,
    ) -> None:
        """Run the heartbeat loop.

        Sends heartbeats at the configured interval until stopped.
        If not yet registered, attempts registration each interval before
        switching to heartbeat mode. This makes startup order irrelevant
        and recovers from transient registration failures.

        Args:
            get_status: Callback that returns current agent status
            get_job_id: Callback that returns current job ID or None
            get_metrics: Callback that returns metrics dict or None
            on_response: Optional callback invoked with each successful
                heartbeat response body (carries ``intents``). Used by
                callers that need to react to drain / version-upgrade
                hints. Sync or async.
        """
        logger.info(f"Starting heartbeat loop (interval: {self.heartbeat_interval}s)")
        self._stop_heartbeat.clear()

        while not self._stop_heartbeat.is_set():
            try:
                if not self.agent_id:
                    # Not registered yet — keep trying
                    if await self.register():
                        logger.info(
                            "Registration succeeded, switching to heartbeat mode"
                        )
                    else:
                        logger.warning(
                            "Registration attempt failed, will retry next interval"
                        )
                else:
                    status = get_status()
                    job_id = get_job_id()
                    metrics = get_metrics()

                    response = await self.heartbeat(status, job_id, metrics)
                    actor = self.runtime_actor
                    if (
                        response is not None
                        and actor is not None
                        and actor.caller_kind == "officer"
                    ):
                        # Credential maintenance is liveness work, not model
                        # choice. This call is normally a local no-op until the
                        # six-hour renewal margin, then refreshes before the
                        # 24-hour idle wall even if no privileged tool ran.
                        await self.maintain_runtime_actor(force=False)
                    if response is not None and on_response is not None:
                        try:
                            ret = on_response(response)
                            if asyncio.iscoroutine(ret):
                                await ret
                        except Exception:
                            logger.exception("on_response callback raised")

            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")

            # Wait for interval or stop signal
            try:
                await asyncio.wait_for(
                    self._stop_heartbeat.wait(),
                    timeout=self.heartbeat_interval,
                )
                break  # Stop signal received
            except asyncio.TimeoutError:
                pass  # Continue loop

        logger.info("Heartbeat loop stopped")

    def stop_heartbeat(self) -> None:
        """Signal the heartbeat loop to stop."""
        self._stop_heartbeat.set()

    async def get_upload_info(self, upload_id: str) -> Optional[UploadInfo]:
        """Get information about an upload from the orchestrator.

        Args:
            upload_id: Upload identifier

        Returns:
            UploadInfo with file list and metadata, or None if not found/error
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/uploads/{upload_id}"

        try:
            response = await self._client.get(url)

            if response.status_code == 200:
                data = response.json()
                return UploadInfo(**data)
            elif response.status_code == 404:
                logger.debug(f"Upload not found on orchestrator: {upload_id}")
                return None
            else:
                logger.warning(
                    f"Failed to get upload info: {response.status_code} - {response.text}"
                )
                return None

        except httpx.RequestError as e:
            logger.warning(f"Failed to connect to orchestrator for upload info: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error getting upload info: {e}")
            return None

    async def download_file(self, upload_id: str, filename: str) -> Optional[bytes]:
        """Download a file from an upload on the orchestrator.

        Uses streaming to handle large files efficiently.

        Args:
            upload_id: Upload identifier
            filename: Name of the file to download

        Returns:
            File contents as bytes, or None if not found/error
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/uploads/{upload_id}/files/{filename}"

        try:
            # Use streaming for large file support
            async with self._client.stream("GET", url) as response:
                if response.status_code == 200:
                    chunks = []
                    async for chunk in response.aiter_bytes():
                        chunks.append(chunk)
                    content = b"".join(chunks)
                    logger.debug(
                        f"Downloaded {filename} from {upload_id} ({len(content)} bytes)"
                    )
                    return content
                elif response.status_code == 404:
                    logger.debug(
                        f"File not found on orchestrator: {upload_id}/{filename}"
                    )
                    return None
                else:
                    logger.warning(f"Failed to download file: {response.status_code}")
                    return None

        except httpx.RequestError as e:
            logger.warning(f"Failed to connect to orchestrator for file download: {e}")
            return None
        except Exception as e:
            logger.warning(f"Unexpected error downloading file: {e}")
            return None

    async def save_citation_snapshot(
        self, data: bytes, content_type: str = "application/octet-stream"
    ) -> Optional[str]:
        """Persist a cited cloud document's original bytes to the snapshot store.

        Phase 3 (D7): the agent has no S3 credentials, so the original bytes are
        round-tripped through the orchestrator (``POST /api/citations/snapshot``,
        internal-key auth), which content-addresses them and returns a
        ``snapshot_blob_key``. Best-effort — returns the key, or ``None`` on any
        failure (citation integrity rests on the extracted-text copy, not this
        blob).

        Args:
            data: Raw original file bytes.
            content_type: MIME type to store the blob under (for "view original").

        Returns:
            The content-addressed snapshot key, or None on failure.
        """
        if not data:
            return None
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/citations/snapshot"
        try:
            response = await self._client.post(
                url,
                content=data,
                params={"content_type": content_type},
                headers={"Content-Type": "application/octet-stream"},
            )
        except httpx.RequestError as e:
            logger.warning(f"Citation snapshot upload failed (network): {e}")
            return None

        if response.status_code == 200:
            return response.json().get("snapshot_blob_key")
        logger.warning(
            f"Citation snapshot upload rejected: {response.status_code} - "
            f"{response.text[:200]}"
        )
        return None

    async def record_verification_round(
        self,
        target_job_id: str,
        critic_job_id: str,
        asserted_verdict: str,
        opened: list,
        dispositions: list,
        head_commit: Optional[str] = None,
        content_tree: Optional[str] = None,
    ) -> dict[str, Any]:
        """Durably record this round and return the SERVER-COMPUTED verdict.

        Journal-before-observe: the tool must not return to the model until the
        round is committed. Raises VerdictRecordingError on any failure —
        including a 409, whose ``errors`` list is model-facing and must be
        surfaced verbatim so the model can correct itself.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{target_job_id}/verification/rounds"
        payload = {
            "critic_job_id": critic_job_id,
            "asserted_verdict": asserted_verdict,
            "opened": opened,
            "dispositions": dispositions,
            "head_commit": head_commit,
            "content_tree": content_tree,
        }
        try:
            response = await self._client.post(url, json=payload)
        except httpx.RequestError as e:
            raise VerdictRecordingError(f"network error: {e}") from e

        if response.status_code == 200:
            return response.json()
        if response.status_code == 409:
            detail = response.json().get("detail", {})
            errors = detail.get("errors") if isinstance(detail, dict) else None
            if isinstance(detail, dict) and detail.get("escalated"):
                # The orchestrator hit the per-critic rejection cap and
                # escalated the target to a human — the usual "correct and
                # resubmit" instruction must become a stop order or the
                # critic livelocks (rejected_verdict_livelocks_critic_and_
                # wedges_parent.md).
                err = VerdictRecordingError(
                    "verdict rejected and the review has been escalated to a "
                    "human after repeated invalid submissions:\n- "
                    + "\n- ".join(errors or [str(detail)])
                )
                err.escalated = True
                raise err
            raise VerdictRecordingError(
                "verdict rejected:\n- " + "\n- ".join(errors or [str(detail)])
            )
        raise VerdictRecordingError(
            f"HTTP {response.status_code}: {response.text[:200]}"
        )

    async def record_completion_decision(
        self,
        job_id: str,
        tool_call_id: str,
        summary: str,
        deliverables: list,
        confidence: float,
        notes: Optional[str] = None,
    ) -> dict[str, Any]:
        """Durably journal the job_complete decision before the tool returns.

        Journal-before-observe: the sibling of ``record_verification_round``
        for the worker's own terminating decision. Idempotent on
        ``(job_id, tool_call_id)`` — a ToolNode re-execution of the same call
        after a checkpoint gap returns the stored record (``replay: true``).
        Raises CompletionDecisionError on any failure so the tool reports
        NOT-recorded to the model instead of a false success.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/completion-decision"
        payload = {
            "tool_call_id": tool_call_id,
            "summary": summary,
            "deliverables": deliverables,
            "confidence": confidence,
            "notes": notes,
        }
        try:
            response = await self._client.post(url, json=payload)
        except httpx.RequestError as e:
            raise CompletionDecisionError(f"network error: {e}") from e

        if response.status_code == 200:
            return response.json()
        raise CompletionDecisionError(
            f"HTTP {response.status_code}: {response.text[:200]}"
        )

    async def fetch_completion_decision(self, job_id: str) -> Optional[dict[str, Any]]:
        """Read back the journaled job_complete decision, or None.

        Best-effort by design (unlike the write path): used by resume
        hydration, where a fetch failure must degrade to "no cached decision"
        — the durable record still exists and the graph-state mirror /
        model re-decision paths remain.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/completion-decision"
        try:
            response = await self._client.get(url)
            if response.status_code == 200:
                decision = response.json().get("decision")
                return decision if isinstance(decision, dict) else None
            logger.warning(
                f"Completion-decision fetch for {job_id} returned "
                f"HTTP {response.status_code}"
            )
        except httpx.RequestError as e:
            logger.warning(f"Completion-decision fetch for {job_id} failed: {e}")
        return None

    async def resume_job(
        self,
        job_id: str,
        feedback: Optional[str] = None,
    ) -> bool:
        """Resume a job via the orchestrator API.

        Used to resume the target job with critic feedback, or to resume
        a waiting critic job for another review round.

        If no agent is available, the orchestrator queues the job for
        auto-dispatch (returns 200 with status "queued").

        Args:
            job_id: UUID of the job to resume
            feedback: Optional feedback to inject before resuming

        Returns:
            True if resume succeeded or queued, False otherwise
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/resume"
        payload: dict[str, Any] = {}
        if feedback:
            payload["feedback"] = feedback

        try:
            response = await self._client.post(url, json=payload)

            if response.status_code in (200, 202):
                data = (
                    response.json()
                    if response.headers.get("content-type", "").startswith(
                        "application/json"
                    )
                    else {}
                )
                status = data.get("status", "resumed")
                if status == "queued":
                    logger.info(
                        f"Job {job_id} queued for auto-dispatch (no agents available)"
                    )
                else:
                    logger.info(f"Resumed job {job_id} via orchestrator")
                return True
            else:
                logger.error(
                    f"Failed to resume job {job_id}: {response.status_code} - {response.text}"
                )
                return False

        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for resume: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error resuming job: {e}")
            return False

    async def trigger_subjob_merge(self, job_id: str) -> bool:
        """Trigger squash merge of a completed subjob via the orchestrator.

        Args:
            job_id: UUID of the completed subjob

        Returns:
            True if merge succeeded or was skipped, False on error
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/subjob-merge"

        try:
            response = await self._client.post(url)

            if response.status_code == 200:
                data = response.json()
                logger.info(
                    f"Subjob merge for {job_id}: {data.get('status', 'unknown')}"
                )
                return True
            else:
                logger.error(
                    f"Subjob merge failed for {job_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return False

        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for subjob merge: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error triggering subjob merge: {e}")
            return False

    async def report_pause(
        self,
        job_id: str,
        *,
        lease_token: int | None = None,
    ) -> bool:
        """Report that the agent is releasing a job (e.g. during graceful shutdown).

        Tells the orchestrator to set the job to 'paused' and clear its agent
        assignment so the dispatcher can reassign it.  Uses the agent-initiated
        pause endpoint which skips the agent-callback loop (since we *are* the
        agent).

        Args:
            job_id: UUID of the job to pause

        Returns:
            True if the orchestrator accepted the pause, False otherwise.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/agent-release"

        try:
            params: dict[str, str | int] = {}
            if self.agent_id:
                params["agent_id"] = self.agent_id
            if lease_token is not None:
                params["lease_token"] = lease_token
            response = await self._client.put(url, params=params, timeout=10.0)
            if response.status_code == 200:
                logger.info(f"Orchestrator accepted agent-release for job {job_id}")
                return True
            else:
                logger.warning(
                    f"Agent-release failed for job {job_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.warning(f"Failed to report agent-release for job {job_id}: {e}")
            return False

    async def report_completion(
        self,
        job_id: str,
        result: dict[str, Any],
        lease_token: int | None = None,
        client_report_id: str | None = None,
        agent_id: str | None = None,
    ) -> bool:
        """Report job completion to the orchestrator.

        The orchestrator is the single authority for DB status. It handles
        all post-completion logic: status determination, freeze_data persistence,
        critic verdict handling, verification job spawning, curation, and dispatch.

        Args:
            job_id: UUID of the completed job
            result: Final graph state (should_stop, goal_achieved, error, freeze_data)
            lease_token: Current worker_batch fence for stateless jobs. Pinned
                callers omit it and retain the historical payload byte shape.
            client_report_id: Optional idempotency identity. When omitted, a
                checkpointed graph envelope supplies it if present.
            agent_id: Optional pinned-lane ownership fence. Stateless callers
                use ``lease_token`` instead.

        Returns:
            True if the orchestrator handled completion successfully.

        Raises:
            CompletionNonTerminalReportError: The exact machine-coded 422
                proves the stateless payload was refused before any write.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/complete"
        checkpointed_payload = result.get("completion_report_payload")
        if isinstance(checkpointed_payload, dict) and set(checkpointed_payload) == set(
            _COMPLETION_REPORT_PAYLOAD_FIELDS
        ):
            # Never re-derive a retry payload. Freeze data can embed timestamps
            # and repository state, so the checkpointed four-field operation
            # envelope is the only safe source after a failed HTTP attempt.
            payload: dict[str, Any] = {
                field: checkpointed_payload[field]
                for field in _COMPLETION_REPORT_PAYLOAD_FIELDS
            }
            if client_report_id is None:
                stored_report_id = result.get("client_report_id")
                if stored_report_id is not None:
                    client_report_id = str(stored_report_id)
        else:
            # Backwards compatibility for old checkpoints and direct callers.
            payload = {
                "should_stop": result.get("should_stop", False),
                "goal_achieved": result.get("goal_achieved", False),
                "error": result.get("error"),
                "freeze_data": result.get("freeze_data"),
            }
        if lease_token is not None:
            payload["lease_token"] = int(lease_token)
        if agent_id is not None:
            payload["agent_id"] = str(agent_id)
        if client_report_id is not None:
            payload["client_report_id"] = str(client_report_id)
        if getattr(self, "pinned_delivery_job_id", None) == job_id:
            payload.update({
                "pinned_delivery_id": getattr(self, "pinned_delivery_id", None),
                "pinned_projection_digest": getattr(self, "pinned_projection_digest", None),
                "pinned_delivery_proof": getattr(self, "pinned_delivery_proof", None),
                "pinned_process_generation": self.dispatch_process_generation,
                "pinned_pod_uid": os.environ.get("POD_UID"),
            })

        try:
            # Stateless terminal handling can include workspace/archive and
            # verification side effects. A short client disconnect cancels the
            # FastAPI handler, so the leased path gets a deliberately wide
            # budget. Pinned retains its historical 60-second behavior.
            timeout_seconds = 300.0 if lease_token is not None else 60.0
            response = await self._client.post(
                url, json=payload, timeout=timeout_seconds
            )
            if response.status_code in {200, 202}:
                try:
                    resp_data = response.json()
                except ValueError:
                    # A body is useful for logging but not part of the durable
                    # acceptance contract; a bodyless 202 is still success.
                    resp_data = {}
                actions = resp_data.get("actions", [])
                logger.info(
                    f"Orchestrator accepted completion for job {job_id}: "
                    f"status={resp_data.get('new_status')}, actions={actions}"
                )
                return True
            elif response.status_code == 422:
                try:
                    response_payload = response.json()
                except (TypeError, ValueError):
                    response_payload = None
                detail = (
                    response_payload.get("detail")
                    if isinstance(response_payload, dict)
                    else None
                )
                if (
                    isinstance(detail, dict)
                    and detail.get("code") == CompletionNonTerminalReportError.code
                ):
                    message = detail.get("message")
                    raise CompletionNonTerminalReportError(
                        message if isinstance(message, str) else ""
                    )
                logger.warning(
                    f"Completion report failed for job {job_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return False
            elif response.status_code == 404:
                logger.info(
                    "Orchestrator does not support /complete endpoint — "
                    "falling back to local handling"
                )
                return False
            else:
                logger.warning(
                    f"Completion report failed for job {job_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return False

        except CompletionNonTerminalReportError:
            raise
        except httpx.RequestError as e:
            logger.warning(f"Failed to report completion for job {job_id}: {e}")
            return False
        except Exception as e:
            logger.warning(
                f"Unexpected error reporting completion for job {job_id}: {e}"
            )
            return False

    async def ack_job_guidance(
        self,
        job_id: str,
        guidance_ids: list[str] | None = None,
        reply_threads: list[str] | None = None,
        reply_keys: list[str] | None = None,
        feedback_keys: list[str] | None = None,
        delegation_keys: list[str] | None = None,
        checkpoint_id: str | None = None,
    ) -> bool:
        """Ack delivered supervisor guidance / drained queued replies.

        The orchestrator atomically moves the named entries from
        ``context.pending_guidance`` (by entry id) and
        ``context.queued_replies`` (by exact key for stateless workers, thread
        id for legacy pinned callers) to ``context.consumed_replies``. A
        stateless caller supplies the committed checkpoint proving the entries
        reached durable graph state. Best-effort: failure means redelivery.

        Args:
            job_id: UUID of the job the guidance was delivered to
            guidance_ids: ``pending_guidance`` entry ids rendered into context
            reply_threads: thread ids whose queued replies were drained
            reply_keys: exact queued-reply identities absorbed by a checkpoint
            feedback_keys: exact queued-feedback generations absorbed
            delegation_keys: exact delegation-result generations absorbed
            checkpoint_id: durable checkpoint proving stateless delivery

        Returns:
            True if the orchestrator recorded the ack.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/guidance/ack"
        payload = {
            "guidance_ids": guidance_ids or [],
            "reply_threads": reply_threads or [],
            "reply_keys": reply_keys or [],
            "feedback_keys": feedback_keys or [],
            "delegation_keys": delegation_keys or [],
            "checkpoint_id": checkpoint_id,
        }

        try:
            response = await self._client.post(url, json=payload)
            if response.status_code == 200:
                return True
            logger.warning(
                f"Guidance ack failed for job {job_id}: "
                f"{response.status_code} - {response.text}"
            )
            return False
        except httpx.RequestError as e:
            logger.warning(f"Failed to ack guidance for job {job_id}: {e}")
            return False

    async def approve_job(
        self,
        job_id: str,
        notes: str | None = None,
    ) -> bool:
        """Approve a frozen job via the orchestrator.

        Args:
            job_id: UUID of the job to approve
            notes: Optional reviewer notes

        Returns:
            True if the orchestrator approved successfully.
        """
        if not self._client:
            await self.connect()

        url = f"{self.orchestrator_url}/api/jobs/{job_id}/approve"
        payload: dict[str, Any] = {}
        if notes:
            payload["notes"] = notes

        try:
            response = await self._client.put(url, json=payload, timeout=30.0)
            if response.status_code == 200:
                logger.info(f"Job {job_id} approved via orchestrator")
                return True
            else:
                logger.warning(
                    f"Approval failed for job {job_id}: "
                    f"{response.status_code} - {response.text}"
                )
                return False
        except Exception as e:
            logger.warning(f"Failed to approve job {job_id}: {e}")
            return False

    async def create_verification_job(
        self,
        job_id: str,
        description: str,
        freeze_data: dict[str, Any],
        config_name: str,
        project_id: Optional[str] = None,
        max_rounds: int = 3,
        parent_llm_override: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        """Create a critic verification job for a completed job.

        Datasource IDs are intentionally omitted: the job endpoint inherits
        and reauthorizes the target parent job's materialized selection.

        Loads the verification instructions template, formats it with
        target job details, and creates a new critic job via the
        orchestrator API.

        Args:
            job_id: UUID of the target job being verified
            description: Original job description
            freeze_data: Freeze data from the target job (has summary, deliverables, confidence)
            config_name: Critic config name to use (e.g., "critic")
            project_id: Optional project UUID (inherit from parent job)
            max_rounds: Maximum feedback round-trips before auto-accepting

        Returns:
            Created job dict from orchestrator, or None on failure
        """
        if not self._client:
            await self.connect()

        # Load and format the verification instructions template
        instructions = self._format_verification_instructions(
            job_id,
            description,
            freeze_data,
            config_name,
        )
        if not instructions:
            logger.error(
                f"Failed to load verification instructions template for job {job_id}"
            )
            return None

        # Build the job creation payload
        verification_description = (
            f"Verify deliverables of job {job_id} ({config_name}). "
            f"Review output against original requirements and either approve or return with feedback."
        )

        context = {
            "verification_target": job_id,
            "original_description": description,
            "original_config": config_name,
            "deliverables": freeze_data.get("deliverables", []),
            "summary": freeze_data.get("summary", ""),
            "confidence": freeze_data.get("confidence", 0),
            "verification_round": 0,
            "max_verification_rounds": max_rounds,
        }

        payload: dict[str, Any] = {
            "description": verification_description,
            "config_name": freeze_data.get("critic_config", "critic"),
            "config_override": {
                "autonomy": "full",  # Critic must run autonomously
                "tools": {
                    "evaluation": [
                        "approve_job_verdict",
                        "return_job_with_feedback",
                    ],
                },
                **({"llm": parent_llm_override} if parent_llm_override else {}),
            },
            "instructions": instructions,
            "context": context,
            "parent_job_id": job_id,
            "priority": 10,  # High priority — verification preempts lower-priority work
        }
        if project_id:
            payload["project_id"] = project_id

        url = f"{self.orchestrator_url}/api/jobs"

        try:
            response = await self._client.post(url, json=payload)

            if response.status_code == 200:
                result = response.json()
                critic_job_id = result.get("id", "unknown")
                logger.info(
                    f"Created verification job {critic_job_id} for target job {job_id}"
                )
                return result
            else:
                logger.error(
                    f"Failed to create verification job: {response.status_code} - {response.text}"
                )
                return None

        except httpx.RequestError as e:
            logger.error(f"Failed to connect to orchestrator for verification job: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating verification job: {e}")
            return None

    @staticmethod
    def _format_verification_instructions(
        job_id: str,
        description: str,
        freeze_data: dict[str, Any],
        config_name: str,
    ) -> Optional[str]:
        """Load and format the verification instructions template.

        Args:
            job_id: Target job UUID
            description: Original job description
            freeze_data: Freeze data with summary, deliverables, confidence
            config_name: Original job's config name

        Returns:
            Formatted instructions string, or None if template not found
        """
        from pathlib import Path

        # Look for template in critic expert directory, then fall back to config/templates
        template_path = None
        search_paths = [
            Path(__file__).resolve().parents[3]
            / "config"
            / "experts"
            / "critic"
            / "verification_instructions.md",
            Path(__file__).resolve().parents[3]
            / "config"
            / "templates"
            / "verification_instructions.md",
        ]

        for path in search_paths:
            if path.exists():
                template_path = path
                break

        if not template_path:
            logger.error(
                f"Verification instructions template not found. "
                f"Searched: {[str(p) for p in search_paths]}"
            )
            return None

        try:
            template = template_path.read_text(encoding="utf-8")
        except Exception as e:
            logger.error(f"Failed to read verification template {template_path}: {e}")
            return None

        # Format deliverables as a bulleted list
        deliverables = freeze_data.get("deliverables", [])
        if deliverables:
            deliverables_list = "\n".join(f"- `{d}`" for d in deliverables)
        else:
            deliverables_list = "- *(no deliverables listed)*"

        confidence = freeze_data.get("confidence", 0)
        confidence_str = (
            f"{confidence:.0%}"
            if isinstance(confidence, (int, float))
            else str(confidence)
        )

        try:
            return template.format(
                target_job_id=job_id,
                target_config=config_name,
                target_description=description,
                deliverables_list=deliverables_list,
                agent_summary=freeze_data.get("summary", "*(no summary provided)*"),
                agent_confidence=confidence_str,
            )
        except KeyError as e:
            logger.error(f"Verification template has unknown placeholder: {e}")
            return None


def create_orchestrator_client_from_env(
    config_name: str, *, user_id: str | None = None
) -> OrchestratorClient:
    """Create an OrchestratorClient from environment variables.

    Optional environment variables:
        ORCHESTRATOR_URL: Base URL of orchestrator service (default: http://localhost:8085)
        AGENT_POD_IP: IP address (auto-detected if not set)
        AGENT_POD_PORT: API port (default 8001)
        AGENT_HOSTNAME: Hostname (auto-detected if not set)

    Args:
        config_name: Agent configuration name
        user_id: Optional delegated persistent-session owner UUID. When set,
            the client sends it alongside the internal service credential.

    Returns:
        OrchestratorClient configured from environment
    """
    orchestrator_url = os.getenv("ORCHESTRATOR_URL", "http://localhost:8085")

    pod_ip = get_agent_ip()
    pod_port = int(os.getenv("AGENT_POD_PORT", "8001"))
    hostname = get_hostname()

    logger.info(
        f"Creating orchestrator client: url={orchestrator_url}, "
        f"pod_ip={pod_ip}, port={pod_port}, hostname={hostname}"
    )

    return OrchestratorClient(
        orchestrator_url=orchestrator_url,
        pod_ip=pod_ip,
        pod_port=pod_port,
        hostname=hostname,
        config_name=config_name,
        user_id=user_id,
    )
