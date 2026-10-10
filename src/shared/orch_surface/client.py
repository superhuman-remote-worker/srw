"""HTTP client for the Cockpit API.

Provides synchronous and asynchronous methods to interact with the debug cockpit's REST API.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
import functools
import os
import ssl
from types import MappingProxyType
from typing import Any, Iterator, Literal, Mapping
from uuid import UUID

import httpx

from shared.runtime_actor import (
    RUNTIME_ACTOR_HEADER,
    RUNTIME_ACTOR_REFRESH_HEADER,
    RuntimeActorContext,
)

FilterCategory = Literal["all", "messages", "tools", "errors"]
DatasourceScopeMode = Literal["all", "projects"]
DatasourceVisibility = Literal["public", "private"]
DatasourceOwnership = Literal["mine", "shared"]
DatasourceAvailability = Literal["all", "projects", "unavailable"]


def _create_safe_read_retry_decorator():
    """Retry transport failures only for operations known to be safe reads."""

    def decorate(function):
        @functools.wraps(function)
        async def retry_safe_read(*args: Any, **kwargs: Any):
            for attempt in range(3):
                try:
                    return await function(*args, **kwargs)
                except (httpx.ConnectError, httpx.TimeoutException):
                    if attempt == 2:
                        raise
                    await asyncio.sleep(2**attempt)

        return retry_safe_read

    return decorate


# Kept as a local spelling for the existing GET wrappers. Mutation methods are
# intentionally undecorated and route through ``_mutation_request`` below.
_create_retry_decorator = _create_safe_read_retry_decorator


def _as_jobs_envelope(payload: Any) -> dict[str, Any]:
    """Normalise a ``/api/jobs`` response into the paging envelope.

    The endpoint returns an envelope; a bare array means we are talking to an
    orchestrator older than that change. Wrapping it here keeps a lagging
    deploy degraded-but-correct instead of iterating dict keys and rendering
    "Found 8 job(s)" over the *field names* — the failure mode this shape
    change is most likely to produce, and a silent one.
    """
    if isinstance(payload, dict):
        payload.setdefault("jobs", [])
        return payload
    rows = list(payload or [])
    return {
        "jobs": rows,
        "total": len(rows),
        "total_is_capped": False,
        "has_more": False,
        "limit": len(rows),
        "offset": 0,
        "filters": {},
    }


class MutationOutcomeUnknown(RuntimeError):
    """A mutation may have committed, but its response was not received.

    Mutations are deliberately never retried here. Callers must verify the
    resource through a read operation before deciding whether to try again.
    """

    def __init__(self, method: str, path: str):
        super().__init__(
            f"Outcome unknown: {method} {path} may have been applied; the MCP "
            "client did not retry it. Verify current state with a read tool "
            "before acting again."
        )
        self.method = method
        self.path = path


class SessionConfigDriftError(Exception):
    """Resume refused: parts of the session's stored config are unavailable.

    The message carries each item's ID as well as its label, because callers
    that render only ``str(error)`` (the MCP tool path) would otherwise have
    no way to build the ``acknowledge`` list the message tells them to send.
    """

    def __init__(self, drift: list[dict[str, Any]]):
        self.drift = drift
        self.ids = [
            str(item.get("id"))
            for item in drift
            if isinstance(item, dict) and item.get("id")
        ]
        described = (
            ", ".join(
                f"{item.get('label') or item.get('id') or '?'} ({item.get('id')})"
                for item in drift
                if isinstance(item, dict)
            )
            or "unknown items"
        )
        super().__init__(
            f"Session config is no longer fully available: {described}. "
            f"Resume again with acknowledge={self.ids!r} to continue without them."
        )


class _RequestScopeAuth(httpx.Auth):
    """Copy the current task's MCP identity onto one outgoing request.

    The underlying ``httpx.AsyncClient`` is process-wide for connection
    pooling, so its default headers must never carry caller identity. A
    ``ContextVar`` keeps concurrent MCP calls isolated and the auth flow takes
    a fresh immutable snapshot as each request is built.
    """

    _HEADER_NAMES = (
        "X-MCP-User-Id",
        "X-MCP-Scope",
        "X-Internal-Key",
        RUNTIME_ACTOR_HEADER,
        RUNTIME_ACTOR_REFRESH_HEADER,
    )

    def __init__(self, headers: ContextVar[Mapping[str, str] | None]):
        self._headers = headers

    def auth_flow(self, request: httpx.Request):
        for name in self._HEADER_NAMES:
            request.headers.pop(name, None)
        for name, value in (self._headers.get() or {}).items():
            request.headers[name] = value
        yield request


class _BearerOnlyAuth(httpx.Auth):
    """Explicit human/client identity cannot inherit internal MCP authority."""

    def __init__(self, token: str):
        if (
            not token
            or len(token) > 65536
            or any(ord(char) < 33 or ord(char) > 126 for char in token)
        ):
            raise ValueError(
                "Bearer credentials must be a nonempty token without whitespace."
            )
        self._token = token

    def auth_flow(self, request: httpx.Request):
        for name in _RequestScopeAuth._HEADER_NAMES:
            request.headers.pop(name, None)
        request.headers["Authorization"] = "Bearer " + self._token
        yield request


class CockpitClient:
    """HTTP client for the cockpit API."""

    def __init__(self, base_url: str | None = None):
        """Initialize the client.

        Args:
            base_url: Cockpit API URL. Defaults to COCKPIT_API_URL env var
                      or http://localhost:8085.
        """
        self.base_url = base_url or os.environ.get(
            "COCKPIT_API_URL", "http://localhost:8085"
        )
        self._client = httpx.Client(base_url=self.base_url, timeout=30.0)

    def close(self) -> None:
        """Close the HTTP client."""
        self._client.close()

    def __enter__(self) -> CockpitClient:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # =========================================================================
    # Health
    # =========================================================================

    def health_check(self) -> dict[str, str]:
        """Check API health status."""
        resp = self._client.get("/api/health")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Jobs
    # =========================================================================

    def list_jobs(
        self,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """List jobs with optional status filter.

        Args:
            status: Filter by lifecycle status, including pending_review and paused
            limit: Maximum number of jobs to return (1-500)

        Returns:
            The ``/api/jobs`` envelope: ``jobs``, ``total``,
            ``total_is_capped``, ``has_more``, ``limit``, ``offset``,
            ``as_of`` and a ``filters`` echo. Read the rows off ``["jobs"]``.
        """
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = self._client.get("/api/jobs", params=params)
        resp.raise_for_status()
        return _as_jobs_envelope(resp.json())

    def get_job(self, job_id: str) -> dict[str, Any]:
        """Get a single job by ID.

        Args:
            job_id: Job UUID

        Returns:
            Job dict with full details
        """
        resp = self._client.get(f"/api/jobs/{job_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Audit Trail
    # =========================================================================

    def get_audit_trail(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
        filter_category: FilterCategory = "all",
    ) -> dict[str, Any]:
        """Get paginated audit entries for a job.

        Args:
            job_id: Job UUID
            page: Page number (1-indexed, -1 for last page)
            page_size: Entries per page (1-200)
            filter_category: Filter type (all, messages, tools, errors)

        Returns:
            Dict with entries, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_category,
        }
        resp = self._client.get(f"/api/jobs/{job_id}/audit", params=params)
        resp.raise_for_status()
        return resp.json()

    def get_audit_time_range(self, job_id: str) -> dict[str, str] | None:
        """Get first and last timestamps for job audit entries.

        Args:
            job_id: Job UUID

        Returns:
            Dict with start and end ISO timestamps, or None
        """
        resp = self._client.get(f"/api/jobs/{job_id}/audit/timerange")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Chat History
    # =========================================================================

    def get_chat_history(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """Get paginated chat history for a job.

        Args:
            job_id: Job UUID
            page: Page number (1-indexed, -1 for last page)
            page_size: Entries per page (1-200)

        Returns:
            Dict with entries, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
        }
        resp = self._client.get(f"/api/jobs/{job_id}/chat", params=params)
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Todos
    # =========================================================================

    def get_todos(self, job_id: str) -> dict[str, Any]:
        """Get all todos for a job (current + archives).

        Args:
            job_id: Job UUID

        Returns:
            Dict with job_id, current, archives, has_workspace
        """
        resp = self._client.get(f"/api/jobs/{job_id}/todos")
        resp.raise_for_status()
        return resp.json()

    def get_current_todos(self, job_id: str) -> dict[str, Any] | None:
        """Get current active todos from todos.yaml.

        Args:
            job_id: Job UUID

        Returns:
            Dict with todos list and metadata, or None if not found
        """
        resp = self._client.get(f"/api/jobs/{job_id}/todos/current")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    def list_archived_todos(self, job_id: str) -> list[dict[str, Any]]:
        """List all archived todo files for a job.

        Args:
            job_id: Job UUID

        Returns:
            List of archive metadata (filename, phase_name, timestamp)
        """
        resp = self._client.get(f"/api/jobs/{job_id}/todos/archives")
        resp.raise_for_status()
        return resp.json()

    def get_archived_todos(self, job_id: str, filename: str) -> dict[str, Any] | None:
        """Get parsed content of an archived todo file.

        Args:
            job_id: Job UUID
            filename: Archive filename (e.g., todos_phase1_20260124_183618.md)

        Returns:
            Dict with parsed todos, summary, and metadata, or None if not found
        """
        resp = self._client.get(f"/api/jobs/{job_id}/todos/archives/{filename}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Graph Changes
    # =========================================================================

    def get_graph_changes(self, job_id: str) -> dict[str, Any]:
        """Get parsed graph changes for timeline visualization.

        Args:
            job_id: Job UUID

        Returns:
            Dict with jobId, timeRange, summary, snapshots, deltas
        """
        resp = self._client.get(f"/api/graph/changes/{job_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # LLM Requests
    # =========================================================================

    def get_llm_request(self, doc_id: str) -> dict[str, Any]:
        """Get a single LLM request by its audit-store request ID.

        Args:
            doc_id: Audit-store request ID (string)

        Returns:
            Full LLM request document with messages and response
        """
        resp = self._client.get(f"/api/requests/{doc_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Tables (for debugging)
    # =========================================================================

    # =========================================================================
    # Job Actions (mutations)
    # =========================================================================

    def approve_job(self, job_id: str) -> dict[str, Any]:
        """Approve a frozen job, marking it as completed."""
        resp = self._client.post(f"/api/jobs/{job_id}/approve")
        resp.raise_for_status()
        return resp.json()

    def resume_job(self, job_id: str, feedback: str | None = None) -> dict[str, Any]:
        """Resume a job from its checkpoint with optional feedback."""
        body: dict[str, Any] = {}
        if feedback:
            body["feedback"] = feedback
        resp = self._client.post(
            f"/api/jobs/{job_id}/resume",
            json=body if body else None,
        )
        resp.raise_for_status()
        return resp.json()

    def cancel_job(self, job_id: str) -> dict[str, Any]:
        """Cancel a running job."""
        resp = self._client.put(f"/api/jobs/{job_id}/cancel")
        resp.raise_for_status()
        return resp.json()

    def create_job(
        self,
        description: str,
        config_name: str = "worker_base",
        datasource_ids: list[str] | None = None,
        instructions: str | None = None,
        config_override: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        required_deliverables: list[str] | None = None,
    ) -> dict[str, Any]:
        """Create a new job.

        ``datasource_ids`` is tri-state: omit it to let the server resolve
        inheritance/defaults, pass ``[]`` to attach no connectors, or pass IDs
        to use exactly that authorized selection.
        """
        body: dict[str, Any] = {
            "description": description,
            "config_name": config_name,
        }
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        else:
            body["use_datasource_defaults"] = True
        if instructions:
            body["instructions"] = instructions
        if config_override:
            body["config_override"] = config_override
        if context:
            body["context"] = context
        if required_deliverables:
            body["required_deliverables"] = required_deliverables
        resp = self._client.post("/api/jobs", json=body)
        resp.raise_for_status()
        return resp.json()

    def delete_job(self, job_id: str) -> dict[str, Any]:
        """Delete a job and its associated data."""
        resp = self._client.delete(f"/api/jobs/{job_id}")
        resp.raise_for_status()
        return resp.json()

    def assign_job(self, job_id: str, agent_id: str) -> dict[str, Any]:
        """Request the admin assignment override (may queue provisioning)."""
        resp = self._client.post(f"/api/jobs/{job_id}/assign/{agent_id}")
        resp.raise_for_status()
        return resp.json()

    def test_datasource(self, datasource_id: str) -> dict[str, Any]:
        """Test connectivity to a datasource."""
        resp = self._client.post(f"/api/datasources/{datasource_id}/test")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Tables (for debugging)
    # =========================================================================

    def list_tables(self) -> list[dict[str, Any]]:
        """List available database tables with row counts."""
        resp = self._client.get("/api/tables")
        resp.raise_for_status()
        return resp.json()

    def get_table_data(
        self,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """Get paginated table data.

        Args:
            table_name: Table name (jobs, requirements, citations, etc.)
            page: Page number (1-indexed, -1 for last page)
            page_size: Rows per page (1-500)

        Returns:
            Dict with data, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
        }
        resp = self._client.get(f"/api/tables/{table_name}", params=params)
        resp.raise_for_status()
        return resp.json()


# =============================================================================
# Async Client
# =============================================================================


class AsyncCockpitClient:
    """Async HTTP client for the cockpit API with retry logic."""

    def __init__(
        self,
        base_url: str | None = None,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
        bearer_token: str | None = None,
        verify: bool | ssl.SSLContext = True,
        trust_env: bool = True,
    ):
        """Initialize the async client.

        Args:
            base_url: Cockpit API URL. Defaults to COCKPIT_API_URL env var
                      or http://localhost:8085.
        """
        self.base_url = base_url or os.environ.get(
            "COCKPIT_API_URL", "http://localhost:8085"
        )
        self._internal_key = (
            os.environ.get("MCP_INTERNAL_KEY", "") if bearer_token is None else ""
        )
        self._scope_headers: ContextVar[Mapping[str, str] | None] = ContextVar(
            f"mcp_scope_headers_{id(self)}", default=None
        )
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            auth=(
                _RequestScopeAuth(self._scope_headers)
                if bearer_token is None
                else _BearerOnlyAuth(bearer_token)
            ),
            transport=transport,
            verify=verify,
            trust_env=trust_env,
        )

    # F15: the imperative set_scope_headers/clear_scope_headers pair was
    # removed — invocation_scope() below is the one binding mechanism (it
    # restores the previous context instead of erasing an outer invocation).

    def _invocation_headers(
        self,
        *,
        user_id: str | None,
        scope: str | None,
        unauthenticated: bool = False,
        runtime_actor: RuntimeActorContext | None = None,
        runtime_actor_refresh: str | None = None,
    ) -> Mapping[str, str] | None:
        # ``unauthenticated`` is the explicit fail-closed binding for an
        # adapter whose auth context could not be resolved: NONE of the three
        # identity headers are attached — the internal key is deliberately
        # withheld, never an error fallback — so guarded orchestrator
        # endpoints 401. Anonymous callers WITHOUT the flag (stdio MCP,
        # worker-mode agent tools) keep their deliberate internal-key-only
        # contract below.
        if unauthenticated:
            return None
        headers: dict[str, str] = {}
        if self._internal_key:
            headers["X-Internal-Key"] = self._internal_key
        if user_id:
            headers["X-MCP-User-Id"] = user_id
        if scope:
            headers["X-MCP-Scope"] = scope
        if runtime_actor and runtime_actor.access_credential:
            headers[RUNTIME_ACTOR_HEADER] = runtime_actor.access_credential
        if runtime_actor_refresh:
            headers[RUNTIME_ACTOR_REFRESH_HEADER] = runtime_actor_refresh
        return MappingProxyType(headers) if headers else None

    @contextmanager
    def invocation_scope(
        self,
        *,
        user_id: str | None = None,
        scope: str | None = None,
        unauthenticated: bool = False,
        runtime_actor: RuntimeActorContext | None = None,
        runtime_actor_refresh: str | None = None,
    ) -> Iterator[None]:
        """Bind and reliably reset one invocation's identity/scope headers.

        The token returned by ``ContextVar.set`` is essential: setting ``None``
        in ``finally`` would erase an outer invocation's scope, while resetting
        restores the exact previous context. Separate asyncio tasks retain
        independent values even though they share this client's connection pool.

        ``unauthenticated=True`` binds the invocation with NO identity headers
        at all (no internal key, no user, no scope): the fail-closed shape for
        a caller whose auth context could not be resolved.
        """
        token = self._scope_headers.set(
            self._invocation_headers(
                user_id=user_id,
                scope=scope,
                unauthenticated=unauthenticated,
                runtime_actor=runtime_actor,
                runtime_actor_refresh=runtime_actor_refresh,
            )
        )
        try:
            yield
        finally:
            self._scope_headers.reset(token)

    async def ensure_runtime_actor(
        self, actor: RuntimeActorContext | None
    ) -> tuple[bool, str]:
        """Refresh a near-expiry actor token without exposing it to a schema."""

        if actor is None:
            return False, "server-derived actor context is missing"
        if not actor.access_needs_refresh():
            return True, "authorized credential is current"
        if not actor.refresh_credential:
            return False, "runtime actor refresh credential is missing"
        try:
            with self.invocation_scope(runtime_actor_refresh=actor.refresh_credential):
                response = await self._mutation_request(
                    "POST", "/api/runtime-actors/refresh"
                )
        except (
            httpx.RequestError,
            httpx.TimeoutException,
            MutationOutcomeUnknown,
        ) as exc:
            return False, f"actor refresh unavailable ({type(exc).__name__})"
        if response.status_code != 200:
            code = f"http-{response.status_code}"
            try:
                detail = response.json().get("detail")
                if isinstance(detail, dict) and isinstance(detail.get("code"), str):
                    code = detail["code"]
            except Exception:
                pass
            return False, f"actor refresh denied ({code})"
        try:
            payload = response.json().get("runtime_actor")
        except Exception:
            return False, "actor refresh returned malformed JSON"
        if not actor.apply_refreshed_payload(payload):
            return False, "actor refresh changed identity or was malformed"
        return True, "runtime actor refreshed"

    async def _mutation_request(
        self,
        method: Literal["POST", "PUT", "PATCH", "DELETE"],
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a mutation exactly once and surface ambiguous outcomes.

        A connect failure or pool timeout happens before a response is
        expected and is preserved as-is. Read/write timeouts and a broken
        response stream can happen after the orchestrator committed, so those
        are reported as unknown outcomes and are never retried.
        """
        request = getattr(self._client, method.lower())
        try:
            return await request(path, **kwargs)
        except (
            httpx.ReadTimeout,
            httpx.WriteTimeout,
            httpx.ReadError,
            httpx.WriteError,
            httpx.RemoteProtocolError,
        ) as exc:
            raise MutationOutcomeUnknown(method, path) from exc

    async def _non_get_read_request(
        self,
        method: Literal["POST"],
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Send a semantically read-only non-GET operation once."""
        request = getattr(self._client, method.lower())
        return await request(path, **kwargs)

    async def close(self) -> None:
        """Close the HTTP client."""
        await self._client.aclose()

    async def __aenter__(self) -> "AsyncCockpitClient":
        return self

    async def __aexit__(self, *args: Any) -> None:
        await self.close()

    # =========================================================================
    # Native manifests and canonical resources
    # =========================================================================

    async def manifest_validate(
        self, source: str, *, format: str = "yaml"
    ) -> dict[str, Any]:
        response = await self._non_get_read_request(
            "POST", "/api/manifests/validate", json={"source": source, "format": format}
        )
        response.raise_for_status()
        return response.json()

    async def manifest_preview(
        self,
        source: str,
        *,
        format: str = "yaml",
        default_scope: dict[str, str] | None = None,
        resolution: str = "stored",
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "source": source,
            "format": format,
            "resolution": resolution,
        }
        if default_scope is not None:
            body["default_scope"] = default_scope
        response = await self._non_get_read_request(
            "POST", "/api/manifests/preview", json=body
        )
        response.raise_for_status()
        return response.json()

    async def manifest_apply(
        self,
        source: str,
        *,
        format: str = "yaml",
        default_scope: dict[str, str] | None = None,
        expected_versions: dict[str, int] | None = None,
        plan_revision: str | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"source": source, "format": format}
        for key, value in (
            ("default_scope", default_scope),
            ("expected_versions", expected_versions),
            ("plan_revision", plan_revision),
            ("idempotency_key", idempotency_key),
        ):
            if value is not None:
                body[key] = value
        response = await self._mutation_request(
            "POST", "/api/manifests/apply", json=body
        )
        response.raise_for_status()
        return response.json()

    async def manifest_export(
        self,
        source: str,
        *,
        format: str = "yaml",
        default_scope: dict[str, str] | None = None,
        output_format: str = "yaml",
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "source": source,
            "format": format,
            "output_format": output_format,
        }
        if default_scope is not None:
            body["default_scope"] = default_scope
        response = await self._non_get_read_request(
            "POST", "/api/manifests/export", json=body
        )
        response.raise_for_status()
        return response.json()

    @_create_retry_decorator()
    async def list_manifest_resources(
        self,
        *,
        scope_kind: str = "Account",
        scope_name: str = "me",
        kind: str | None = None,
    ) -> dict[str, Any]:
        params = {"scope_kind": scope_kind, "scope_name": scope_name}
        if kind is not None:
            params["kind"] = kind
        response = await self._client.get("/api/resources", params=params)
        response.raise_for_status()
        return response.json()

    @_create_retry_decorator()
    async def get_manifest_resource(self, resource_id: str) -> dict[str, Any]:
        identity = str(UUID(str(resource_id)))
        response = await self._client.get(f"/api/resources/{identity}")
        response.raise_for_status()
        return response.json()

    async def delete_manifest_resource(
        self, resource_id: str, *, expected_version: int
    ) -> dict[str, Any]:
        identity = str(UUID(str(resource_id)))
        if (
            isinstance(expected_version, bool)
            or not isinstance(expected_version, int)
            or expected_version < 1
        ):
            raise ValueError("Resource deletion requires a positive expected version.")
        response = await self._mutation_request(
            "DELETE",
            f"/api/resources/{identity}",
            params={"expected_version": expected_version},
        )
        response.raise_for_status()
        return response.json()

    @_create_retry_decorator()
    async def list_workspace_cache(self, *, scope_kind="Account", scope_name="me"):
        response = await self._client.get(
            "/api/workspace-cache",
            params={"scope_kind": scope_kind, "scope_name": scope_name},
        )
        response.raise_for_status()
        return response.json()

    async def delete_workspace_cache(
        self, artifact_id, *, scope_kind="Account", scope_name="me"
    ):
        identity = str(UUID(str(artifact_id)))
        response = await self._mutation_request(
            "DELETE",
            f"/api/workspace-cache/{identity}",
            params={"scope_kind": scope_kind, "scope_name": scope_name},
        )
        response.raise_for_status()
        return response.json()

    async def export_manifest_resource(
        self, resource_id: str, *, output_format: str = "yaml"
    ) -> dict[str, Any]:
        import json

        record = await self.get_manifest_resource(resource_id)
        return await self.manifest_export(
            json.dumps(record["resource"], allow_nan=False),
            format="json",
            output_format=output_format,
        )

    # =========================================================================
    # Health
    # =========================================================================

    @_create_retry_decorator()
    async def health_check(self) -> dict[str, str]:
        """Check API health status."""
        resp = await self._client.get("/api/health")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Jobs
    # =========================================================================

    @_create_retry_decorator()
    async def list_jobs(
        self,
        status: str | None = None,
        limit: int = 100,
    ) -> dict[str, Any]:
        """List jobs with optional status filter.

        Args:
            status: Filter by status/outcome (created, processing, completed,
                failed, cancelled, blocked_undelivered, pending_review)
            limit: Maximum number of jobs to return (1-500)

        Returns:
            The ``/api/jobs`` envelope: ``jobs``, ``total``,
            ``total_is_capped``, ``has_more``, ``limit``, ``offset``,
            ``as_of`` and a ``filters`` echo. Read the rows off ``["jobs"]``.
        """
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = await self._client.get("/api/jobs", params=params)
        resp.raise_for_status()
        return _as_jobs_envelope(resp.json())

    @_create_retry_decorator()
    async def get_job(self, job_id: str) -> dict[str, Any]:
        """Get a single job by ID.

        Args:
            job_id: Job UUID

        Returns:
            Job dict with full details
        """
        resp = await self._client.get(f"/api/jobs/{job_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Audit Trail
    # =========================================================================

    @_create_retry_decorator()
    async def get_audit_trail(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
        filter_category: FilterCategory = "all",
    ) -> dict[str, Any]:
        """Get paginated audit entries for a job.

        Args:
            job_id: Job UUID
            page: Page number (1-indexed, -1 for last page)
            page_size: Entries per page (1-200)
            filter_category: Filter type (all, messages, tools, errors)

        Returns:
            Dict with entries, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
            "filter": filter_category,
        }
        resp = await self._client.get(f"/api/jobs/{job_id}/audit", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_audit_bulk(
        self,
        job_id: str,
        offset: int = 0,
        limit: int = 200,
        filter_category: FilterCategory = "all",
    ) -> dict[str, Any]:
        """Get a chunk of audit entries via offset/limit (lean projection).

        Routes through the standard ``/audit`` endpoint with ``lean=true`` so
        the heavy per-row payload (resolved_config metadata, tool arguments,
        state, tracebacks) is dropped server-side — tool *results* are still
        included. The former ``/audit/bulk`` route was removed for OOMing the
        orchestrator on large jobs.

        Args:
            job_id: Job UUID
            offset: Number of entries to skip
            limit: Maximum entries to return (capped at 200)
            filter_category: Filter type (all, messages, tools, errors)

        Returns:
            Dict with entries, total, offset, limit, hasMore
        """
        limit = min(limit, 200)
        resp = await self._client.get(
            f"/api/jobs/{job_id}/audit",
            params={
                "offset": offset,
                "limit": limit,
                "lean": "true",
                "filter": filter_category,
            },
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_audit_time_range(self, job_id: str) -> dict[str, str] | None:
        """Get first and last timestamps for job audit entries.

        Args:
            job_id: Job UUID

        Returns:
            Dict with start and end ISO timestamps, or None
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/audit/timerange")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Chat History
    # =========================================================================

    @_create_retry_decorator()
    async def get_chat_history(
        self,
        job_id: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """Get paginated chat history for a job.

        Args:
            job_id: Job UUID
            page: Page number (1-indexed, -1 for last page)
            page_size: Entries per page (1-200)

        Returns:
            Dict with entries, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
        }
        resp = await self._client.get(f"/api/jobs/{job_id}/chat", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_chat_bulk(
        self,
        job_id: str,
        offset: int = 0,
        limit: int = 200,
    ) -> dict[str, Any]:
        """Get a chunk of chat turns via offset/limit.

        Routes through the standard ``/chat`` endpoint (which now accepts
        offset/limit, mirroring ``/audit``). The former ``/chat/bulk`` route
        was removed alongside the other bulk endpoints.

        Args:
            job_id: Job UUID
            offset: Number of entries to skip
            limit: Maximum entries to return (capped at 200)

        Returns:
            Dict with entries, total, offset, limit, hasMore
        """
        limit = min(limit, 200)
        resp = await self._client.get(
            f"/api/jobs/{job_id}/chat",
            params={"offset": offset, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Todos
    # =========================================================================

    @_create_retry_decorator()
    async def get_todos(self, job_id: str) -> dict[str, Any]:
        """Get all todos for a job (current + archives).

        Args:
            job_id: Job UUID

        Returns:
            Dict with job_id, current, archives, has_workspace
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/todos")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_current_todos(self, job_id: str) -> dict[str, Any] | None:
        """Get current active todos from todos.yaml.

        Args:
            job_id: Job UUID

        Returns:
            Dict with todos list and metadata, or None if not found
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/todos/current")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_archived_todos(self, job_id: str) -> list[dict[str, Any]]:
        """List all archived todo files for a job.

        Args:
            job_id: Job UUID

        Returns:
            List of archive metadata (filename, phase_name, timestamp)
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/todos/archives")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_archived_todos(
        self, job_id: str, filename: str
    ) -> dict[str, Any] | None:
        """Get parsed content of an archived todo file.

        Args:
            job_id: Job UUID
            filename: Archive filename (e.g., todos_phase1_20260124_183618.md)

        Returns:
            Dict with parsed todos, summary, and metadata, or None if not found
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/todos/archives/{filename}")
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Graph Changes
    # =========================================================================

    @_create_retry_decorator()
    async def get_graph_changes(self, job_id: str) -> dict[str, Any]:
        """Get parsed graph changes for timeline visualization.

        Args:
            job_id: Job UUID

        Returns:
            Dict with jobId, timeRange, summary, snapshots, deltas
        """
        resp = await self._client.get(f"/api/graph/changes/{job_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # LLM Requests
    # =========================================================================

    @_create_retry_decorator()
    async def get_llm_request(self, doc_id: str) -> dict[str, Any]:
        """Get a single LLM request by its audit-store request ID.

        Args:
            doc_id: Audit-store request ID (string)

        Returns:
            Full LLM request document with messages and response
        """
        resp = await self._client.get(f"/api/requests/{doc_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Job Actions (mutations)
    # =========================================================================

    async def approve_job(self, job_id: str) -> dict[str, Any]:
        """Approve a frozen job, marking it as completed.

        Args:
            job_id: Job UUID

        Returns:
            Approval result with status and completion data
        """
        resp = await self._mutation_request("POST", f"/api/jobs/{job_id}/approve")
        resp.raise_for_status()
        return resp.json()

    async def resume_job(
        self,
        job_id: str,
        feedback: str | None = None,
    ) -> dict[str, Any]:
        """Resume a job from its checkpoint with optional feedback.

        Args:
            job_id: Job UUID
            feedback: Optional feedback to inject before resuming

        Returns:
            Resume result with status
        """
        body: dict[str, Any] = {}
        if feedback:
            body["feedback"] = feedback
        resp = await self._mutation_request(
            "POST",
            f"/api/jobs/{job_id}/resume",
            json=body if body else None,
        )
        resp.raise_for_status()
        return resp.json()

    async def cancel_job(self, job_id: str) -> dict[str, Any]:
        """Cancel a running job.

        Args:
            job_id: Job UUID

        Returns:
            Cancellation result with status
        """
        resp = await self._mutation_request("PUT", f"/api/jobs/{job_id}/cancel")
        resp.raise_for_status()
        return resp.json()

    async def pause_job(self, job_id: str) -> dict[str, Any]:
        """Pause a running job.

        Args:
            job_id: Job UUID

        Returns:
            Pause result with status
        """
        resp = await self._mutation_request("PUT", f"/api/jobs/{job_id}/pause")
        resp.raise_for_status()
        return resp.json()

    async def create_job(
        self,
        description: str,
        config_name: str = "worker_base",
        expert_id: str | None = None,
        datasource_ids: list[str] | None = None,
        instructions: str | None = None,
        kickoff_message: str | None = None,
        config_override: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        parent_job_id: str | None = None,
        project_id: str | None = None,
        user_id: str | None = None,
        thread_id: str | None = None,
        priority: int = 5,
        required_deliverables: list[str] | None = None,
        ticket: str | None = None,
        work_category: str | None = None,
        workspace: dict[str, Any] | None = None,
        workspace_supplied: bool = False,
    ) -> dict[str, Any]:
        """Create a new job.

        Args:
            description: Natural language task description
            config_name: Expert/agent config to use
            expert_id: Preferred database-backed expert UUID
            datasource_ids: Connector selection. Omit to inherit from an
                authoritative parent or use root defaults; pass [] to attach
                none; pass IDs to request exactly those connectors.
            instructions: Additional inline markdown instructions
            kickoff_message: Opening task brief sent to the agent
            config_override: Per-job config overrides
            context: Additional context dictionary
            parent_job_id: Parent job UUID for verification/follow-up jobs
            project_id: Project UUID to associate this job with
            user_id: User UUID who created this job
            thread_id: Persistent session origin used for server-side lineage
            priority: Dispatch priority from 0 (low) to 10 (high)
            required_deliverables: Immutable paths, "kb:<slug>" notes, or
                one exact "pr:<owner>/<repository>" bound to a writable
                attached repository. Do not use repos/<alias>/ paths or
                replace a refused publication contract with a note.
            ticket: Backlog ticket (knowledge-note slug) this job claims —
                the funnel stamps it into ``context.ticket_note_id``, the
                one key the one-shot claim ledger reads
            work_category: Explicit work category for the precedence law —
                the caller's stated intent, recorded against the slot's
                category in the kickoff contract
            workspace / workspace_supplied: the REST WorkspaceBinding (None = no
                workspace), sent only when supplied.

        Returns:
            Created job record with ID
        """
        body: dict[str, Any] = {
            "description": description,
            "config_name": config_name,
            "priority": priority,
        }
        if ticket:
            body["ticket"] = ticket
        if work_category:
            body["work_category"] = work_category
        if expert_id:
            body["expert_id"] = expert_id
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        else:
            body["use_datasource_defaults"] = True
        if instructions:
            body["instructions"] = instructions
        if kickoff_message:
            body["kickoff_message"] = kickoff_message
        if config_override:
            body["config_override"] = config_override
        if context:
            body["context"] = context
        if parent_job_id:
            body["parent_job_id"] = parent_job_id
        if project_id:
            body["project_id"] = project_id
        if user_id:
            body["user_id"] = user_id
        if thread_id:
            body["thread_id"] = thread_id
        if required_deliverables:
            body["required_deliverables"] = required_deliverables
        if workspace_supplied:
            body["workspace"] = workspace
        resp = await self._mutation_request("POST", "/api/jobs", json=body)
        resp.raise_for_status()
        return resp.json()

    async def delete_job(self, job_id: str) -> dict[str, Any]:
        """Delete a job and its associated data.

        Args:
            job_id: Job UUID

        Returns:
            Deletion result with status
        """
        resp = await self._mutation_request("DELETE", f"/api/jobs/{job_id}")
        resp.raise_for_status()
        return resp.json()

    async def assign_job(self, job_id: str, agent_id: str) -> dict[str, Any]:
        """Request the admin assignment override.

        Args:
            job_id: Job UUID
            agent_id: Agent UUID

        Returns:
            Assignment result; status is ``assigned`` or ``queued`` when the
            workspace first requires automatic provisioning
        """
        resp = await self._mutation_request(
            "POST", f"/api/jobs/{job_id}/assign/{agent_id}"
        )
        resp.raise_for_status()
        return resp.json()

    async def test_datasource(self, datasource_id: str) -> dict[str, Any]:
        """Test connectivity to a datasource.

        Args:
            datasource_id: Datasource UUID

        Returns:
            Test result with status and message
        """
        resp = await self._non_get_read_request(
            "POST", f"/api/datasources/{datasource_id}/test"
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Git History (Gitea proxy)
    # =========================================================================

    @_create_retry_decorator()
    async def list_job_commits(
        self,
        job_id: str,
        sha: str = "main",
        since_ref: str | None = None,
        page: int = 1,
        limit: int = 20,
    ) -> dict[str, Any]:
        """List git commits for a job's repository.

        Args:
            job_id: Job UUID
            sha: Branch, tag, or SHA to list from
            since_ref: Only show commits after this ref
            page: Page number
            limit: Max commits

        Returns:
            Dict with total_commits and commits list
        """
        params: dict[str, Any] = {"sha": sha, "page": page, "limit": limit}
        if since_ref:
            params["since_ref"] = since_ref
        resp = await self._client.get(f"/api/jobs/{job_id}/repo/commits", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_job_diff(
        self,
        job_id: str,
        base: str,
        head: str = "HEAD",
    ) -> dict[str, Any]:
        """Get unified diff between two refs in a job's repository.

        Args:
            job_id: Job UUID
            base: Base ref (commit SHA, tag, or branch)
            head: Head ref

        Returns:
            Dict with base, head, and diff text
        """
        resp = await self._client.get(
            f"/api/jobs/{job_id}/repo/diff",
            params={"base": base, "head": head},
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_job_file(
        self,
        job_id: str,
        path: str,
        ref: str | None = None,
    ) -> dict[str, Any]:
        """Read a file from a job's Gitea repository at any ref.

        Args:
            job_id: Job UUID
            path: File path within the repo
            ref: Branch, tag, or commit SHA

        Returns:
            Dict with path, content, and size
        """
        params: dict[str, Any] = {"path": path}
        if ref:
            params["ref"] = ref
        resp = await self._client.get(f"/api/jobs/{job_id}/repo/file", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_job_files(
        self,
        job_id: str,
        path: str = "",
        ref: str | None = None,
    ) -> list[dict[str, Any]]:
        """List directory contents from a job's Gitea repository.

        Args:
            job_id: Job UUID
            path: Directory path
            ref: Branch, tag, or commit SHA

        Returns:
            List of entries with name, path, type, size
        """
        params: dict[str, Any] = {"path": path}
        if ref:
            params["ref"] = ref
        resp = await self._client.get(
            f"/api/jobs/{job_id}/repo/contents", params=params
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_job_tags(self, job_id: str) -> list[dict[str, Any]]:
        """List tags in a job's repository.

        Args:
            job_id: Job UUID

        Returns:
            List of tags with name, sha, and message
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/repo/tags")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Workspace & Job Context
    # =========================================================================

    @_create_retry_decorator()
    async def get_frozen_job(self, job_id: str) -> dict[str, Any]:
        """Get frozen job review data.

        Args:
            job_id: Job UUID

        Returns:
            Frozen job data (summary, confidence, deliverables, notes)
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/frozen")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_workspace_file(self, job_id: str, path: str) -> dict[str, Any]:
        """Read a file from the job's workspace repo (Gitea-backed).

        Committed state as of the worker's last phase-boundary push, read
        at the job branch head.

        Args:
            job_id: Job UUID
            path: Relative path within the workspace repo

        Returns:
            Dict with path, content, and size
        """
        resp = await self._client.get(
            f"/api/jobs/{job_id}/repo/file", params={"path": path}
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_workspace_overview(self, job_id: str) -> dict[str, Any]:
        """Get workspace overview with file listing and content previews.

        Composed from the job's Gitea repo (committed state as of the
        worker's last phase-boundary push): repo-root listing, truncated
        workspace.md/plan.md previews when present, and the Gitea-backed
        todo state. Degrades to ``has_workspace: False`` when Gitea or the
        job repo is unavailable.

        Args:
            job_id: Job UUID

        Returns:
            Workspace overview dict (job_id, has_workspace, files,
            workspace_md, plan_md, todos, archive_count)
        """
        overview: dict[str, Any] = {
            "job_id": job_id,
            "has_workspace": False,
            "files": [],
            "workspace_md": None,
            "plan_md": None,
            "todos": None,
            "archive_count": 0,
        }

        resp = await self._client.get(
            f"/api/jobs/{job_id}/repo/contents", params={"path": ""}
        )
        if resp.status_code in (404, 503):
            return overview
        resp.raise_for_status()
        entries = resp.json() or []

        overview["has_workspace"] = True
        overview["files"] = [
            {
                "name": entry.get("name"),
                "size": entry.get("size", 0),
                "type": entry.get("type"),
            }
            for entry in entries
        ]

        file_names = {e.get("name") for e in entries if e.get("type") == "file"}
        for key, name in (("workspace_md", "workspace.md"), ("plan_md", "plan.md")):
            if name not in file_names:
                continue
            file_resp = await self._client.get(
                f"/api/jobs/{job_id}/repo/file", params={"path": name}
            )
            if file_resp.status_code != 200:
                continue
            content = file_resp.json().get("content") or ""
            preview = content[:2000]
            if len(content) > 2000:
                preview += "\n\n... (truncated)"
            overview[key] = preview

        todos_resp = await self._client.get(f"/api/jobs/{job_id}/todos")
        if todos_resp.status_code == 200:
            todos_data = todos_resp.json()
            overview["todos"] = todos_data.get("current")
            overview["archive_count"] = len(todos_data.get("archives") or [])

        return overview

    @_create_retry_decorator()
    async def get_job_progress(self, job_id: str) -> dict[str, Any]:
        """Get job liveness/progress (E3 shared liveness contract).

        Args:
            job_id: Job UUID

        Returns:
            Liveness data dict: status, state, reasons, observed_at,
            last_activity_at, elapsed_seconds, sources
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/progress")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Evidence Manifest (E4 — officer_supervision_surface §3.3)
    # =========================================================================

    @_create_retry_decorator()
    async def list_job_evidence(self, job_id: str) -> dict[str, Any]:
        """List the typed evidence manifest recorded at completion.

        Args:
            job_id: Job UUID

        Returns:
            Dict with recorded_at and entries (id, kind, label, media_type,
            byte_size, sha256, source revision, producer, availability)
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/evidence")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def read_job_evidence(
        self, job_id: str, evidence_id: str, offset: int = 0
    ) -> dict[str, Any]:
        """Read one evidence entry by opaque ID (server-resolved, bounded).

        Args:
            job_id: Job UUID
            evidence_id: Opaque manifest entry ID
            offset: Character offset for paginated text reads

        Returns:
            Dict with the entry metadata plus content page or safe binary view
        """
        resp = await self._client.get(
            f"/api/jobs/{job_id}/evidence/{evidence_id}",
            params={"offset": offset},
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_job_completion_report(self, job_id: str) -> dict[str, Any]:
        """Get the server-recorded completion report for a job.

        Args:
            job_id: Job UUID

        Returns:
            Dict with report (summary/confidence/deliverables/notes),
            recorded_at, and source_revision
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/completion-report")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # System Monitoring
    # =========================================================================

    @_create_retry_decorator()
    async def get_job_stats(self) -> dict[str, Any]:
        """Get job queue statistics."""
        resp = await self._client.get("/api/stats/jobs")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_agent_stats(self) -> dict[str, Any]:
        """Get agent workforce summary."""
        resp = await self._client.get("/api/stats/agents")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_stuck_jobs(
        self, threshold_minutes: int | None = None
    ) -> dict[str, Any]:
        """Get jobs stuck in processing beyond a threshold."""
        params = (
            {"threshold_minutes": threshold_minutes}
            if threshold_minutes is not None
            else None
        )
        resp = await self._client.get(
            "/api/stats/stuck",
            params=params,
        )
        resp.raise_for_status()
        payload = resp.json()
        # Rolling compatibility with a pre-OC-08 server. New servers always
        # return the policy envelope, including for an empty result.
        if isinstance(payload, list):
            return {
                "jobs": payload,
                "threshold_minutes": threshold_minutes,
                "threshold_source": (
                    "request_override" if threshold_minutes is not None else "legacy"
                ),
            }
        return payload

    @_create_retry_decorator()
    async def list_agents(
        self, status: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """List registered agents."""
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = await self._client.get("/api/agents", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_experts(self) -> list[dict[str, Any]]:
        """List available expert configurations."""
        resp = await self._client.get("/api/experts")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_expert(self, expert_id: str) -> dict[str, Any]:
        """Get full expert config detail."""
        resp = await self._client.get(f"/api/experts/{expert_id}")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_skills(self) -> list[dict[str, Any]]:
        """List available skills (bundled + DB)."""
        resp = await self._client.get("/api/skills")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_skill(self, skill_id: str) -> dict[str, Any]:
        """Get full skill detail (metadata + files)."""
        resp = await self._client.get(f"/api/skills/{skill_id}")
        resp.raise_for_status()
        return resp.json()

    async def reload_skills(self) -> dict[str, Any]:
        """Force reload of bundled skills from disk."""
        resp = await self._mutation_request("POST", "/api/skills/reload")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_models(self) -> dict[str, Any]:
        """List available models from the model catalog."""
        resp = await self._client.get("/api/models")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_connector_drivers(self) -> dict[str, Any]:
        """The capability matrix of every installed connector driver."""
        resp = await self._client.get("/api/datasources/drivers")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_datasources(
        self,
        ds_type: str | None = None,
        *,
        q: str | None = None,
        project_id: str | None = None,
        scope_mode: DatasourceScopeMode | None = None,
        auto_attach: bool | None = None,
        visibility: DatasourceVisibility | None = None,
        ownership: DatasourceOwnership | None = None,
        availability: DatasourceAvailability | None = None,
        limit: int = 50,
        cursor: str | None = None,
    ) -> dict[str, Any]:
        """List the authorized, cursor-paginated connector catalog."""
        params: dict[str, Any] = {"limit": limit}
        if ds_type:
            params["type"] = ds_type
        if q:
            params["q"] = q
        if project_id:
            params["project_id"] = project_id
        if scope_mode:
            params["scope_mode"] = scope_mode
        if auto_attach is not None:
            params["auto_attach"] = auto_attach
        if visibility:
            params["visibility"] = visibility
        if ownership:
            params["ownership"] = ownership
        if availability:
            params["availability"] = availability
        if cursor:
            params["cursor"] = cursor
        resp = await self._client.get("/api/datasources/catalog", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_datasource(self, datasource_id: str) -> dict[str, Any]:
        """Get one authorized connector with its exact policy revision."""
        resp = await self._client.get(f"/api/datasources/{datasource_id}")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_daily_stats(self, days: int = 7) -> list[dict[str, Any]]:
        """Get daily job statistics for the past N days.

        Args:
            days: Number of days to look back (1-90, default 7)

        Returns:
            List of daily stats with date, jobs_created, jobs_completed,
            jobs_failed, jobs_cancelled
        """
        resp = await self._client.get("/api/stats/daily", params={"days": days})
        resp.raise_for_status()
        return resp.json()

    async def reload_experts(self) -> dict[str, Any]:
        """Force reload of expert configurations from disk.

        Returns:
            Dict with status and count of loaded experts
        """
        resp = await self._mutation_request("POST", "/api/experts/reload")
        resp.raise_for_status()
        return resp.json()

    async def deregister_agent(self, agent_id: str) -> dict[str, str]:
        """Deregister (delete) an agent.

        Args:
            agent_id: Agent UUID

        Returns:
            Status dict
        """
        resp = await self._mutation_request("DELETE", f"/api/agents/{agent_id}")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Tables (for debugging)
    # =========================================================================

    @_create_retry_decorator()
    async def list_tables(self) -> list[dict[str, Any]]:
        """List available database tables with row counts."""
        resp = await self._client.get("/api/tables")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_table_data(
        self,
        table_name: str,
        page: int = 1,
        page_size: int = 50,
    ) -> dict[str, Any]:
        """Get paginated table data.

        Args:
            table_name: Table name (jobs, requirements, citations, etc.)
            page: Page number (1-indexed, -1 for last page)
            page_size: Rows per page (1-500)

        Returns:
            Dict with data, total, page, pageSize, hasMore
        """
        params = {
            "page": page,
            "pageSize": page_size,
        }
        resp = await self._client.get(f"/api/tables/{table_name}", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_table_schema(self, table_name: str) -> list[dict[str, Any]]:
        """Get column definitions for a table.

        Args:
            table_name: Table name

        Returns:
            List of column definitions with name, type, nullable, default
        """
        resp = await self._client.get(f"/api/tables/{table_name}/schema")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Citation & Source Library
    # =========================================================================

    @_create_retry_decorator()
    async def list_job_sources(
        self,
        job_id: str | None = None,
        source_type: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List sources, optionally filtered by job and/or type.

        Args:
            job_id: Filter by job UUID (omit for all sources)
            source_type: Filter by type (document, website, database, custom)
            limit: Max results
            offset: Pagination offset

        Returns:
            Dict with sources list and total count
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if job_id:
            params["job_id"] = job_id
        if source_type:
            params["type"] = source_type
        resp = await self._client.get("/api/sources", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_source_detail(
        self,
        source_id: int,
        content_limit: int = 2000,
    ) -> dict[str, Any]:
        """Get full detail for a single source.

        Args:
            source_id: Source ID (integer)
            content_limit: Max characters of content to return

        Returns:
            Source record with type, identifier, name, content, metadata
        """
        params: dict[str, Any] = {"content_limit": content_limit}
        resp = await self._client.get(f"/api/sources/{source_id}", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_job_citations(
        self,
        job_id: str,
        source_id: int | None = None,
        verification_status: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List citations for a job with optional filters.

        Args:
            job_id: Job UUID
            source_id: Filter by source ID
            verification_status: Filter by status (pending, verified, failed, unverified)
            limit: Max results
            offset: Pagination offset

        Returns:
            Dict with citations list and total count
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if source_id is not None:
            params["source_id"] = source_id
        if verification_status:
            params["status"] = verification_status
        resp = await self._client.get(f"/api/jobs/{job_id}/citations", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_citation_detail(self, citation_id: int) -> dict[str, Any]:
        """Get full citation record with source info and verification details.

        Args:
            citation_id: Citation ID (integer)

        Returns:
            Full citation with claim, quote, source, verification data
        """
        resp = await self._client.get(f"/api/citations/{citation_id}")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def search_job_sources(
        self,
        job_id: str,
        query: str,
        mode: str = "keyword",
        source_type: str | None = None,
        tags: str | None = None,
        top_k: int = 10,
    ) -> dict[str, Any]:
        """Search a job's source library.

        Args:
            job_id: Job UUID
            query: Search query text
            mode: Search mode (keyword, semantic, hybrid)
            source_type: Filter by source type
            tags: Comma-separated tags (AND logic)
            top_k: Max results

        Returns:
            Search results with evidence labels and snippets
        """
        params: dict[str, Any] = {"query": query, "mode": mode, "top_k": top_k}
        if source_type:
            params["source_type"] = source_type
        if tags:
            params["tags"] = tags
        resp = await self._client.get(
            f"/api/jobs/{job_id}/sources/search", params=params
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_source_annotations(
        self,
        job_id: str,
        source_id: int,
        annotation_type: str | None = None,
    ) -> list[dict[str, Any]]:
        """Get annotations for a source within a job.

        Args:
            job_id: Job UUID
            source_id: Source ID
            annotation_type: Filter by type (note, highlight, summary, question, critique)

        Returns:
            List of annotations with type, content, page reference
        """
        params: dict[str, Any] = {}
        if annotation_type:
            params["type"] = annotation_type
        resp = await self._client.get(
            f"/api/jobs/{job_id}/sources/{source_id}/annotations", params=params
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_source_tags(self, job_id: str, source_id: int) -> list[str]:
        """Get tags for a source within a job.

        Args:
            job_id: Job UUID
            source_id: Source ID

        Returns:
            List of tag strings
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/sources/{source_id}/tags")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_citation_stats(self, job_id: str) -> dict[str, Any]:
        """Get citation statistics for a job.

        Args:
            job_id: Job UUID

        Returns:
            Stats with source/citation counts by type, status, confidence, method
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/citations/stats")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_memory_stats(self, job_id: str) -> dict[str, Any]:
        """Get memory statistics for a job.

        Args:
            job_id: Job UUID

        Returns:
            Stats with counts by type, source, tokens, accesses, avg importance
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/memory/stats")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Logs & LLM Requests
    # =========================================================================

    @_create_retry_decorator()
    async def get_job_logs(
        self,
        job_id: str,
        lines: int = 100,
        grep: str | None = None,
        level: str | None = None,
    ) -> dict[str, Any]:
        """Get tail of a job's log file with optional filtering.

        Args:
            job_id: Job UUID
            lines: Number of tail lines (1-1000)
            grep: Case-insensitive substring filter
            level: Log level filter (DEBUG, INFO, WARNING, ERROR)

        Returns:
            Dict with lines list, total_lines, filtered flag
        """
        params: dict[str, Any] = {"lines": lines}
        if grep:
            params["grep"] = grep
        if level:
            params["level"] = level
        resp = await self._client.get(f"/api/jobs/{job_id}/logs", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_thread_logs(
        self,
        thread_id: str,
        lines: int = 100,
        grep: str | None = None,
        level: str | None = None,
    ) -> dict[str, Any]:
        """Get the archived agent-pod log for a session with optional filtering.

        Args:
            thread_id: Thread UUID
            lines: Number of tail lines (1-1000)
            grep: Case-insensitive substring filter
            level: Log level filter (DEBUG, INFO, WARNING, ERROR)

        Returns:
            Dict with lines list, total_lines, filtered flag
        """
        params: dict[str, Any] = {"lines": lines}
        if grep:
            params["grep"] = grep
        if level:
            params["level"] = level
        resp = await self._client.get(
            f"/api/persistent/threads/{thread_id}/logs", params=params
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_llm_requests(
        self,
        job_id: str,
        limit: int = 20,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List LLM requests for a job with summary fields.

        Args:
            job_id: Job UUID
            limit: Max entries (1-100)
            offset: Pagination offset

        Returns:
            Dict with entries, total, offset, limit, hasMore
        """
        resp = await self._client.get(
            f"/api/jobs/{job_id}/llm-requests",
            params={"limit": limit, "offset": offset},
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_shell_state(self, job_id: str) -> dict[str, Any]:
        """Get shell state for a job's agent.

        Proxied through the orchestrator to the agent's /system/shell-state endpoint.

        Args:
            job_id: Job UUID

        Returns:
            Dict with tabs list, each containing name, type, recent output
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/shell-state")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Agent System Monitoring
    # =========================================================================

    @_create_retry_decorator()
    async def get_agent_system_info(self, agent_id: str) -> dict[str, Any]:
        """Get system information from an agent's container.

        Proxied through the orchestrator to the agent's /system/info endpoint.
        Returns CPU, memory, disk, listening ports, processes, and network connections.

        Args:
            agent_id: Agent UUID

        Returns:
            Dict with cpu, memory, disk, listening_ports, processes,
            network_connections, and agent info
        """
        resp = await self._client.get(f"/api/agents/{agent_id}/system-info")
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Knowledge Base
    # =========================================================================

    @_create_retry_decorator()
    async def get_knowledge_summary(self, project_id: str) -> dict[str, Any]:
        """Get knowledge base summary statistics for a project.

        Args:
            project_id: Project UUID

        Returns:
            Dict with total, by_type, by_status, and recent notes
        """
        resp = await self._client.get(f"/api/projects/{project_id}/knowledge/summary")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_knowledge_notes(
        self,
        project_id: str,
        note_type: str | None = None,
        status: str | None = None,
        tag: str | None = None,
        job_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """List knowledge notes for a project with optional filters.

        Args:
            project_id: Project UUID
            note_type: Filter by type (insight, decision, pattern, issue, etc.)
            status: Filter by status (active, resolved, superseded, archived)
            tag: Filter by tag
            job_id: Filter by originating job
            limit: Max results (1-200)
            offset: Pagination offset

        Returns:
            Dict with notes list, total, limit, offset
        """
        params: dict[str, Any] = {"limit": limit, "offset": offset}
        if note_type:
            params["type"] = note_type
        if status:
            params["status"] = status
        if tag:
            params["tag"] = tag
        if job_id:
            params["job_id"] = job_id
        resp = await self._client.get(
            f"/api/projects/{project_id}/knowledge", params=params
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_knowledge_note(self, project_id: str, note_id: str) -> dict[str, Any]:
        """Get a single knowledge note with full content and relationships.

        Args:
            project_id: Project UUID
            note_id: Note ID

        Returns:
            Full note record with content, metadata, and Neo4j relationships
        """
        resp = await self._client.get(f"/api/projects/{project_id}/knowledge/{note_id}")
        resp.raise_for_status()
        return resp.json()

    async def search_knowledge(
        self,
        project_id: str,
        query: str,
        limit: int = 10,
    ) -> dict[str, Any]:
        """Hybrid search over project knowledge base.

        Uses dense vector + sparse keyword search when embeddings are
        available, falls back to keyword-only search otherwise.

        Args:
            project_id: Project UUID
            query: Search query text
            limit: Max results (1-50)

        Returns:
            Dict with notes list, query, and total count
        """
        resp = await self._non_get_read_request(
            "POST",
            f"/api/projects/{project_id}/knowledge/search",
            json={"query": query, "limit": limit},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Projects
    # =========================================================================

    @_create_retry_decorator()
    async def list_projects(
        self,
        user_id: str | None = None,
        statuses: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """List projects, optionally filtered by user membership and status.

        Args:
            user_id: Filter to projects this user belongs to
            statuses: Lifecycle statuses to include (repeatable ``?status=``).
                ``None`` leaves the server default in place, which is
                active-only — so a caller that wants archives must ask for
                them explicitly. Without this the formatter's
                ``include_archived`` is a no-op, because the server has
                already dropped the rows it would have marked.

        Returns:
            List of project dicts
        """
        params: dict[str, Any] = {}
        if user_id:
            params["user_id"] = user_id
        if statuses:
            params["status"] = list(statuses)
        resp = await self._client.get("/api/projects", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_project(self, project_id: str) -> dict[str, Any]:
        """Get a single project by ID.

        Args:
            project_id: Project UUID

        Returns:
            Project dict with name, description, goal, config, timestamps
        """
        resp = await self._client.get(f"/api/projects/{project_id}")
        resp.raise_for_status()
        return resp.json()

    async def create_project(
        self,
        name: str,
        user_id: str,
        description: str | None = None,
        goal: str | None = None,
        default_config_name: str | None = None,
        default_config_override: dict[str, Any] | None = None,
        external_kb: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Create a new project.

        Args:
            name: Project name
            user_id: Owner user UUID
            description: Project description
            goal: Project goal statement
            default_config_name: Default agent config for new jobs
            default_config_override: Default config overrides for new jobs
            external_kb: The live KB's GitHub repo, either as
                ``{"datasource_id": ...}`` naming an existing OKF Knowledge
                Base connector to adopt, or ``{"repo_url", "token",
                "branch"?, "forge"?}`` inline

        Returns:
            Created project record with ID
        """
        body: dict[str, Any] = {"name": name, "user_id": user_id}
        if description:
            body["description"] = description
        if goal:
            body["goal"] = goal
        if default_config_name:
            body["default_config_name"] = default_config_name
        if default_config_override is not None:
            body["default_config_override"] = default_config_override
        if external_kb is not None:
            body["external_kb"] = external_kb
        resp = await self._mutation_request("POST", "/api/projects", json=body)
        resp.raise_for_status()
        return resp.json()

    async def attach_project_knowledge_repository(
        self, project_id: str, external_kb: dict[str, Any]
    ) -> dict[str, Any]:
        """Attach an existing private GitHub live vault to a project."""
        resp = await self._mutation_request(
            "POST",
            f"/api/projects/{project_id}/knowledge/repository",
            json=external_kb,
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_project_jobs(
        self,
        project_id: str,
        status: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """List jobs belonging to a project.

        Args:
            project_id: Project UUID
            status: Optional status filter
            limit: Max results (1-500)

        Returns:
            List of job dicts
        """
        params: dict[str, Any] = {"limit": limit}
        if status:
            params["status"] = status
        resp = await self._client.get(f"/api/projects/{project_id}/jobs", params=params)
        resp.raise_for_status()
        return resp.json()

    async def create_project_job(
        self,
        project_id: str,
        description: str,
        config_name: str = "worker_base",
        expert_id: str | None = None,
        datasource_ids: list[str] | None = None,
        instructions: str | None = None,
        kickoff_message: str | None = None,
        config_override: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
        priority: int = 5,
        required_deliverables: list[str] | None = None,
        workspace: dict[str, Any] | None = None,
        workspace_supplied: bool = False,
    ) -> dict[str, Any]:
        """Create a job within a project context.

        Args:
            project_id: Project UUID
            description: Task description
            config_name: Expert/agent config to use
            expert_id: Preferred database-backed expert UUID
            datasource_ids: Connector selection. Omit to use the project's
                automatic defaults; pass [] to attach none; pass IDs to
                request exactly those connectors.
            instructions: Additional inline instructions
            kickoff_message: Opening task brief sent to the agent
            config_override: Per-job config overrides
            context: Additional context dictionary
            priority: Dispatch priority from 0 (low) to 10 (high)
            required_deliverables: Immutable paths, "kb:<slug>" notes, or
                one exact "pr:<owner>/<repository>" bound to a writable
                attached repository. Do not use repos/<alias>/ paths or
                replace a refused publication contract with a note.
            workspace / workspace_supplied: the REST WorkspaceBinding (None = no
                workspace), sent only when supplied.

        Returns:
            Created job record with ID
        """
        body: dict[str, Any] = {
            "description": description,
            "config_name": config_name,
            "priority": priority,
        }
        if expert_id:
            body["expert_id"] = expert_id
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        else:
            body["use_datasource_defaults"] = True
        if instructions:
            body["instructions"] = instructions
        if kickoff_message:
            body["kickoff_message"] = kickoff_message
        if config_override:
            body["config_override"] = config_override
        if context:
            body["context"] = context
        if required_deliverables:
            body["required_deliverables"] = required_deliverables
        if workspace_supplied:
            body["workspace"] = workspace
        resp = await self._mutation_request(
            "POST", f"/api/projects/{project_id}/jobs", json=body
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Project Management (Extended)
    # =========================================================================

    async def update_project(
        self,
        project_id: str,
        name: str | None = None,
        description: str | None = None,
        goal: str | None = None,
        status: str | None = None,
        default_config_name: str | None = None,
        default_config_override: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Update a project's metadata or defaults.

        Args:
            project_id: Project UUID
            name: New name
            description: New description
            goal: New goal statement
            status: New status
            default_config_name: New default agent config
            default_config_override: New default config overrides

        Returns:
            Status dict
        """
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        if goal is not None:
            body["goal"] = goal
        if status is not None:
            body["status"] = status
        if default_config_name is not None:
            body["default_config_name"] = default_config_name
        if default_config_override is not None:
            body["default_config_override"] = default_config_override
        resp = await self._mutation_request(
            "PATCH", f"/api/projects/{project_id}", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def delete_project(self, project_id: str) -> dict[str, str]:
        """Delete a project and its associated data.

        Cannot delete default projects.

        Args:
            project_id: Project UUID

        Returns:
            Status dict
        """
        resp = await self._mutation_request("DELETE", f"/api/projects/{project_id}")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_project_members(self, project_id: str) -> list[dict[str, Any]]:
        """List members of a project with their roles.

        Args:
            project_id: Project UUID

        Returns:
            List of member dicts with user_id, role, display_name, etc.
        """
        resp = await self._client.get(f"/api/projects/{project_id}/members")
        resp.raise_for_status()
        return resp.json()

    async def add_project_member(
        self,
        project_id: str,
        user_id: str,
        role: str = "editor",
    ) -> dict[str, Any]:
        """Add a member to a project.

        Args:
            project_id: Project UUID
            user_id: User UUID to add
            role: Member role (owner, editor, viewer)

        Returns:
            Created member record
        """
        body = {"user_id": user_id, "role": role}
        resp = await self._mutation_request(
            "POST", f"/api/projects/{project_id}/members", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def update_project_member(
        self,
        project_id: str,
        user_id: str,
        role: str,
    ) -> dict[str, str]:
        """Update a project member's role.

        Args:
            project_id: Project UUID
            user_id: User UUID
            role: New role (owner, editor, viewer)

        Returns:
            Status dict
        """
        resp = await self._mutation_request(
            "PATCH",
            f"/api/projects/{project_id}/members/{user_id}",
            json={"role": role},
        )
        resp.raise_for_status()
        return resp.json()

    async def remove_project_member(
        self,
        project_id: str,
        user_id: str,
    ) -> dict[str, str]:
        """Remove a member from a project.

        Cannot remove the last owner.

        Args:
            project_id: Project UUID
            user_id: User UUID to remove

        Returns:
            Status dict
        """
        resp = await self._mutation_request(
            "DELETE", f"/api/projects/{project_id}/members/{user_id}"
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_project_experts(self, project_id: str) -> list[dict[str, Any]]:
        """List project-specific expert configurations.

        Args:
            project_id: Project UUID

        Returns:
            List of ExpertInfo dicts
        """
        resp = await self._client.get(f"/api/projects/{project_id}/experts")
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_project_expert(
        self, project_id: str, expert_name: str
    ) -> dict[str, Any]:
        """Get detailed expert configuration for a project.

        Args:
            project_id: Project UUID
            expert_name: Expert config name

        Returns:
            ExpertInfo with merged config and instructions
        """
        resp = await self._client.get(
            f"/api/projects/{project_id}/experts/{expert_name}"
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Datasource CRUD
    # =========================================================================

    async def create_datasource(
        self,
        name: str,
        ds_type: str,
        connection_url: str | None = None,
        description: str | None = None,
        credentials: dict[str, Any] | None = None,
        cli_hint: str | None = None,
        default_branch: str | None = None,
        config: dict[str, Any] | None = None,
        is_global: bool = False,
        read_only: bool | None = None,
        scope_mode: DatasourceScopeMode = "all",
        project_ids: list[str] | None = None,
        auto_attach: bool = False,
    ) -> dict[str, Any]:
        """Create a new datasource.

        Args:
            name: User-provided label
            ds_type: Canonical connector type ID
            connection_url: Connection string (nullable for generic)
            description: What this datasource contains
            credentials: Auth details
            cli_hint: Suggested CLI command
            default_branch: Branch to clone (repository type)
            config: Non-secret type-specific configuration
            is_global: Publish for all users (capability-gated)
            read_only: Declarative public/project access hint
            scope_mode: ``all`` for every otherwise-authorized work context,
                or ``projects`` to restrict availability to project_ids
            project_ids: Full initial project scope. Omit for all-scope
                connectors; projects mode requires a nonempty list.
            auto_attach: Preselect this connector for its owner's new work
                wherever it is available. This never force-attaches it.

        Returns:
            Created datasource record with ID
        """
        body: dict[str, Any] = {
            "name": name,
            "type": ds_type,
        }
        if connection_url is not None:
            body["connection_url"] = connection_url
        if description is not None:
            body["description"] = description
        if credentials is not None:
            body["credentials"] = credentials
        if cli_hint is not None:
            body["cli_hint"] = cli_hint
        if default_branch is not None:
            body["default_branch"] = default_branch
        if config is not None:
            body["config"] = config
        body["is_global"] = is_global
        if read_only is not None:
            body["read_only"] = read_only
        body["scope_mode"] = scope_mode
        if project_ids is not None:
            body["project_ids"] = project_ids
        body["auto_attach"] = auto_attach
        resp = await self._mutation_request("POST", "/api/datasources", json=body)
        resp.raise_for_status()
        return resp.json()

    async def update_datasource(
        self,
        datasource_id: str,
        name: str | None = None,
        description: str | None = None,
        connection_url: str | None = None,
        credentials: dict[str, Any] | None = None,
        cli_hint: str | None = None,
        default_branch: str | None = None,
        config: dict[str, Any] | None = None,
        is_global: bool | None = None,
        read_only: bool | None = None,
        scope_mode: DatasourceScopeMode | None = None,
        project_ids: list[str] | None = None,
        auto_attach: bool | None = None,
        policy_revision: int | None = None,
    ) -> dict[str, str]:
        """Update a datasource.

        Args:
            datasource_id: Datasource UUID
            name: New label
            description: New description
            connection_url: New connection string
            credentials: New auth details
            cli_hint: New CLI hint
            default_branch: New default branch
            config: New non-secret type-specific configuration
            is_global: Publish/unpublish (publication is capability-gated)
            read_only: New read-only tag (agents get no write tools for it)
            scope_mode: New availability scope. Omit to preserve it.
            project_ids: Desired full project set. Omit to preserve existing
                links; pass [] to remove all links (valid only with all scope).
            auto_attach: New owner-only default-selection preference
            policy_revision: Revision loaded by the caller. Required for a
                scope, project-link, or auto-attach edit.

        Returns:
            Status dict
        """
        body: dict[str, Any] = {}
        if name is not None:
            body["name"] = name
        if description is not None:
            body["description"] = description
        if connection_url is not None:
            body["connection_url"] = connection_url
        if credentials is not None:
            body["credentials"] = credentials
        if cli_hint is not None:
            body["cli_hint"] = cli_hint
        if default_branch is not None:
            body["default_branch"] = default_branch
        if config is not None:
            body["config"] = config
        if is_global is not None:
            body["is_global"] = is_global
        if read_only is not None:
            body["read_only"] = read_only
        if scope_mode is not None:
            body["scope_mode"] = scope_mode
        if project_ids is not None:
            body["project_ids"] = project_ids
        if auto_attach is not None:
            body["auto_attach"] = auto_attach
        if policy_revision is not None:
            body["policy_revision"] = policy_revision
        resp = await self._mutation_request(
            "PUT", f"/api/datasources/{datasource_id}", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def delete_datasource(self, datasource_id: str) -> dict[str, str]:
        """Delete a datasource.

        Args:
            datasource_id: Datasource UUID

        Returns:
            Status dict
        """
        resp = await self._mutation_request(
            "DELETE", f"/api/datasources/{datasource_id}"
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Project ↔ Datasource (N:M)
    # =========================================================================

    async def list_project_datasources(self, project_id: str) -> list[dict[str, Any]]:
        """List datasources linked to a project."""
        resp = await self._client.get(f"/api/projects/{project_id}/datasources")
        resp.raise_for_status()
        return resp.json()

    async def link_datasource_to_project(
        self, project_id: str, datasource_id: str
    ) -> dict[str, str]:
        """Link a datasource to a project."""
        resp = await self._mutation_request(
            "POST", f"/api/projects/{project_id}/datasources/{datasource_id}"
        )
        resp.raise_for_status()
        return resp.json()

    async def update_project_datasource(
        self,
        project_id: str,
        datasource_id: str,
        read_only: bool | None = None,
        description: str | None = None,
    ) -> dict[str, str]:
        """Update project-level settings for a linked datasource."""
        body: dict[str, Any] = {}
        if read_only is not None:
            body["read_only"] = read_only
        if description is not None:
            body["description"] = description
        resp = await self._mutation_request(
            "PATCH",
            f"/api/projects/{project_id}/datasources/{datasource_id}",
            json=body,
        )
        resp.raise_for_status()
        return resp.json()

    async def unlink_datasource_from_project(
        self, project_id: str, datasource_id: str
    ) -> dict[str, str]:
        """Unlink a datasource from a project."""
        resp = await self._mutation_request(
            "DELETE", f"/api/projects/{project_id}/datasources/{datasource_id}"
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Knowledge Base Mutations
    # =========================================================================

    async def update_knowledge_note(
        self,
        project_id: str,
        note_id: str,
        status: str | None = None,
        add_tags: list[str] | None = None,
        remove_tags: list[str] | None = None,
    ) -> dict[str, str]:
        """Update a knowledge note's status or tags.

        Args:
            project_id: Project UUID
            note_id: Note ID
            status: New status (active, resolved, superseded, archived)
            add_tags: Tags to add
            remove_tags: Tags to remove

        Returns:
            Status dict
        """
        body: dict[str, Any] = {}
        if status:
            body["status"] = status
        if add_tags:
            body["add_tags"] = add_tags
        if remove_tags:
            body["remove_tags"] = remove_tags
        resp = await self._mutation_request(
            "PATCH", f"/api/projects/{project_id}/knowledge/{note_id}", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def delete_knowledge_note(
        self, project_id: str, note_id: str
    ) -> dict[str, str]:
        """Hard delete a knowledge note from both stores.

        Args:
            project_id: Project UUID
            note_id: Note ID

        Returns:
            Status dict
        """
        resp = await self._mutation_request(
            "DELETE", f"/api/projects/{project_id}/knowledge/{note_id}"
        )
        resp.raise_for_status()
        return resp.json()

    async def export_knowledge(self, project_id: str) -> dict[str, Any]:
        """Export project knowledge base as Obsidian-compatible markdown.

        Args:
            project_id: Project UUID

        Returns:
            Dict with status, path, note_count, project_name
        """
        resp = await self._mutation_request(
            "POST", f"/api/projects/{project_id}/knowledge/export"
        )
        resp.raise_for_status()
        return resp.json()

    async def reindex_knowledge(
        self, project_id: str, full: bool = False
    ) -> dict[str, Any]:
        """Rebuild/refresh the KB chunk index from the vault repo (slice 3).

        Args:
            project_id: Project UUID
            full: Re-embed the whole vault instead of only changed blobs

        Returns:
            Reindex summary dict (status, indexed_commit, upserted, ...)
        """
        resp = await self._mutation_request(
            "POST",
            f"/api/projects/{project_id}/knowledge/reindex",
            params={"full": str(full).lower()},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Job Promotion
    # =========================================================================

    async def promote_job(
        self,
        job_id: str,
        name: str,
        user_id: str,
        description: str | None = None,
        goal: str | None = None,
    ) -> dict[str, Any]:
        """Promote a completed job into a dedicated project.

        Creates a new project, seeds its repo from the job's branch,
        and moves the job to the new project.

        Args:
            job_id: Job UUID (must be completed)
            name: Name for the new project
            user_id: Owner user UUID
            description: Project description
            goal: Project goal

        Returns:
            Dict with status, project_id, project_name, job_id
        """
        body: dict[str, Any] = {"name": name, "user_id": user_id}
        if description:
            body["description"] = description
        if goal:
            body["goal"] = goal
        resp = await self._mutation_request(
            "POST", f"/api/jobs/{job_id}/promote", json=body
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Sudo Approval Gate
    # =========================================================================

    @_create_retry_decorator()
    async def list_sudo_requests(
        self,
        job_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[dict[str, Any]]:
        """List sudo approval requests.

        Args:
            job_id: Optional job ID filter.
            status: Optional status filter (pending, approved, denied, expired).
            limit: Maximum results (default 50).

        Returns:
            List of sudo approval request dicts.
        """
        params: dict[str, Any] = {"limit": limit}
        if job_id:
            params["job_id"] = job_id
        if status:
            params["status"] = status
        resp = await self._client.get("/api/sudo/requests", params=params)
        resp.raise_for_status()
        return resp.json()

    async def approve_sudo_request(
        self,
        request_id: str,
        reason: str = "",
    ) -> dict[str, Any]:
        """Approve a pending sudo request.

        Args:
            request_id: UUID of the sudo approval request.
            reason: Optional approval reason.

        Returns:
            Dict with id and status.
        """
        body: dict[str, Any] = {}
        if reason:
            body["reason"] = reason
        resp = await self._mutation_request(
            "POST", f"/api/sudo/requests/{request_id}/approve", json=body
        )
        resp.raise_for_status()
        return resp.json()

    async def deny_sudo_request(
        self,
        request_id: str,
        reason: str,
    ) -> dict[str, Any]:
        """Deny a pending sudo request.

        Args:
            request_id: UUID of the sudo approval request.
            reason: Denial reason (required).

        Returns:
            Dict with id and status.
        """
        resp = await self._mutation_request(
            "POST",
            f"/api/sudo/requests/{request_id}/deny",
            json={"reason": reason},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Messaging
    # =========================================================================

    @_create_retry_decorator()
    async def list_message_threads(self, job_id: str) -> dict[str, Any]:
        """List message threads for a job.

        Args:
            job_id: Job UUID.

        Returns:
            Dict with ``threads`` list.
        """
        resp = await self._client.get(f"/api/jobs/{job_id}/messages")
        resp.raise_for_status()
        return resp.json()

    async def reply_to_message(
        self,
        job_id: str,
        thread_id: str,
        message: str,
        urgent: bool = False,
    ) -> dict[str, Any]:
        """Send a reply to an agent message thread.

        Args:
            job_id: Job UUID.
            thread_id: Thread ID.
            message: Reply body.
            urgent: Whether to deliver as immediate interrupt.

        Returns:
            Dict with delivery strategy and sequence.
        """
        resp = await self._mutation_request(
            "POST",
            f"/api/jobs/{job_id}/messages/{thread_id}/reply",
            json={"message": message, "urgent": urgent},
        )
        resp.raise_for_status()
        return resp.json()

    async def officer_reply_to_job_message(
        self,
        job_id: str,
        thread_id: str,
        message: str,
    ) -> dict[str, Any]:
        """Answer a routed worker message as the commissioned officer."""
        resp = await self._mutation_request(
            "POST",
            f"/api/jobs/{job_id}/messages/{thread_id}/officer-reply",
            json={"message": message},
        )
        resp.raise_for_status()
        return resp.json()

    async def officer_escalate_job_message(
        self,
        job_id: str,
        thread_id: str,
        context: str | None = None,
    ) -> dict[str, Any]:
        """Escalate a routed worker message to the user with officer context."""
        resp = await self._mutation_request(
            "POST",
            f"/api/jobs/{job_id}/messages/{thread_id}/officer-escalate",
            json={"context": context},
        )
        resp.raise_for_status()
        return resp.json()

    async def officer_acknowledge_job_message(
        self,
        job_id: str,
        thread_id: str,
        note: str | None = None,
    ) -> dict[str, Any]:
        """Close an async routed worker message without a reply."""
        resp = await self._mutation_request(
            "POST",
            f"/api/jobs/{job_id}/messages/{thread_id}/officer-ack",
            json={"note": note},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Officers (the Legate's side — knowledge-base/knowledge/features/officer_legate_channel.md)
    # =========================================================================

    async def list_officers(self) -> dict[str, Any]:
        """Every post the caller can see, vacant ones included."""
        resp = await self._client.get("/api/officers")
        resp.raise_for_status()
        return resp.json()

    async def get_project_officer(self, project_id: str) -> dict[str, Any]:
        """One project's post: commission state, kit, wake, pages, digest."""
        resp = await self._client.get(f"/api/projects/{project_id}/officer")
        resp.raise_for_status()
        return resp.json()

    async def send_officer_note(self, project_id: str, message: str) -> dict[str, Any]:
        """Send the project's officer a one-way Legate note.

        The response says how it landed (``live`` / ``queued`` / ``held``) —
        callers must surface that rather than reporting a bare success.
        """
        resp = await self._mutation_request(
            "POST",
            f"/api/projects/{project_id}/officer/note",
            json={"message": message},
        )
        resp.raise_for_status()
        return resp.json()

    # =========================================================================
    # Persistent Threads
    # =========================================================================

    async def create_persistent_thread(
        self,
        config_name: str = "session_base",
        title: str = "Untitled Session",
        permission_mode: str = "supervised",
        project_id: str | None = None,
        project_ids: list[str] | None = None,
        datasource_ids: list[str] | None = None,
        model: str | None = None,
        temperature: float | None = None,
        workspace: dict[str, Any] | None = None,
        workspace_supplied: bool = False,
    ) -> dict[str, Any]:
        """Create a new persistent thread.

        A session has one project or none: pass ``project_id``.
        ``project_ids`` is deprecated and takes at most one project.

        ``datasource_ids`` is tri-state at the MCP boundary: an omitted field
        arrives here as the internal ``None`` sentinel and requests automatic
        defaults, while ``[]`` attaches no connectors and IDs request exactly
        that authorized selection. The MCP schema rejects explicit JSON null.

        workspace / workspace_supplied: the REST WorkspaceBinding (None = no
        workspace), sent only when supplied.

        Returns:
            Dict with ``thread_id`` and ``status``.
        """
        body: dict[str, Any] = {
            "config_name": config_name,
            "title": title,
            "permission_mode": permission_mode,
        }
        if project_id:
            body["project_id"] = project_id
        if project_ids:
            body["project_ids"] = project_ids
        if datasource_ids is not None:
            body["datasource_ids"] = datasource_ids
        else:
            body["use_datasource_defaults"] = True
        if model:
            body["model"] = model
        if temperature is not None:
            body["temperature"] = temperature
        if workspace_supplied:
            body["workspace"] = workspace
        resp = await self._mutation_request(
            "POST", "/api/persistent/threads", json=body
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def list_persistent_threads(
        self,
        project_id: str | None = None,
        status: str | None = None,
    ) -> dict[str, Any]:
        """List persistent threads for the authenticated user.

        Returns:
            Dict with ``threads`` list.
        """
        params: dict[str, Any] = {}
        if project_id:
            params["project_id"] = project_id
        if status:
            params["status"] = status
        resp = await self._client.get("/api/persistent/threads", params=params)
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_persistent_thread(self, thread_id: str) -> dict[str, Any]:
        """Get persistent thread details.

        Returns:
            Full thread dict.
        """
        resp = await self._client.get(f"/api/persistent/threads/{thread_id}")
        resp.raise_for_status()
        return resp.json()

    async def end_persistent_thread(
        self, thread_id: str, permanent: bool = False
    ) -> dict[str, Any]:
        """End or permanently delete a persistent thread.

        Returns:
            Dict with ``status`` ('ended' or 'deleted').
        """
        resp = await self._mutation_request(
            "DELETE",
            f"/api/persistent/threads/{thread_id}",
            params={"permanent": permanent},
        )
        resp.raise_for_status()
        return resp.json()

    async def resume_persistent_thread(
        self, thread_id: str, acknowledge: list[str] | None = None
    ) -> dict[str, Any]:
        """Resume an ended persistent thread.

        Raises ``SessionConfigDriftError`` when the session references
        connectors, projects, or grants that are no longer available. Pass
        their ids back as ``acknowledge`` to resume without them.

        Returns:
            Dict with ``status`` and ``thread_id``.
        """
        payload: dict[str, Any] = {}
        if acknowledge is not None:
            payload["acknowledge"] = acknowledge
        resp = await self._mutation_request(
            "POST", f"/api/persistent/threads/{thread_id}/resume", json=payload
        )
        if resp.status_code == 428:
            try:
                body = resp.json() if resp.content else {}
            except ValueError:
                body = {}
            detail = body.get("detail") if isinstance(body, dict) else None
            detail = detail if isinstance(detail, dict) else {}
            items = detail.get("drift")
            raise SessionConfigDriftError(items if isinstance(items, list) else [])
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_persistent_thread_messages(
        self,
        thread_id: str,
        limit: int = 200,
        offset: int = 0,
        before: str | None = None,
    ) -> dict[str, Any]:
        """Get message history for a persistent thread.

        ``before`` (ISO-8601) switches the endpoint to its backfill cursor —
        the NEWEST ``limit`` messages at or before that instant. Reaching the
        end of a long log by paging ``offset`` from zero is not a read
        strategy; the two are mutually exclusive server-side, so a cursor read
        sends no offset at all.

        Returns:
            Dict with ``messages``, ``total``, and ``thread_id``.
        """
        params: dict[str, Any] = {"limit": limit}
        if before:
            params["before"] = before
        else:
            params["offset"] = offset
        resp = await self._client.get(
            f"/api/persistent/threads/{thread_id}/messages",
            params=params,
        )
        resp.raise_for_status()
        return resp.json()

    @_create_retry_decorator()
    async def get_persistent_thread_ide(self, thread_id: str) -> dict[str, Any]:
        """Get IDE session status for a persistent thread.

        Returns:
            Dict with ``status``, ``code_server_url``, ``source``, ``gitea_url``.
        """
        resp = await self._client.get(f"/api/persistent/threads/{thread_id}/ide")
        resp.raise_for_status()
        return resp.json()
