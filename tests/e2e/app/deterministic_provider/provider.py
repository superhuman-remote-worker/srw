"""Run-scoped deterministic OpenAI-compatible model fixture.

The inference and control applications intentionally share only an in-memory
``ScenarioStore``.  ``run.py`` serves them on separate ports in one process so the
control surface can stay off the Kubernetes Service while still observing exactly
the calls made through the inference surface.

The store never retains request bodies, prompts, tool arguments, or credentials.
Its diagnostic records contain only run/model/endpoint/stream/outcome metadata.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import re
import shlex
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, AsyncIterator, Final, Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

CHAT_MODEL_ID = os.environ.get("E2E_CHAT_MODEL_ID", "e2e-chat")
if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,126}", CHAT_MODEL_ID):
    raise RuntimeError("E2E_CHAT_MODEL_ID must be a lowercase test model identifier")
EMBEDDING_MODEL_ID = "e2e-embedding"
RERANK_MODEL_ID = "qwen3-reranker-8b"
EMBEDDING_DIMENSIONS = 4096

SUPPORTED_SCENARIOS = frozenset(
    {
        "reply",
        "slow-stream",
        "error-once",
        "tool-call",
        "numbered-stream",
        "search-job",
        "fetch-job",
        "worker-job",
        "retained-sentinel-worker",
        "prepared-workspace-job",
    }
)
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{2,127}$")
_CORRELATION_RE = re.compile(
    r"(?<![A-Za-z0-9_-])E2E-([A-Za-z0-9][A-Za-z0-9_-]{2,127})(?![A-Za-z0-9_-])"
)
_DIAGNOSTIC_MODELS = frozenset({CHAT_MODEL_ID, EMBEDDING_MODEL_ID, RERANK_MODEL_ID})
_MAX_UNSCOPED_DIAGNOSTICS: Final = 4096


class ArmScenarioRequest(BaseModel):
    """Control-plane request used to create a fail-closed run namespace."""

    model_config = ConfigDict(extra="forbid")

    scenario: Literal[
        "reply",
        "slow-stream",
        "error-once",
        "tool-call",
        "numbered-stream",
        "search-job",
        "fetch-job",
        "worker-job",
        "retained-sentinel-worker",
        "prepared-workspace-job",
    ] = "reply"
    required_responses: int = Field(default=1, ge=1, le=100)
    chunk_delay_ms: int = Field(default=100, ge=0, le=2_000)
    sentinel_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")


@dataclass(frozen=True)
class CallDecision:
    run_id: str
    sequence: int
    scenario: str
    endpoint: str
    model: str
    stream: bool
    consume_required: bool
    chunk_delay_ms: int
    tool_phase: bool = False


@dataclass(frozen=True)
class ToolCallSpec:
    """One deterministic tool call returned without retaining its arguments."""

    name: str
    arguments: str


@dataclass
class PendingCall:
    decision: CallDecision
    started_at: float


@dataclass
class RunState:
    run_id: str
    scenario: str
    required_responses: int
    chunk_delay_ms: int
    consumed_required_responses: int = 0
    unexpected_calls: int = 0
    error_once_emitted: bool = False
    search_job_tool_steps: int = 0
    fetch_job_tool_steps: int = 0
    worker_job_tool_steps: int = 0
    sentinel_sha256: str | None = None
    next_sequence: int = 1
    counters: Counter[tuple[str, str, bool, str]] = field(default_factory=Counter)
    calls: list[dict[str, Any]] = field(default_factory=list)
    pending: dict[int, PendingCall] = field(default_factory=dict)

    @property
    def reserved_required_responses(self) -> int:
        """Count required-response slots already owned by in-flight calls."""

        return sum(
            pending.decision.consume_required for pending in self.pending.values()
        )

    @property
    def remaining_required_responses(self) -> int:
        return max(
            0,
            self.required_responses
            - self.consumed_required_responses
            - self.reserved_required_responses,
        )


class ScenarioError(Exception):
    """Expected fixture rejection with an OpenAI-shaped HTTP error."""

    def __init__(self, status_code: int, error_type: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.error_type = error_type
        self.message = message


class ScenarioStore:
    """Concurrency-safe, sanitized state for armed E2E scenarios."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._runs: dict[str, RunState] = {}
        self._unscoped_unexpected_calls = 0
        self._unscoped_calls: list[dict[str, Any]] = []

    async def arm(self, run_id: str, request: ArmScenarioRequest) -> dict[str, Any]:
        _validate_run_id(run_id)
        if (request.scenario == "retained-sentinel-worker") != (
            request.sentinel_sha256 is not None
        ):
            raise ScenarioError(
                422, "sentinel_contract_invalid", "Sentinel hash is required only for retained worker scenario."
            )
        async with self._lock:
            if run_id in self._runs:
                raise ScenarioError(
                    409,
                    "scenario_already_armed",
                    "A scenario is already armed for this run id; reset it first.",
                )
            self._runs[run_id] = RunState(
                run_id=run_id,
                scenario=request.scenario,
                required_responses=request.required_responses,
                chunk_delay_ms=request.chunk_delay_ms,
                sentinel_sha256=request.sentinel_sha256,
            )
            return self._serialize(self._runs[run_id])

    async def reset(self, run_id: str) -> bool:
        _validate_run_id(run_id)
        async with self._lock:
            return self._runs.pop(run_id, None) is not None

    async def state(self, run_id: str) -> dict[str, Any]:
        _validate_run_id(run_id)
        async with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                raise ScenarioError(404, "scenario_not_found", "No scenario is armed.")
            return self._serialize(state)

    async def overview(self) -> dict[str, Any]:
        async with self._lock:
            return {
                "runs": [
                    self._serialize(self._runs[key]) for key in sorted(self._runs)
                ],
                "unscoped_unexpected_calls": self._unscoped_unexpected_calls,
                "unscoped_calls_truncated": max(
                    0,
                    self._unscoped_unexpected_calls - len(self._unscoped_calls),
                ),
                "unscoped_calls": list(self._unscoped_calls),
            }

    def _record_unscoped_locked(
        self,
        *,
        endpoint: str,
        outcome: str,
        model: Any = None,
        stream: Any = False,
        correlation_run_ids: set[str] | None = None,
    ) -> None:
        """Record one rejected/unaccounted request using safe metadata only."""

        self._unscoped_unexpected_calls += 1
        sequence = self._unscoped_unexpected_calls
        diagnostic = {
            "sequence": sequence,
            "correlation_id": f"unscoped:{sequence}",
            "observed_at": datetime.now(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "endpoint": endpoint,
            "model": _diagnostic_model(model),
            "stream": stream if isinstance(stream, bool) else False,
            "outcome": outcome,
            "correlation_run_ids": sorted(correlation_run_ids or ()),
            "active_run_ids": sorted(self._runs),
        }
        if len(self._unscoped_calls) == _MAX_UNSCOPED_DIAGNOSTICS:
            del self._unscoped_calls[0]
        self._unscoped_calls.append(diagnostic)

    async def resolve_run(
        self,
        payload: dict[str, Any],
        *,
        endpoint: str,
    ) -> str:
        """Resolve one run without retaining any request content.

        A correlation token wins.  Calls without a token are accepted only when
        exactly one scenario is active; that is how lifecycle title/memory calls,
        embeddings, and reranking remain associated with the P0 run.
        """

        try:
            explicit_run_id = _metadata_run_id(payload)
        except ScenarioError as exc:
            async with self._lock:
                self._record_unscoped_locked(
                    endpoint=endpoint,
                    outcome=exc.error_type,
                    model=payload.get("model"),
                    stream=payload.get("stream", False),
                )
            raise
        discovered = _discover_run_ids(payload)
        if explicit_run_id:
            discovered.add(explicit_run_id)

        async with self._lock:
            if len(discovered) > 1:
                self._record_unscoped_locked(
                    endpoint=endpoint,
                    outcome="ambiguous_run",
                    model=payload.get("model"),
                    stream=payload.get("stream", False),
                    correlation_run_ids=discovered,
                )
                raise ScenarioError(
                    409,
                    "ambiguous_run",
                    "The request contains more than one E2E run correlation.",
                )
            if discovered:
                run_id = next(iter(discovered))
                if run_id not in self._runs:
                    self._record_unscoped_locked(
                        endpoint=endpoint,
                        outcome="scenario_not_armed",
                        model=payload.get("model"),
                        stream=payload.get("stream", False),
                        correlation_run_ids=discovered,
                    )
                    raise ScenarioError(
                        409,
                        "scenario_not_armed",
                        "No scenario is armed for the request correlation.",
                    )
                return run_id
            if len(self._runs) == 1:
                return next(iter(self._runs))

            outcome = (
                "run_correlation_required_no_active_scenario"
                if not self._runs
                else "run_correlation_required_multiple_active_scenarios"
            )
            self._record_unscoped_locked(
                endpoint=endpoint,
                outcome=outcome,
                model=payload.get("model"),
                stream=payload.get("stream", False),
            )
            message = (
                "No E2E scenario is armed."
                if not self._runs
                else "The request has no run correlation while multiple scenarios are armed."
            )
            raise ScenarioError(409, "run_correlation_required", message)

    async def record_unexpected_request(
        self,
        *,
        endpoint: str,
        outcome: str,
        payload: dict[str, Any] | None = None,
        run_id: str | None = None,
        model: Any = None,
        stream: Any = False,
    ) -> None:
        """Account a rejected call without retaining request-controlled content.

        Known routes pass their already-resolved ``run_id``. Unknown routes and
        malformed bodies use the same correlation rules where possible; an
        ambiguous or unarmed request increments the global unscoped counter.
        """

        candidates: set[str] = set()
        correlation_invalid = False
        if run_id is not None:
            candidates.add(run_id)
        elif payload is not None:
            try:
                explicit = _metadata_run_id(payload)
            except ScenarioError:
                correlation_invalid = True
            else:
                candidates = _discover_run_ids(payload)
                if explicit:
                    candidates.add(explicit)

        safe_model = _diagnostic_model(model)
        safe_stream = stream if isinstance(stream, bool) else False
        async with self._lock:
            target: RunState | None = None
            if not correlation_invalid and len(candidates) == 1:
                target = self._runs.get(next(iter(candidates)))
            elif not correlation_invalid and not candidates and len(self._runs) == 1:
                target = next(iter(self._runs.values()))

            if target is None:
                self._record_unscoped_locked(
                    endpoint=endpoint,
                    outcome=outcome,
                    model=model,
                    stream=stream,
                    correlation_run_ids=candidates,
                )
                return
            self._record_immediate(
                target,
                endpoint=endpoint,
                model=safe_model,
                stream=safe_stream,
                outcome=outcome,
                unexpected=True,
            )

    async def begin_call(
        self,
        *,
        run_id: str,
        endpoint: str,
        model: str,
        stream: bool,
        consume_required: bool,
        tool_phase: bool = False,
    ) -> CallDecision:
        """Validate and register a call, or raise after accounting a rejection."""

        async with self._lock:
            state = self._runs.get(run_id)
            if state is None:
                self._record_unscoped_locked(
                    endpoint=endpoint,
                    outcome="scenario_reset_before_start",
                    model=model,
                    stream=stream,
                    correlation_run_ids={run_id},
                )
                raise ScenarioError(
                    409,
                    "scenario_not_armed",
                    "The scenario was reset before the request could start.",
                )
            expected_models = {
                "chat.completions": {CHAT_MODEL_ID},
                "embeddings": {EMBEDDING_MODEL_ID},
                # The production memory plugin defaults to this canonical reranker
                # id while deriving /rerank from the embedding transport.  The E2E
                # overlay may explicitly use e2e-embedding instead; both are known,
                # fixture-owned route contracts and neither weakens chat routing.
                "rerank": {EMBEDDING_MODEL_ID, RERANK_MODEL_ID},
            }[endpoint]
            if model not in expected_models:
                self._record_immediate(
                    state,
                    endpoint=endpoint,
                    model=_diagnostic_model(model),
                    stream=stream,
                    outcome="unexpected_model",
                    unexpected=True,
                )
                raise ScenarioError(
                    400,
                    "unknown_model",
                    f"Model is not available for {endpoint}.",
                )

            if consume_required and state.remaining_required_responses == 0:
                self._record_immediate(
                    state,
                    endpoint=endpoint,
                    model=model,
                    stream=stream,
                    outcome="unexpected_exhausted",
                    unexpected=True,
                )
                raise ScenarioError(
                    409,
                    "scenario_exhausted",
                    "All required responses for this scenario were already consumed.",
                )

            if (
                consume_required
                and state.scenario == "error-once"
                and not state.error_once_emitted
            ):
                state.error_once_emitted = True
                self._record_immediate(
                    state,
                    endpoint=endpoint,
                    model=model,
                    stream=stream,
                    outcome="retryable_error",
                    unexpected=False,
                )
                raise ScenarioError(
                    503,
                    "fixture_retryable_error",
                    "The armed error-once scenario rejected its first response.",
                )

            sequence = state.next_sequence
            state.next_sequence += 1
            decision = CallDecision(
                run_id=run_id,
                sequence=sequence,
                scenario=state.scenario,
                endpoint=endpoint,
                model=model,
                stream=stream,
                consume_required=consume_required,
                chunk_delay_ms=state.chunk_delay_ms,
                tool_phase=tool_phase,
            )
            state.pending[sequence] = PendingCall(
                decision=decision, started_at=time.monotonic()
            )
            return decision

    async def finish_call(self, decision: CallDecision, outcome: str) -> None:
        """Finish a registered call exactly once and update grouped counters."""

        async with self._lock:
            state = self._runs.get(decision.run_id)
            if state is None:
                # Reset is allowed only after clients are closed; if a caller violates
                # that order there is intentionally no recreated/tombstoned state.
                # Keep the successful/rejected HTTP request globally visible instead.
                self._record_unscoped_locked(
                    endpoint=decision.endpoint,
                    outcome="scenario_reset_before_finish",
                    model=decision.model,
                    stream=decision.stream,
                    correlation_run_ids={decision.run_id},
                )
                return
            pending = state.pending.pop(decision.sequence, None)
            if pending is None:
                return
            if outcome == "success" and decision.consume_required:
                state.consumed_required_responses += 1
            if (
                outcome == "success"
                and decision.scenario == "search-job"
                and decision.tool_phase
            ):
                state.search_job_tool_steps += 1
            if (
                outcome == "success"
                and decision.scenario == "fetch-job"
                and decision.tool_phase
            ):
                state.fetch_job_tool_steps += 1
            if (
                outcome == "success"
                and decision.scenario in {"worker-job", "prepared-workspace-job", "retained-sentinel-worker"}
                and decision.tool_phase
            ):
                state.worker_job_tool_steps += 1
            if outcome != "success":
                state.unexpected_calls += 1
            duration_ms = max(0, int((time.monotonic() - pending.started_at) * 1000))
            state.counters[
                (decision.model, decision.endpoint, decision.stream, outcome)
            ] += 1
            state.calls.append(
                {
                    "run_id": decision.run_id,
                    "sequence": decision.sequence,
                    "correlation_id": f"{decision.run_id}:{decision.sequence}",
                    "model": decision.model,
                    "endpoint": decision.endpoint,
                    "stream": decision.stream,
                    "outcome": outcome,
                    "duration_ms": duration_ms,
                }
            )

    def _record_immediate(
        self,
        state: RunState,
        *,
        endpoint: str,
        model: str,
        stream: bool,
        outcome: str,
        unexpected: bool,
    ) -> None:
        sequence = state.next_sequence
        state.next_sequence += 1
        state.counters[(model, endpoint, stream, outcome)] += 1
        if unexpected:
            state.unexpected_calls += 1
        state.calls.append(
            {
                "run_id": state.run_id,
                "sequence": sequence,
                "correlation_id": f"{state.run_id}:{sequence}",
                "model": model,
                "endpoint": endpoint,
                "stream": stream,
                "outcome": outcome,
                "duration_ms": 0,
            }
        )

    @staticmethod
    def _serialize(state: RunState) -> dict[str, Any]:
        counters = [
            {
                "run_id": state.run_id,
                "model": model,
                "endpoint": endpoint,
                "stream": stream,
                "outcome": outcome,
                "count": count,
            }
            for (model, endpoint, stream, outcome), count in sorted(
                state.counters.items(), key=lambda item: item[0]
            )
        ]
        return {
            "run_id": state.run_id,
            "scenario": state.scenario,
            "required_responses": state.required_responses,
            "consumed_required_responses": state.consumed_required_responses,
            "reserved_required_responses": state.reserved_required_responses,
            "remaining_required_responses": state.remaining_required_responses,
            "search_job_tool_steps": state.search_job_tool_steps,
            "fetch_job_tool_steps": state.fetch_job_tool_steps,
            "worker_job_tool_steps": state.worker_job_tool_steps,
            "sentinel_sha256": state.sentinel_sha256,
            "unexpected_count": state.unexpected_calls,
            "pending_calls": len(state.pending),
            "counters": counters,
            "calls": list(state.calls),
        }


def create_inference_app(
    store: ScenarioStore, *, inference_api_key: str | None
) -> FastAPI:
    """Create the inference-only application.

    ``inference_api_key`` is mandatory in the deployed runner.  Passing ``None``
    is useful only for a narrowly scoped local contract test.
    """

    app = FastAPI(
        title="SRW deterministic E2E provider",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def authenticate_inference(request: Request, call_next):
        if request.url.path != "/health" and inference_api_key is not None:
            if not _valid_bearer(
                request.headers.get("authorization"), inference_api_key
            ):
                await store.record_unexpected_request(
                    endpoint="authentication",
                    outcome="unexpected_unauthorized",
                )
                return _error_response(401, "invalid_api_key", "Invalid API key.")
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.exception_handler(StarletteHTTPException)
    async def account_unknown_inference_route(
        request: Request, exc: StarletteHTTPException
    ) -> JSONResponse:
        if exc.status_code not in {404, 405}:
            return _error_response(
                exc.status_code, "http_error", "Inference request failed."
            )
        payload = await _optional_json_object(request)
        outcome = "unexpected_route" if exc.status_code == 404 else "unexpected_method"
        await store.record_unexpected_request(
            endpoint="unknown_route",
            outcome=outcome,
            payload=payload,
            model=payload.get("model") if payload else None,
            stream=payload.get("stream", False) if payload else False,
        )
        error_type = "unknown_route" if exc.status_code == 404 else "method_not_allowed"
        return _error_response(
            exc.status_code, error_type, "Inference route is not supported."
        )

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/models")
    async def models() -> dict[str, Any]:
        return {
            "object": "list",
            "data": [
                _model_object(CHAT_MODEL_ID),
                _model_object(EMBEDDING_MODEL_ID),
            ],
        }

    @app.post("/v1/chat/completions")
    async def chat_completions(request: Request):
        try:
            payload = await _accounted_json_object(
                request, store=store, endpoint="chat.completions"
            )
            run_id = await store.resolve_run(payload, endpoint="chat.completions")
            try:
                model = _required_string(payload, "model")
                messages = payload.get("messages")
                if not isinstance(messages, list) or not messages:
                    raise ScenarioError(
                        400, "invalid_request", "messages must be a non-empty list."
                    )
                stream = payload.get("stream", False)
                if not isinstance(stream, bool):
                    raise ScenarioError(
                        400, "invalid_request", "stream must be a boolean."
                    )
            except ScenarioError as exc:
                await store.record_unexpected_request(
                    run_id=run_id,
                    endpoint="chat.completions",
                    outcome=f"unexpected_{exc.error_type}",
                    model=payload.get("model"),
                    stream=payload.get("stream", False),
                )
                raise

            structured_name = _structured_output_name(payload)
            if structured_name not in _MODELLED_SCHEMAS:
                await _account_rejection(
                    store,
                    run_id=run_id,
                    endpoint="chat.completions",
                    model=model,
                    stream=stream,
                    outcome="unexpected_schema",
                )
                raise ScenarioError(
                    422,
                    "unsupported_schema",
                    "The requested structured-output schema is not supported by this fixture.",
                )

            state = await store.state(run_id)
            tool_call: ToolCallSpec | None = None
            if structured_name is None and state["scenario"] == "tool-call":
                has_tool_result = any(
                    isinstance(message, dict) and message.get("role") == "tool"
                    for message in messages
                )
                if not has_tool_result:
                    tool_call = ToolCallSpec(
                        name=_first_tool_name(payload) or "e2e_tool",
                        arguments="{}",
                    )
            elif structured_name is None and state["scenario"] == "search-job":
                tool_names = _tool_names(payload)
                if tool_names & {
                    "read_file",
                    "todo_complete",
                    "next_phase_todos",
                    "web_search",
                    "job_complete",
                }:
                    tool_call = _search_job_tool_call(
                        state["search_job_tool_steps"], run_id
                    )
                if tool_call is not None and tool_call.name not in tool_names:
                    await _account_rejection(
                        store,
                        run_id=run_id,
                        endpoint="chat.completions",
                        model=model,
                        stream=stream,
                        outcome="unexpected_required_tool_missing",
                    )
                    raise ScenarioError(
                        422,
                        "required_tool_missing",
                        "The search-job scenario requires a tool that was not bound.",
                    )
            elif structured_name is None and state["scenario"] in {
                "worker-job",
                "retained-sentinel-worker",
                "prepared-workspace-job",
            }:
                tool_names = _tool_names(payload)
                if tool_names & {
                    "read_file",
                    "todo_complete",
                    "next_phase_todos",
                    "job_complete",
                    "run_command",
                }:
                    if state["scenario"] == "prepared-workspace-job":
                        tool_call = _prepared_workspace_tool_call(
                            state["worker_job_tool_steps"], run_id, messages
                        )
                    elif state["scenario"] == "retained-sentinel-worker":
                        tool_call = _retained_sentinel_tool_call(
                            state["worker_job_tool_steps"], run_id,
                            state["sentinel_sha256"], messages,
                        )
                    else:
                        tool_call = _worker_job_tool_call(
                            state["worker_job_tool_steps"], run_id
                        )
                if tool_call is not None and tool_call.name not in tool_names:
                    await _account_rejection(
                        store,
                        run_id=run_id,
                        endpoint="chat.completions",
                        model=model,
                        stream=stream,
                        outcome="unexpected_required_tool_missing",
                    )
                    raise ScenarioError(
                        422,
                        "required_tool_missing",
                        "The worker-job scenario requires a tool that was not bound.",
                    )
            elif structured_name is None and state["scenario"] == "fetch-job":
                tool_names = _tool_names(payload)
                if tool_names & {
                    "read_file",
                    "todo_complete",
                    "next_phase_todos",
                    "extract_webpage",
                    "crawl_website",
                    "job_complete",
                }:
                    tool_call = _fetch_job_tool_call(
                        state["fetch_job_tool_steps"], run_id
                    )
                if tool_call is not None and tool_call.name not in tool_names:
                    await _account_rejection(
                        store,
                        run_id=run_id,
                        endpoint="chat.completions",
                        model=model,
                        stream=stream,
                        outcome="unexpected_required_tool_missing",
                    )
                    raise ScenarioError(
                        422,
                        "required_tool_missing",
                        "The fetch-job scenario requires a tool that was not bound.",
                    )

            tool_phase = tool_call is not None
            consume_required = structured_name is None and not tool_phase
            decision = await store.begin_call(
                run_id=run_id,
                endpoint="chat.completions",
                model=model,
                stream=stream,
                consume_required=consume_required,
                tool_phase=tool_phase,
            )

            if structured_name is not None:
                content = _structured_content(structured_name, run_id)
                finish_reason = "stop"
            elif tool_phase:
                content = ""
                finish_reason = "tool_calls"
            else:
                content = _scenario_reply(decision)
                finish_reason = "stop"

            if stream:
                return StreamingResponse(
                    _stream_completion(
                        store=store,
                        decision=decision,
                        content=content,
                        finish_reason=finish_reason,
                        tool_call=tool_call,
                    ),
                    media_type="text/event-stream",
                    headers={
                        "Cache-Control": "no-cache, no-store",
                        "Connection": "keep-alive",
                        "X-Accel-Buffering": "no",
                    },
                )

            response = _non_stream_completion(
                decision=decision,
                content=content,
                finish_reason=finish_reason,
                tool_call=tool_call,
            )
            await store.finish_call(decision, "success")
            return response
        except ScenarioError as exc:
            return _scenario_error_response(exc)

    @app.post("/v1/embeddings")
    async def embeddings(request: Request):
        try:
            payload = await _accounted_json_object(
                request, store=store, endpoint="embeddings"
            )
            run_id = await store.resolve_run(payload, endpoint="embeddings")
            try:
                model = _required_string(payload, "model")
                raw_input = payload.get("input")
                inputs = _embedding_inputs(raw_input)
            except ScenarioError as exc:
                await store.record_unexpected_request(
                    run_id=run_id,
                    endpoint="embeddings",
                    outcome=f"unexpected_{exc.error_type}",
                    model=payload.get("model"),
                )
                raise
            decision = await store.begin_call(
                run_id=run_id,
                endpoint="embeddings",
                model=model,
                stream=False,
                consume_required=False,
            )
            data = [
                {
                    "object": "embedding",
                    "embedding": _stable_embedding(value),
                    "index": index,
                }
                for index, value in enumerate(inputs)
            ]
            await store.finish_call(decision, "success")
            return {
                "object": "list",
                "data": data,
                "model": model,
                "usage": {"prompt_tokens": 0, "total_tokens": 0},
            }
        except ScenarioError as exc:
            return _scenario_error_response(exc)

    @app.post("/v1/rerank")
    async def rerank(request: Request):
        try:
            payload = await _accounted_json_object(
                request, store=store, endpoint="rerank"
            )
            run_id = await store.resolve_run(payload, endpoint="rerank")
            try:
                model = _required_string(payload, "model")
                query = payload.get("query")
                documents = payload.get("documents")
                if (
                    not isinstance(query, str)
                    or not isinstance(documents, list)
                    or not all(isinstance(document, str) for document in documents)
                ):
                    raise ScenarioError(
                        400,
                        "invalid_request",
                        "query must be a string and documents must be a list of strings.",
                    )
            except ScenarioError as exc:
                await store.record_unexpected_request(
                    run_id=run_id,
                    endpoint="rerank",
                    outcome=f"unexpected_{exc.error_type}",
                    model=payload.get("model"),
                )
                raise
            decision = await store.begin_call(
                run_id=run_id,
                endpoint="rerank",
                model=model,
                stream=False,
                consume_required=False,
            )
            results = _rerank_results(query, documents)
            await store.finish_call(decision, "success")
            return {"id": _response_id("rerank"), "results": results}
        except ScenarioError as exc:
            return _scenario_error_response(exc)

    return app


def create_control_app(store: ScenarioStore, *, control_token: str) -> FastAPI:
    """Create a token-protected control application for the unserviced port."""

    if not control_token:
        raise ValueError("control_token must not be empty")

    app = FastAPI(
        title="SRW deterministic E2E provider control plane",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )

    @app.middleware("http")
    async def authenticate_control(request: Request, call_next):
        if not _valid_bearer(request.headers.get("authorization"), control_token):
            return _error_response(
                401, "invalid_control_token", "Invalid control token."
            )
        response = await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/control/health")
    async def control_health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/control/scenarios")
    async def runs():
        return await store.overview()

    @app.post("/control/scenarios/{run_id}/arm", status_code=201)
    async def arm(run_id: str, arm_request: ArmScenarioRequest):
        try:
            return await store.arm(run_id, arm_request)
        except ScenarioError as exc:
            return _scenario_error_response(exc)

    @app.get("/control/scenarios/{run_id}")
    async def run_state(run_id: str):
        try:
            return await store.state(run_id)
        except ScenarioError as exc:
            return _scenario_error_response(exc)

    @app.delete("/control/scenarios/{run_id}")
    async def reset(run_id: str):
        try:
            removed = await store.reset(run_id)
        except ScenarioError as exc:
            return _scenario_error_response(exc)
        if not removed:
            return _error_response(404, "scenario_not_found", "No scenario is armed.")
        return {"run_id": run_id, "reset": True}

    return app


async def _account_rejection(
    store: ScenarioStore,
    *,
    run_id: str,
    endpoint: str,
    model: str,
    stream: bool,
    outcome: str,
) -> None:
    decision = await store.begin_call(
        run_id=run_id,
        endpoint=endpoint,
        model=model,
        stream=stream,
        consume_required=False,
    )
    await store.finish_call(decision, outcome)


def _model_object(model_id: str) -> dict[str, Any]:
    return {
        "id": model_id,
        "object": "model",
        "created": 0,
        "owned_by": "srw-e2e",
    }


async def _json_object(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise ScenarioError(
            400, "invalid_json", "Request body must be valid JSON."
        ) from exc
    if not isinstance(payload, dict):
        raise ScenarioError(
            400, "invalid_request", "Request body must be a JSON object."
        )
    return payload


async def _optional_json_object(request: Request) -> dict[str, Any] | None:
    try:
        body = await request.body()
    except Exception:
        return None
    if not body:
        return None
    try:
        payload = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


async def _accounted_json_object(
    request: Request, *, store: ScenarioStore, endpoint: str
) -> dict[str, Any]:
    try:
        return await _json_object(request)
    except ScenarioError as exc:
        await store.record_unexpected_request(
            endpoint=endpoint,
            outcome=f"unexpected_{exc.error_type}",
        )
        raise


def _diagnostic_model(value: Any) -> str:
    if isinstance(value, str) and value in _DIAGNOSTIC_MODELS:
        return value
    return "<invalid>"


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise ScenarioError(
            400, "invalid_request", f"{key} must be a non-empty string."
        )
    return value


def _validate_run_id(run_id: str) -> None:
    if not _RUN_ID_RE.fullmatch(run_id):
        raise ScenarioError(
            400,
            "invalid_run_id",
            "run_id must be 3-128 characters using letters, digits, underscore, or hyphen.",
        )


def _metadata_run_id(payload: dict[str, Any]) -> str | None:
    metadata = payload.get("metadata")
    if not isinstance(metadata, dict) or "e2e_run_id" not in metadata:
        return None
    run_id = metadata["e2e_run_id"]
    if not isinstance(run_id, str):
        raise ScenarioError(
            400, "invalid_run_id", "metadata.e2e_run_id must be a string."
        )
    _validate_run_id(run_id)
    return run_id


def _discover_run_ids(payload: dict[str, Any]) -> set[str]:
    """Find safe correlation tokens without keeping any matched source text."""

    run_ids: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, str):
            run_ids.update(match.group(1) for match in _CORRELATION_RE.finditer(value))
        elif isinstance(value, list):
            for item in value:
                visit(item)
        elif isinstance(value, dict):
            for key, item in value.items():
                # response schemas can contain arbitrary examples/descriptions; they
                # are not conversation correlations and need not be scanned.
                if key not in {"response_format"}:
                    visit(item)

    for field_name in ("messages", "input", "query", "documents"):
        if field_name in payload:
            visit(payload[field_name])
    return run_ids


#: Structured-output schemas this fixture answers deterministically. Anything
#: else is a real unexpected call and must stay a 422 — the set is deliberately
#: an allowlist, not a fallback, so a NEW schema shows up as a rejection rather
#: than as a silently fabricated answer.
_MODELLED_SCHEMAS: Final = frozenset(
    {
        None,
        "ConversationTitle",
        "ExtractedMemories",
        "AssemblyResult",
        "ConversationSummary",
        "CurationResult",
        "KnowledgeAssemblyResult",
    }
)


def _structured_output_name(payload: dict[str, Any]) -> str | None:
    response_format = payload.get("response_format")
    if not isinstance(response_format, dict):
        return None
    response_type = response_format.get("type")
    if response_type not in {"json_schema", "json_object"}:
        return None
    json_schema = response_format.get("json_schema")
    if not isinstance(json_schema, dict):
        return "unknown"
    name = json_schema.get("name")
    schema = json_schema.get("schema")
    if isinstance(name, str) and name:
        return name
    if isinstance(schema, dict):
        title = schema.get("title")
        if isinstance(title, str) and title:
            return title
        properties = schema.get("properties")
        if isinstance(properties, dict):
            if set(properties) == {"title"}:
                return "ConversationTitle"
            if "memories" in properties:
                return "ExtractedMemories"
    return "unknown"


def _structured_content(schema_name: str, run_id: str) -> str:
    if schema_name == "ConversationTitle":
        return json.dumps(
            {"title": f"E2E-{run_id} deterministic assistant reply session"},
            separators=(",", ":"),
        )
    if schema_name == "ExtractedMemories":
        return '{"memories":[]}'
    if schema_name == "AssemblyResult":
        # Memory Light's assembler (`AssembleMemoriesTask`) runs as a
        # non-blocking auxiliary task during any sufficiently long worker job.
        # It is not part of any scenario's assertion, but leaving its schema
        # unmodelled made every worker run record two `unexpected_schema`
        # rejections that the agent then logged as "Memory assembly failed
        # (non-fatal)" — real degradation, and noise that hides a genuine
        # unexpected call. A no-op review is the honest deterministic answer.
        return json.dumps(
            {
                "actions_taken": [],
                "gaps_identified": [],
                "summary": f"E2E-{run_id} deterministic no-op assembly review.",
            },
            separators=(",", ":"),
        )
    if schema_name == "ConversationSummary":
        # Context compaction (`SummarizeTask`) folds a long worker conversation
        # through this schema. A loop member that runs long enough to compact
        # asked for it and got a 422, which the agent retried three times and
        # then degraded to trimming — real behaviour change, and two
        # `unexpected_schema` rejections that hide a genuine unexpected call.
        # The deterministic answer is a valid, content-free summary.
        return json.dumps(
            {
                "summary": f"E2E-{run_id} deterministic conversation summary.",
                "tasks_completed": "",
                "tasks_in_progress": "",
                "key_decisions": "",
                "current_state": "",
                "blockers": "",
                "critical_facts": "",
                "state_changes": "",
                "pinned_instructions": "",
                "identity_anchor": "",
            },
            separators=(",", ":"),
        )
    if schema_name == "CurationResult":
        return json.dumps(
            {
                "notes_created": 0,
                "notes_updated": 0,
                "summary": f"E2E-{run_id} deterministic no-op curation.",
            },
            separators=(",", ":"),
        )
    if schema_name == "KnowledgeAssemblyResult":
        return json.dumps(
            {
                "notes_refreshed": 0,
                "notes_superseded": 0,
                "notes_merged": 0,
                "notes_archived": 0,
                "summary": f"E2E-{run_id} deterministic no-op convergence.",
            },
            separators=(",", ":"),
        )
    raise AssertionError(f"unsupported structured schema: {schema_name}")


def _scenario_reply(decision: CallDecision) -> str:
    if decision.scenario == "numbered-stream":
        return f"E2E_PART:1|E2E_PART:2|E2E_REPLY:{decision.run_id}"
    return f"E2E_REPLY:{decision.run_id}"


def _completion_chunks(content: str, scenario: str) -> list[str]:
    if scenario == "numbered-stream":
        return content.split("|")
    if not content:
        return []
    if content.startswith("E2E_REPLY:"):
        return ["E2E_", "REPLY:", content.removeprefix("E2E_REPLY:")]
    midpoint = max(1, len(content) // 2)
    return [content[:midpoint], content[midpoint:]]


async def _stream_completion(
    *,
    store: ScenarioStore,
    decision: CallDecision,
    content: str,
    finish_reason: str,
    tool_call: ToolCallSpec | None,
) -> AsyncIterator[str]:
    completion_id = _response_id("chatcmpl")
    created = int(time.time())

    async def emit(payload: dict[str, Any]) -> str:
        if decision.scenario == "slow-stream" and decision.chunk_delay_ms:
            await asyncio.sleep(decision.chunk_delay_ms / 1000)
        return f"data: {json.dumps(payload, separators=(',', ':'))}\n\n"

    def chunk(choices: list[dict[str, Any]], usage: dict[str, int] | None = None):
        result: dict[str, Any] = {
            "id": completion_id,
            "object": "chat.completion.chunk",
            "created": created,
            "model": decision.model,
            "choices": choices,
        }
        if usage is not None:
            result["usage"] = usage
        return result

    try:
        yield await emit(
            chunk(
                [
                    {
                        "index": 0,
                        "delta": {"role": "assistant", "content": ""},
                        "finish_reason": None,
                    }
                ]
            )
        )
        if decision.tool_phase:
            yield await emit(
                chunk(
                    [
                        {
                            "index": 0,
                            "delta": {
                                "tool_calls": [
                                    {
                                        "index": 0,
                                        "id": f"call_{decision.sequence}",
                                        "type": "function",
                                        "function": {
                                            "name": (
                                                tool_call.name
                                                if tool_call is not None
                                                else "e2e_tool"
                                            ),
                                            "arguments": (
                                                tool_call.arguments
                                                if tool_call is not None
                                                else "{}"
                                            ),
                                        },
                                    }
                                ]
                            },
                            "finish_reason": None,
                        }
                    ]
                )
            )
        else:
            for piece in _completion_chunks(content, decision.scenario):
                yield await emit(
                    chunk(
                        [
                            {
                                "index": 0,
                                "delta": {"content": piece},
                                "finish_reason": None,
                            }
                        ]
                    )
                )
        yield await emit(
            chunk(
                [
                    {
                        "index": 0,
                        "delta": {},
                        "finish_reason": finish_reason,
                    }
                ]
            )
        )
        yield await emit(
            chunk(
                [],
                usage={"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
            )
        )
        yield "data: [DONE]\n\n"
    except (asyncio.CancelledError, GeneratorExit):
        await store.finish_call(decision, "cancelled")
        raise
    except Exception:
        await store.finish_call(decision, "fixture_error")
        raise
    else:
        await store.finish_call(decision, "success")


def _non_stream_completion(
    *,
    decision: CallDecision,
    content: str,
    finish_reason: str,
    tool_call: ToolCallSpec | None,
) -> dict[str, Any]:
    message: dict[str, Any] = {"role": "assistant", "content": content}
    if decision.tool_phase:
        message["content"] = None
        message["tool_calls"] = [
            {
                "id": f"call_{decision.sequence}",
                "type": "function",
                "function": {
                    "name": tool_call.name if tool_call is not None else "e2e_tool",
                    "arguments": (
                        tool_call.arguments if tool_call is not None else "{}"
                    ),
                },
            }
        ]
    return {
        "id": _response_id("chatcmpl"),
        "object": "chat.completion",
        "created": int(time.time()),
        "model": decision.model,
        "choices": [
            {
                "index": 0,
                "message": message,
                "finish_reason": finish_reason,
            }
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1, "total_tokens": 2},
    }


def _first_tool_name(payload: dict[str, Any]) -> str | None:
    tools = payload.get("tools")
    if not isinstance(tools, list):
        return None
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function")
        if isinstance(function, dict) and isinstance(function.get("name"), str):
            return function["name"]
    return None


def _tool_names(payload: dict[str, Any]) -> set[str]:
    tools = payload.get("tools")
    if not isinstance(tools, list):
        return set()
    return {
        name
        for tool in tools
        if isinstance(tool, dict)
        and isinstance((function := tool.get("function")), dict)
        and isinstance((name := function.get("name")), str)
    }


def _search_job_tool_call(step: int, run_id: str) -> ToolCallSpec:
    """Drive the real phased agent through one off-pod search and completion."""

    if step == 0:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/todo-guide/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step < 5:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: live-gate strategic setup step."},
                separators=(",", ":"),
            ),
        )
    if step == 5:
        return ToolCallSpec(
            name="next_phase_todos",
            arguments=json.dumps(
                {
                    "todos": [
                        "Run one live SearXNG web search for the official documentation.",
                        "Verify the search answer and close the research phase.",
                    ],
                    "phase_name": "SearXNG live search gate",
                },
                separators=(",", ":"),
            ),
        )
    if step == 6:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: tactical search phase staged."},
                separators=(",", ":"),
            ),
        )
    if step == 7:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/verify-before-done/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step == 8:
        return ToolCallSpec(
            name="web_search",
            arguments=json.dumps(
                {
                    "query": "SearXNG official documentation",
                    "max_results": 3,
                },
                separators=(",", ":"),
            ),
        )
    if step in {9, 10}:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: SearXNG tactical research step."},
                separators=(",", ":"),
            ),
        )
    if step == 11:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/verify-before-done/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step == 12:
        return ToolCallSpec(
            name="job_complete",
            arguments=json.dumps(
                {
                    "summary": (
                        f"Completed the SearXNG live search gate for E2E-{run_id}."
                    ),
                    "deliverables": [],
                    "confidence": 1.0,
                },
                separators=(",", ":"),
            ),
        )
    return ToolCallSpec(
        name="todo_complete",
        arguments=json.dumps(
            {"completion_note": "PASS: SearXNG live search gate completed."},
            separators=(",", ":"),
        ),
    )


def _prepared_workspace_tool_call(
    step: int, run_id: str, messages: list[dict[str, Any]]
) -> ToolCallSpec:
    """Require an actual prepared-workspace shell result before completion."""
    if step == 6:
        existing = "-f" if run_id.endswith("-reuse") else "! -e"
        command = "\n".join(
            [
                "set -eu",
                'test "$(srw-cache-check)" = srw-prepared-tool-v1',
                'test "$(cat .srw-initialize-count)" = initialized',
                f"test {existing} .srw-execution-marker",
                "printf '%s\\n' " + shlex.quote(run_id) + " > .srw-execution-marker",
                "printf 'SRW_PREPARED_PASS:%s\\n' " + shlex.quote(run_id),
            ]
        )
        if "-job-sudo-" in run_id:
            # Keep sudo as the first word to exercise the harness gate. This
            # only queries the installed version; it runs no privileged command.
            command = "sudo --version >/dev/null && (\n" + command + "\n)"
        return ToolCallSpec(
            name="run_command",
            arguments=json.dumps(
                {"command": command, "working_dir": ".", "timeout": 30},
                separators=(",", ":"),
            ),
        )
    if step == 7:
        previous = next(
            (
                message
                for message in reversed(messages)
                if isinstance(message, dict) and message.get("role") == "tool"
            ),
            {},
        )
        output = previous.get("content")
        if (
            not isinstance(output, str)
            or f"SRW_PREPARED_PASS:{run_id}" not in output.splitlines()
            or "Exit code: 0" not in output.splitlines()
        ):
            raise ScenarioError(
                422,
                "workspace_proof_missing",
                "Prepared workspace execution did not return the required proof.",
            )
    return _worker_job_tool_call(step if step < 6 else step - 1, run_id)


def _retained_sentinel_tool_call(
    step: int, run_id: str, digest: str, messages: list[dict[str, Any]]
) -> ToolCallSpec:
    """Read the exact run-owned file in the real worker before completion."""
    if step == 6:
        path = f".srw-a1-gate/{run_id}/sentinel"
        command = "\n".join(
            (
                "set -eu",
                f"test -f {shlex.quote(path)}",
                f"actual=$(sha256sum {shlex.quote(path)})",
                f"test \"${{actual%% *}}\" = {shlex.quote(digest)}",
                f"printf '%s\\n' {shlex.quote('SRW_A1_SENTINEL_PASS:' + run_id)}",
            )
        )
        return ToolCallSpec(
            name="run_command",
            arguments=json.dumps(
                {"command": command, "working_dir": ".", "timeout": 30},
                separators=(",", ":"),
            ),
        )
    if step == 7:
        previous = next(
            (
                message
                for message in reversed(messages)
                if isinstance(message, dict) and message.get("role") == "tool"
            ),
            {},
        )
        output = previous.get("content")
        if (
            not isinstance(output, str)
            or "Exit code: 0" not in output.splitlines()
            or f"SRW_A1_SENTINEL_PASS:{run_id}" not in output.splitlines()
        ):
            raise ScenarioError(
                422,
                "workspace_proof_missing",
                "Retained sentinel command did not return the required proof.",
            )
    return _worker_job_tool_call(step if step < 6 else step - 1, run_id)


def _worker_job_tool_call(step: int, run_id: str) -> ToolCallSpec:
    """Drive the real phased agent to completion without any off-pod tool.

    `search-job` and `fetch-job` are deliberately *live-gate* drivers: each
    requires a third-party provider (SearXNG, Crawl4AI) so it can exercise the
    off-pod boundary. The owned minimal profile has neither, and adding one
    breaks its determinism contract ("exactly one endpoint, exactly two
    models"). This scenario covers the case those two cannot: a real worker
    job reaching `job_complete` using only core, in-workspace tools.

    Same shape as its siblings — read the todo guide the staging contract
    requires, run the strategic todos, stage a tactical phase, read the
    verification guide at the completion boundary, then complete.
    """

    if step == 0:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/todo-guide/SKILL.md"}, separators=(",", ":")
            ),
        )
    if step < 5:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: hermetic strategic setup step."},
                separators=(",", ":"),
            ),
        )
    if step == 5:
        return ToolCallSpec(
            name="next_phase_todos",
            arguments=json.dumps(
                {
                    "todos": [
                        "Record the hermetic worker-job acceptance marker.",
                        "Verify the marker and close the phase.",
                    ],
                    "phase_name": "Hermetic worker-job gate",
                },
                separators=(",", ":"),
            ),
        )
    if step in {6, 7}:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: hermetic tactical step."},
                separators=(",", ":"),
            ),
        )
    if step == 8:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/verify-before-done/SKILL.md"}, separators=(",", ":")
            ),
        )
    if step == 9:
        return ToolCallSpec(
            name="job_complete",
            arguments=json.dumps(
                {
                    "summary": f"Completed the hermetic worker gate for E2E-{run_id}.",
                    "deliverables": [],
                    "confidence": 1.0,
                },
                separators=(",", ":"),
            ),
        )
    return ToolCallSpec(
        name="todo_complete",
        arguments=json.dumps(
            {"completion_note": "PASS: hermetic worker-job gate completed."},
            separators=(",", ":"),
        ),
    )


def _fetch_job_tool_call(step: int, run_id: str) -> ToolCallSpec:
    """Drive the real phased agent through both off-pod fetch operations."""

    if step == 0:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/todo-guide/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step < 5:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: live-gate strategic setup step."},
                separators=(",", ":"),
            ),
        )
    if step == 5:
        return ToolCallSpec(
            name="next_phase_todos",
            arguments=json.dumps(
                {
                    "todos": [
                        "Extract the stable public example page through Crawl4AI.",
                        "Crawl the same public origin through Crawl4AI.",
                        "Verify both fetch answers and close the research phase.",
                    ],
                    "phase_name": "Crawl4AI live fetch gate",
                },
                separators=(",", ":"),
            ),
        )
    if step == 6:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: tactical fetch phase staged."},
                separators=(",", ":"),
            ),
        )
    if step == 7:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/verify-before-done/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step == 8:
        return ToolCallSpec(
            name="extract_webpage",
            arguments=json.dumps(
                {"urls": "https://example.com/"},
                separators=(",", ":"),
            ),
        )
    if step == 9:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: Crawl4AI extract completed."},
                separators=(",", ":"),
            ),
        )
    if step == 10:
        return ToolCallSpec(
            name="crawl_website",
            arguments=json.dumps(
                {
                    "url": "https://example.com/",
                    "max_depth": 1,
                    "max_breadth": 2,
                    "limit": 2,
                },
                separators=(",", ":"),
            ),
        )
    if step in {11, 12}:
        return ToolCallSpec(
            name="todo_complete",
            arguments=json.dumps(
                {"completion_note": "PASS: Crawl4AI tactical fetch step."},
                separators=(",", ":"),
            ),
        )
    if step == 13:
        return ToolCallSpec(
            name="read_file",
            arguments=json.dumps(
                {"path": "skills/verify-before-done/SKILL.md"},
                separators=(",", ":"),
            ),
        )
    if step == 14:
        return ToolCallSpec(
            name="job_complete",
            arguments=json.dumps(
                {
                    "summary": (
                        f"Completed the Crawl4AI live fetch gate for E2E-{run_id}."
                    ),
                    "deliverables": [],
                    "confidence": 1.0,
                },
                separators=(",", ":"),
            ),
        )
    return ToolCallSpec(
        name="todo_complete",
        arguments=json.dumps(
            {"completion_note": "PASS: Crawl4AI live fetch gate completed."},
            separators=(",", ":"),
        ),
    )


def _embedding_inputs(value: Any) -> list[Any]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        if not value:
            raise ScenarioError(400, "invalid_request", "input must not be empty.")
        # A flat integer array represents one tokenized input; a list of strings or
        # arrays represents a batch.
        if all(isinstance(item, int) and not isinstance(item, bool) for item in value):
            return [value]
        if all(
            isinstance(item, str)
            or (
                isinstance(item, list)
                and all(
                    isinstance(token, int) and not isinstance(token, bool)
                    for token in item
                )
            )
            for item in value
        ):
            return value
    raise ScenarioError(
        400,
        "invalid_request",
        "input must be a string, token array, or batch of strings/token arrays.",
    )


def _stable_embedding(value: Any) -> list[float]:
    canonical = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    )
    seed = hashlib.sha256(canonical.encode("utf-8")).digest()
    return [
        round((seed[index % len(seed)] - 127.5) / 127.5, 8)
        for index in range(EMBEDDING_DIMENSIONS)
    ]


def _rerank_results(query: str, documents: list[str]) -> list[dict[str, Any]]:
    query_terms = set(re.findall(r"[a-z0-9]+", query.casefold()))
    scored: list[tuple[int, float]] = []
    for index, document in enumerate(documents):
        document_terms = set(re.findall(r"[a-z0-9]+", document.casefold()))
        overlap = len(query_terms & document_terms)
        denominator = max(1, len(query_terms | document_terms))
        score = overlap / denominator
        # Deterministic non-zero tie-breaker, kept well below one overlap unit.
        score += (len(documents) - index) / max(10_000, len(documents) * 10_000)
        scored.append((index, min(1.0, round(score, 8))))
    scored.sort(key=lambda item: (-item[1], item[0]))
    return [{"index": index, "relevance_score": score} for index, score in scored]


def _response_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex}"


def _valid_bearer(header: str | None, expected: str) -> bool:
    if not header or not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header.removeprefix("Bearer "), expected)


def _scenario_error_response(exc: ScenarioError) -> JSONResponse:
    return _error_response(exc.status_code, exc.error_type, exc.message)


def _error_response(status_code: int, error_type: str, message: str) -> JSONResponse:
    return JSONResponse(
        status_code=status_code,
        content={"error": {"type": error_type, "message": message}},
        headers={"Cache-Control": "no-store"},
    )
