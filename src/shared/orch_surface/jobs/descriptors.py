"""Framework-independent descriptors for the shared job-management surface."""

from __future__ import annotations

from dataclasses import dataclass
import base64
import functools
import inspect
from typing import Any, Awaitable, Callable, Literal

from shared.orch_surface.client import AsyncCockpitClient
from shared.permission_modes import TOOL_EFFECTS
from shared.runtime_actor import RuntimeActorContext

JobGroup = Literal["job_control", "job_inspection"]
#: officer_supervision_surface §3 capability taxonomy. ``job_inspection`` stays
#: the Cockpit umbrella/display group; caller DEFAULTS are evaluated at this
#: finer plane boundary: bounded orchestrator actions (job_control),
#: control-plane truth (job_observability), declared bounded disposition
#: material (job_evidence), and the object plane (job_workspace) which the
#: background officer never receives by default.
JobPlane = Literal["job_control", "job_observability", "job_evidence", "job_workspace"]
CallerKind = Literal["mcp", "session", "officer"]
GrantKind = Literal["explicit"]


@dataclass(frozen=True, slots=True)
class ToolImageAttachment:
    """One bounded image carried out-of-band from ordinary tool text."""

    base64_data: str
    media_type: str

    def decoded(self) -> bytes:
        return base64.b64decode(self.base64_data, validate=True)


@dataclass(frozen=True, slots=True)
class JobToolResult:
    """Shared descriptor output with at most one optional image attachment."""

    text: str
    image: ToolImageAttachment | None = None


JobHandler = Callable[..., Awaitable[str | JobToolResult]]


@dataclass(frozen=True, slots=True)
class CallerCtx:
    """Trusted caller identity and lineage hidden from public tool schemas.

    ``project_ids`` contains bindings established by the session/token adapter;
    it is never populated from model arguments.  Only an exactly-one binding
    becomes an ``X-MCP-Scope`` header, and only on the MCP and officer lanes
    (officer_supervision_surface E2: the officer is always project-fenced; the
    plain session lane never stamps scope).
    ``lineage_project_id`` is kept separately because an existing
    multi-project session can still have a primary project used by the
    job-create funnel without claiming that its reads are single-project
    scoped.

    ``auth_failed=True`` marks a caller whose adapter tried and FAILED to
    resolve its auth context (http-mode MCP without a resolvable token).  The
    invocation is then bound with no identity headers at all — the internal
    key is never attached as an error fallback — so guarded orchestrator
    endpoints fail closed with 401, and the tool result names the auth
    context failure.  Deliberately anonymous lanes (stdio MCP, worker-mode
    agent tools) keep ``auth_failed=False`` with ``user_id=None``.
    """

    kind: CallerKind
    user_id: str | None = None
    project_ids: tuple[str, ...] = ()
    lineage_project_id: str | None = None
    thread_id: str | None = None
    parent_job_id: str | None = None
    explicit_scope: str | None = None
    resolve_job_id_prefixes: bool = False
    auth_failed: bool = False
    runtime_actor: RuntimeActorContext | None = None
    supports_multimodal: bool | None = None

    @property
    def project_scope(self) -> str | None:
        projects = tuple(dict.fromkeys(p for p in self.project_ids if p))
        if len(projects) == 1:
            return f"project:{projects[0]}"
        return None

    @property
    def scope_header(self) -> str | None:
        return self.project_scope or self.explicit_scope


@dataclass(frozen=True, slots=True)
class JobDescriptor:
    """One public job operation and the policy metadata shared by adapters."""

    name: str
    handler: JobHandler
    group: JobGroup
    plane: JobPlane
    caller_defaults: frozenset[CallerKind]
    phases: tuple[str, ...]
    grant: GrantKind | None
    gate: str | None
    effect: str
    short_description: str
    description: str
    public_signature: inspect.Signature

    def registry_metadata(self) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "module": "orchestrator.jobs",
            "function": self.name,
            "description": self.description,
            "short_description": self.short_description,
            "category": self.group,
            "effect": self.effect,
            "group": self.group,
            "plane": self.plane,
            "caller_defaults": sorted(self.caller_defaults),
            "phases": list(self.phases),
        }
        if self.grant:
            metadata["grant"] = self.grant
        if self.gate:
            metadata["gate"] = self.gate
        return metadata


_DESCRIPTORS: dict[str, JobDescriptor] = {}


def descriptor(
    *,
    group: JobGroup,
    plane: JobPlane,
    caller_defaults: frozenset[CallerKind],
    effect: str,
    grant: GrantKind | None = None,
    gate: str | None = None,
    phases: tuple[str, ...] = ("strategic", "tactical"),
) -> Callable[[JobHandler], JobHandler]:
    """Declare one job operation from its signature, docstring, and metadata."""

    def decorate(handler: JobHandler) -> JobHandler:
        name = handler.__name__
        if name in _DESCRIPTORS:
            raise RuntimeError(f"Duplicate job descriptor: {name}")
        signature = inspect.signature(handler)
        parameters = list(signature.parameters.values())
        if [parameter.name for parameter in parameters[:2]] != ["client", "caller"]:
            raise TypeError(
                f"{name} must begin with (client, caller); got "
                f"{[parameter.name for parameter in parameters[:2]]}"
            )
        description = inspect.getdoc(handler) or ""
        if not description:
            raise TypeError(f"{name} must have a public docstring")
        short_description = description.splitlines()[0].strip()
        if grant and not gate:
            raise TypeError(f"{name}: grant metadata requires a gate explanation")
        if effect not in TOOL_EFFECTS:
            raise TypeError(f"{name}: effect must be one of {TOOL_EFFECTS}")
        public_signature = signature.replace(parameters=parameters[2:])
        _DESCRIPTORS[name] = JobDescriptor(
            name=name,
            handler=handler,
            group=group,
            plane=plane,
            caller_defaults=caller_defaults,
            phases=phases,
            grant=grant,
            gate=gate,
            effect=effect,
            short_description=short_description,
            description=description,
            public_signature=public_signature,
        )
        return handler

    return decorate


def get_descriptors() -> tuple[JobDescriptor, ...]:
    """Return the deterministic canonical inventory."""
    return tuple(_DESCRIPTORS[name] for name in sorted(_DESCRIPTORS))


def get_descriptor(name: str) -> JobDescriptor:
    return _DESCRIPTORS[name]


def registry_metadata() -> dict[str, dict[str, Any]]:
    """Return agent-registry metadata derived only from descriptors."""
    return {item.name: item.registry_metadata() for item in get_descriptors()}


def caller_default_names(caller: CallerKind, group: JobGroup) -> frozenset[str]:
    """Return the current effective default subset for a caller and group."""
    return frozenset(
        item.name
        for item in get_descriptors()
        if item.group == group and caller in item.caller_defaults
    )


#: Prefixed onto the tool result when an adapter could not resolve its auth
#: context.  The invocation is still sent — with NO identity headers at all —
#: so authorization stays server-side and guarded endpoints 401 (fail closed).
AUTH_CONTEXT_FAILURE_NOTICE = (
    "Auth context failure: the caller identity for this invocation could not "
    "be resolved, so it was sent without any identity headers (no internal "
    "key, no user) and protected endpoints refuse it. Re-authenticate and "
    "retry."
)


def make_bound_handler(
    item: JobDescriptor,
    *,
    client_provider: Callable[[], AsyncCockpitClient],
    caller_provider: Callable[[], CallerCtx],
    result_adapter: Callable[[str | JobToolResult], Any] | None = None,
) -> JobHandler:
    """Bind hidden dependencies while retaining the reviewed public signature."""

    @functools.wraps(item.handler)
    async def invoke(*args: Any, **kwargs: Any) -> str:
        client = client_provider()
        caller = caller_provider()
        # Scope stamping is lane-deliberate (officer_supervision_surface E2):
        # * MCP stamps its token scope (pre-unification contract).
        # * The plain SESSION lane deliberately does NOT — stamping it
        #   activated server-side project fencing that made NULL-project jobs
        #   invisible to single-project sessions.
        # * The OFFICER lane always stamps X-MCP-Scope: project:<uuid> so the
        #   server fences every read/write to the commissioned project. A
        #   missing or multiple project binding is an attach error, not an
        #   unscoped fleet view — fail closed before any request is sent.
        if caller.kind == "officer" and caller.project_scope is None:
            bound = len(tuple(p for p in caller.project_ids if p))
            return (
                "Officer project binding error: this background-officer "
                f"session is bound to {bound} project(s); exactly one is "
                "required for scoped supervision reads. Re-attach the officer "
                "post to a single project."
            )
        if caller.kind == "officer":
            refreshed, reason = await client.ensure_runtime_actor(caller.runtime_actor)
            if not refreshed:
                return f"Officer runtime authorization failed: {reason}"
        scope = caller.scope_header if caller.kind in ("mcp", "officer") else None
        with client.invocation_scope(
            user_id=caller.user_id,
            scope=scope,
            unauthenticated=caller.auth_failed,
            runtime_actor=caller.runtime_actor,
        ):
            if not caller.auth_failed:
                outcome = await item.handler(client, caller, *args, **kwargs)
                return result_adapter(outcome) if result_adapter else outcome
            # Fail closed: the request above carries no identity headers, so
            # the orchestrator 401s; surface that as an auth-context failure
            # rather than an anonymous-looking backend error.
            try:
                outcome = await item.handler(client, caller, *args, **kwargs)
            except Exception as error:
                outcome = f"{type(error).__name__}: {error}"
            if isinstance(outcome, JobToolResult):
                # Authentication failure must never carry an attachment even
                # if a doubled client returned one unexpectedly. This also
                # keeps base64 out of the fallback string representation.
                combined: str | JobToolResult = JobToolResult(
                    text=f"{AUTH_CONTEXT_FAILURE_NOTICE}\n{outcome.text}"
                )
            else:
                combined = f"{AUTH_CONTEXT_FAILURE_NOTICE}\n{outcome}"
            return result_adapter(combined) if result_adapter else combined

    invoke.__name__ = item.name
    invoke.__qualname__ = item.name
    invoke.__signature__ = item.public_signature  # type: ignore[attr-defined]
    return invoke


__all__ = [
    "AUTH_CONTEXT_FAILURE_NOTICE",
    "CallerCtx",
    "CallerKind",
    "JobDescriptor",
    "JobToolResult",
    "JobGroup",
    "JobPlane",
    "ToolImageAttachment",
    "caller_default_names",
    "descriptor",
    "get_descriptor",
    "get_descriptors",
    "make_bound_handler",
    "registry_metadata",
]
