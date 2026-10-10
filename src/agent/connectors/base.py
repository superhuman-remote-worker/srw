"""The runtime half of a connector driver: deliveries, materializers, context.

A :class:`Delivery` is one payload entry with the binding descriptor built
from it (:func:`agent.connectors.legacy.binding_from_legacy_entry`). A
materializer owns one delivery form and applies the deliveries routed to it;
the registry (:mod:`agent.connectors.registry`) runs the materializers in the
order each entry point needs. Materializers hold no state of their own: what
they deliver lives on the :class:`RuntimeContext` the caller owns, so the
worker and the session keep their existing attributes as the store.

Routing: a delivery reaches every materializer whose form its driver spec
lists, and each materializer reads what it needs from the descriptor's
entries of its form. README lines come from the materializer of the spec's
first form only, so each connector is described once.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The driver
contract" (Runtime) and lane 1 §3.4.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal, NamedTuple, Protocol, runtime_checkable

from shared.connectors.binding import BindingDescriptor
from shared.connectors.contract import DriverSpec

#: The home the orchestrator resolves credential-file target paths against
#: (``orchestrator.security.credential_files``). The credential-file
#: materializer maps them to the same paths under the workspace home.
AGENT_HOME = "/home/srw"

Execution = Literal["worker", "session"]

#: README sections, in the order the workspace facts list them.
FACTS_SECTIONS: tuple[str, ...] = (
    "Repositories",
    "OKF Knowledge Bases",
    "Databases",
    "MCP Servers",
    "Credential Files",
    "Other",
)


@dataclass(frozen=True, eq=False)
class Delivery:
    """One payload entry and the binding descriptor built from it.

    ``entry`` is the wire entry itself, not a copy: MCP discovery annotates
    it in place and the README reads those annotations back. ``binding`` and
    ``spec`` are ``None`` for a type no installed driver serves.
    """

    index: int
    entry: dict[str, Any]
    binding: BindingDescriptor | None
    spec: DriverSpec | None

    @property
    def name(self) -> str:
        return str(self.entry.get("name", "unnamed"))

    @property
    def primary_form(self) -> str | None:
        """The form whose materializer describes this connector."""
        return self.spec.delivery_forms[0] if self.spec else None

    def routes_to(self, form: str) -> bool:
        return self.spec is not None and form in self.spec.delivery_forms

    def values(self, form: str) -> list[Mapping[str, Any]]:
        """The descriptor's entry values of ``form``, in order."""
        if self.binding is None:
            return []
        return [entry.value for entry in self.binding.entries if entry.form == form]


@dataclass
class RuntimeContext:
    """What one execution's materializers read and write.

    ``connections`` is the harness slot registry (``ToolContext.datasources``)
    and is shared BY REFERENCE: materializers mutate it in place, never
    rebind it. ``backend`` overrides the workspace manager's backend while a
    workspace is still being initialized (the SSH identities load before its
    manager exists). ``ssh_identities`` is the hidden
    ``workspace_ssh_identities`` list; the identity materializer pops every
    private key from it.
    """

    execution: Execution
    workspace_manager: Any = None
    connections: dict[str, Any] = field(default_factory=dict)
    clients: dict[str, Any] = field(default_factory=dict)
    backend: Any = None
    ssh_identities: list[Any] | None = None
    ssh_identity_status: dict[str, str] | None = None
    #: How far a clone may retire pre-agent key files (``sweep``, ``own``,
    #: ``keep``; see ``agent.connectors.checkout``).
    legacy_key_files: str = "sweep"
    #: Forms whose live change waits for the next attach.
    deferred: set[str] = field(default_factory=set)
    #: A live change's ``threading.Event``: set when the change is no longer
    #: wanted (its task cancelled, the session ending), so a step still
    #: running in a worker thread stops at its next step boundary.
    cancel: Any = None

    @property
    def workspace_backend(self) -> Any:
        if self.backend is not None:
            return self.backend
        return getattr(self.workspace_manager, "backend", None)


class FactsLines(NamedTuple):
    """README lines for one delivery, filed under a section."""

    section: str
    index: int
    lines: list[str]


class Materializer(Protocol):
    """Applies the deliveries of one form.

    ``materialize`` receives every delivery routed to the form, in payload
    order, and may raise only where the entry point must fail (an
    environment connector on a workspace without a shell). ``facts`` returns
    README lines for the deliveries this form describes.
    """

    form: str

    def materialize(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> None:
        """Deliver ``deliveries`` into ``rt``."""
        ...

    def facts(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> list[FactsLines]:
        """README lines, one :class:`FactsLines` per described delivery."""
        ...


@runtime_checkable
class SupportsReady(Protocol):
    """A form whose delivery finishes asynchronously (MCP discovery)."""

    async def ready(self, rt: RuntimeContext) -> None: ...


@runtime_checkable
class SupportsReplace(Protocol):
    """A form a live attach or detach changes in place.

    ``old`` and ``new`` are the deliveries routed to the form before and
    after the change.
    """

    def replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None: ...


@runtime_checkable
class SupportsStagedReplace(Protocol):
    """A form whose live change is slow and read by a turn in flight (the
    checkouts), in three steps: ``begin_replace`` on the event loop (fast:
    what a removal takes away goes at once), ``stage_replace`` in a worker
    thread (slow: changes nothing a reader sees), and the callable it
    returns, which swaps the result in on the event loop in one step."""

    def begin_replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
    ) -> None: ...

    def stage_replace(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
        cancel: Any = None,
    ) -> Callable[[], None]:
        """``cancel`` (a ``threading.Event``) is set when the change is no
        longer wanted: the slow step stops at its next step boundary."""
        ...


@runtime_checkable
class SupportsBackendSwap(Protocol):
    """A form that lives on the physical workspace and must follow it."""

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        """Deliver again onto ``backend`` before the old one retires."""
        ...


@runtime_checkable
class SupportsRelease(Protocol):
    """A form that leaves something behind when the execution ends."""

    def release(self, rt: RuntimeContext) -> None:
        """Best effort; never raises."""
        ...


@runtime_checkable
class SupportsKnowledgeBindings(Protocol):
    """The knowledge form: bindings built with the tool context."""

    def bindings(
        self,
        deliveries: Sequence[Delivery],
        *,
        project_ids: Iterable[str],
        runtime_actor: Any,
    ) -> list[Any]: ...


class ConnectionFactory(Protocol):
    """Opens one managed connection (a closed set, owned by SRW's tools).

    ``kind`` is the harness slot the connection takes, which the built-in
    tools read; ``section`` is the README section the connector is listed
    under.
    """

    kind: str
    section: str

    def connect(self, value: Mapping[str, Any]) -> tuple[Any, Any | None]:
        """``(connection, parent client to close, or None)``; raises on failure."""
        ...

    def facts(self, delivery: Delivery, value: Mapping[str, Any]) -> list[str]:
        """The connector's README lines."""
        ...


def declared_read_only_note(entry: Mapping[str, Any]) -> str:
    """Advisory suffix for an entry that carries the creator's ``read_only``.

    The orchestrator folds the creator's read-only tag into the entry's
    ``project_read_only``, which switches the tool surface; its payload does
    not forward ``read_only`` itself, so only an in-process caller that hands
    over a resolved row reaches this. Credentials stay what decides what is
    really allowed; this just tells the agent the intent.
    """
    return " (declared read-only — treat as no-write)" if entry.get("read_only") else ""


def read_only_note(entry: Mapping[str, Any]) -> str:
    """The README note of a read-only connector that has no tools to drop.

    Environment, file, key and kubeconfig connectors deliver the same
    credentials at either level, so their read-only (the project link or the
    creator's tag, which the orchestrator folds into ``project_read_only``)
    reaches the agent only as this note. An in-process resolved row's own
    ``read_only`` counts too. The credentials still decide what is allowed.
    """
    read_only = entry.get("project_read_only") or entry.get("read_only")
    return " (read-only — treat as no-write)" if read_only else ""
