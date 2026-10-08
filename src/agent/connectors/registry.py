"""The materializers, run in the order each entry point needs.

The orders are the ones the agent kept before materializers existed (lane 1
§3.4). They are data here, and ``tests/test_agent_connectors_registry.py``
records the calls through every entry point:

* **workspace init** (worker and session): the SSH identities load while the
  workspace initializes, before anything clones;
* **worker setup** (``UniversalAgent._setup_job_tools``): environment, lease
  tokens, managed connections, MCP, checkouts, credential files;
* **session attach**, in two phases (``agent.api.session_attach``): the
  harness phase (managed connections, MCP) runs before the workspace exists,
  so the tool set can be resolved; the workspace phase (environment, lease
  tokens, checkouts, credential files) after it is initialized;
* **live update** (``PersistentSession.resetup_datasources``): environment,
  lease tokens, SSH identities, the KB deferral, a fresh harness that the
  caller swaps in place, then checkouts and credential files;
* **backend swap** (``PersistentSession.swap_backend``): the environment, the
  lease tokens and the credential files follow the physical workspace.

Credential files reach the workspace, never the agent pod (slice D1d). They
come after the checkouts in every order, as they always did on a worker.
Lease tokens (slice C2) are written before any checkout, so a checkout whose
driver swaps credentials can read its lease.

Blocking steps that the async entry points ran off the event loop still do
(``offload``); the rest run inline, as they did.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from agent.connectors.base import (
    FACTS_SECTIONS,
    Delivery,
    FactsLines,
    Materializer,
    RuntimeContext,
    SupportsBackendSwap,
    SupportsKnowledgeBindings,
    SupportsReady,
    SupportsRelease,
    SupportsReplace,
    declared_read_only_note,
)
from agent.connectors.checkout import CheckoutMaterializer
from agent.connectors.connections import ManagedConnectionMaterializer
from agent.connectors.env import EnvFileMaterializer
from agent.connectors.files import CredentialFileMaterializer
from agent.connectors.knowledge import KnowledgeIndexMaterializer
from agent.connectors.lease import LeaseTokenMaterializer
from agent.connectors.mcp import McpClientMaterializer
from agent.connectors.ssh_identity import SshIdentityMaterializer

logger = logging.getLogger(__name__)

WORKSPACE_INIT_ORDER: tuple[str, ...] = ("ssh_identity",)
WORKER_ORDER: tuple[str, ...] = (
    "env_file",
    "lease_token",
    "managed_connection",
    "mcp_client",
    "checkout",
    "credential_file",
)
SESSION_HARNESS_ORDER: tuple[str, ...] = ("managed_connection", "mcp_client")
SESSION_WORKSPACE_ORDER: tuple[str, ...] = (
    "env_file",
    "lease_token",
    "checkout",
    "credential_file",
)
#: The harness forms a live update rebuilds whole and hands to the caller.
LIVE_HARNESS_FORMS: tuple[str, ...] = ("managed_connection", "mcp_client")
LIVE_ORDER: tuple[str, ...] = (
    "env_file",
    "lease_token",
    "ssh_identity",
    "knowledge_index",
    *LIVE_HARNESS_FORMS,
    "checkout",
    "credential_file",
)
BACKEND_SWAP_ORDER: tuple[str, ...] = ("env_file", "lease_token", "credential_file")
#: What the entry points release when an execution ends. What lives in the
#: workspace (environment, lease tokens, credential files) lives as long as
#: it does.
RELEASE_ORDER: tuple[str, ...] = ("managed_connection",)

#: Steps an async entry point runs in a worker thread.
_SESSION_OFFLOAD = frozenset(
    {"env_file", "lease_token", "ssh_identity", "credential_file"}
)


def routed(deliveries: Iterable[Delivery], form: str) -> list[Delivery]:
    """The deliveries whose driver lists ``form``, in payload order."""
    return [delivery for delivery in deliveries if delivery.routes_to(form)]


class ConnectorRegistry:
    """The installed materializers, by form."""

    def __init__(self, materializers: Iterable[Materializer]) -> None:
        self.by_form: dict[str, Materializer] = {}
        for materializer in materializers:
            if materializer.form in self.by_form:
                raise ValueError(f"two materializers for {materializer.form!r}")
            self.by_form[materializer.form] = materializer

    @classmethod
    def default(cls) -> ConnectorRegistry:
        return cls(
            (
                EnvFileMaterializer(),
                LeaseTokenMaterializer(),
                CredentialFileMaterializer(),
                CheckoutMaterializer(),
                SshIdentityMaterializer(),
                ManagedConnectionMaterializer(),
                McpClientMaterializer(),
                KnowledgeIndexMaterializer(),
            )
        )

    def _materializer(self, form: str) -> Materializer:
        return self.by_form[form]

    @staticmethod
    def _warn_unserved(deliveries: Sequence[Delivery]) -> None:
        for delivery in deliveries:
            if delivery.spec is None and delivery.entry.get("type"):
                logger.warning(
                    "No connector driver serves datasource %r of type %r; "
                    "nothing is delivered for it",
                    delivery.name,
                    delivery.entry.get("type"),
                )

    async def _run(
        self,
        order: Sequence[str],
        deliveries: Sequence[Delivery],
        rt: RuntimeContext,
        *,
        offload: frozenset[str] = frozenset(),
    ) -> None:
        for form in order:
            materializer = self._materializer(form)
            items = routed(deliveries, form)
            if form in offload:
                await asyncio.to_thread(materializer.materialize, items, rt)
            else:
                materializer.materialize(items, rt)
            if isinstance(materializer, SupportsReady):
                await materializer.ready(rt)

    # -- entry points -------------------------------------------------------

    async def initialize_workspace(
        self, rt: RuntimeContext, *, offload: bool = False
    ) -> None:
        """Load the SSH identities into the workspace being initialized."""
        await self._run(
            WORKSPACE_INIT_ORDER,
            (),
            rt,
            offload=_SESSION_OFFLOAD if offload else frozenset(),
        )

    async def setup_worker(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> None:
        self._warn_unserved(deliveries)
        await self._run(WORKER_ORDER, deliveries, rt)

    async def attach_harness(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> None:
        self._warn_unserved(deliveries)
        await self._run(SESSION_HARNESS_ORDER, deliveries, rt)

    async def attach_workspace(
        self, deliveries: Sequence[Delivery], rt: RuntimeContext
    ) -> None:
        await self._run(
            SESSION_WORKSPACE_ORDER, deliveries, rt, offload=_SESSION_OFFLOAD
        )

    async def replace_live(
        self,
        old: Sequence[Delivery],
        new: Sequence[Delivery],
        rt: RuntimeContext,
        *,
        on_harness_replaced: Callable[[dict[str, Any], dict[str, Any]], None],
    ) -> None:
        """Apply a live attach/detach.

        The harness forms are rebuilt into fresh slot dicts from the whole
        new payload, which ``on_harness_replaced(connections, clients)``
        swaps in place: the replaced connections are the caller's to close
        once no tool call can still be using them.
        """
        self._warn_unserved(new)
        fresh = RuntimeContext(
            execution=rt.execution,
            workspace_manager=rt.workspace_manager,
        )
        for form in LIVE_ORDER:
            materializer = self._materializer(form)
            if form in LIVE_HARNESS_FORMS:
                materializer.materialize(routed(new, form), fresh)
                if isinstance(materializer, SupportsReady):
                    await materializer.ready(fresh)
                if form == LIVE_HARNESS_FORMS[-1]:
                    on_harness_replaced(fresh.connections, fresh.clients)
                continue
            if not isinstance(materializer, SupportsReplace):
                continue
            step = (materializer.replace, routed(old, form), routed(new, form), rt)
            if form in _SESSION_OFFLOAD:
                await asyncio.to_thread(*step)
            else:
                step[0](*step[1:])

    def on_backend_swap(self, deliveries: Sequence[Delivery], backend: Any) -> None:
        """Deliver what lives on the physical workspace onto ``backend``."""
        for form in BACKEND_SWAP_ORDER:
            materializer = self._materializer(form)
            if isinstance(materializer, SupportsBackendSwap):
                materializer.on_backend_swap(routed(deliveries, form), backend)

    def release(self, rt: RuntimeContext) -> None:
        """Close the harness slots and remove what the execution wrote."""
        for form in RELEASE_ORDER:
            materializer = self._materializer(form)
            if isinstance(materializer, SupportsRelease):
                materializer.release(rt)

    def knowledge_bindings(
        self,
        deliveries: Sequence[Delivery],
        *,
        project_ids: Iterable[str],
        runtime_actor: Any,
    ) -> list[Any]:
        knowledge = self._materializer("knowledge_index")
        if not isinstance(knowledge, SupportsKnowledgeBindings):
            raise TypeError("the knowledge_index materializer builds no bindings")
        return knowledge.bindings(
            routed(deliveries, "knowledge_index"),
            project_ids=project_ids,
            runtime_actor=runtime_actor,
        )

    def facts(self, deliveries: Sequence[Delivery], rt: RuntimeContext) -> list[str]:
        """The README's connector lines, grouped by section.

        Each connector is described by the materializer of its driver's
        first form; within a section, connectors keep payload order.
        """
        collected: list[FactsLines] = []
        for form, materializer in self.by_form.items():
            described = [d for d in deliveries if d.primary_form == form]
            if described:
                collected.extend(materializer.facts(described, rt))
        for delivery in deliveries:
            if delivery.spec is None:
                ds = delivery.entry
                collected.append(
                    FactsLines(
                        "Other",
                        delivery.index,
                        [
                            f"- **{ds.get('name', 'Unnamed')}** "
                            f"({ds.get('type', 'unknown')}){declared_read_only_note(ds)}"
                        ],
                    )
                )
        lines: list[str] = []
        for section in FACTS_SECTIONS:
            items = sorted(
                (item for item in collected if item.section == section),
                key=lambda item: item.index,
            )
            if not items:
                continue
            lines.append(f"### {section}")
            for item in items:
                lines.extend(item.lines)
            lines.append("")
        if not lines:
            # A live remove-all still needs the section rewritten — an agent
            # re-reading the file must not act on connection names that no
            # longer resolve.
            lines += ["_No connectors attached._", ""]
        return lines


_REGISTRY: ConnectorRegistry | None = None


def connector_registry() -> ConnectorRegistry:
    """The process's registry (tests replace ``_REGISTRY``)."""
    global _REGISTRY
    if _REGISTRY is None:
        _REGISTRY = ConnectorRegistry.default()
    return _REGISTRY
