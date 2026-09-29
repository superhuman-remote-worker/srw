"""Runtime owner of the attached session's identity.

One :class:`SessionIdentityRuntime` per runtime holds every identity value an
attachment adopts and every other owner reads:

- the bound thread;
- the durable session runtime generation and its attach token (the
  orchestrator's reservation for one session life), with the two additive
  contract flags that say whether the exact generation and the status
  identity were advertised;
- this process's input generation, minted at each attach's queue barrier so
  a successor process can reclaim what existed only in its predecessor's RAM;
- the local attach generation that scopes fire-and-forget session tasks.

Values this owner does *not* store are read through
:class:`SessionIdentityPorts` whenever a snapshot is taken: the registered
agent id, the Pod UID, the current stateless :class:`LeaseHandle` (mutable and
repointed in place, so it is never captured) and the orchestrator client whose
outbound headers mirror the adopted generation. The registration-issued
process incarnation (``dispatch_process_generation``), workspace generation
and incarnation, conversation revision and event epoch belong to other owners.

Every write is an explicit operation. :meth:`adopt` validates before it
mutates; :meth:`clear` with an expected generation and token clears only that
exact identity, never a successor's. :meth:`snapshot` is the one source of
:class:`SessionRuntimeIdentity`.

This module does not import the runtime that composes it, an application
factory, the loop or the worker graph (import contract and boundary guard).
"""

from __future__ import annotations

import inspect
import os
from dataclasses import dataclass
from typing import Any, Callable, Optional
from uuid import UUID, uuid4

from agent.api.lease_context import LeaseHandle
from agent.api.session_contract import ProtectedCloudUnavailable, WorkspaceNotReady
from shared.pinned_session_identity import pinned_session_ready_identity_fingerprint


def canonical_runtime_generation(value: Any) -> str | None:
    """The canonical UUID text of a delivered generation/token, else None."""

    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return str(UUID(value.strip()))
    except (TypeError, ValueError):
        return None


def pinned_status_identity_advertised(payload: Any) -> bool:
    """Accept only the exact numeric v1 additive lifecycle capability."""

    return bool(
        isinstance(payload, dict)
        and type(payload.get("pinned_status_identity_contract")) is int
        and payload["pinned_status_identity_contract"] == 1
    )


def pinned_runtime_generation_advertised(payload: Any) -> bool:
    """Accept only the exact numeric v1 runtime-generation capability."""

    return bool(
        isinstance(payload, dict)
        and type(payload.get("pinned_runtime_generation_contract")) is int
        and payload["pinned_runtime_generation_contract"] == 1
    )


@dataclass(frozen=True, slots=True)
class SessionRuntimeIdentity:
    """One synchronous read of the attached runtime's identity.

    ``process_generation`` is this process incarnation's input generation
    (minted per attach); ``session_generation`` is the durable session runtime
    generation; ``attach_generation`` is the local attach counter that scopes
    session side tasks. ``lease`` is the *current* stateless lease handle, or
    ``None`` on the pinned lane.
    """

    thread_id: Optional[str]
    process_generation: Optional[str]
    session_generation: Optional[str]
    attach_token: Optional[str]
    agent_id: Optional[str]
    pod_uid: Optional[str]
    lease: Optional[LeaseHandle]
    attach_generation: int


@dataclass(frozen=True, slots=True)
class SessionIdentityPorts:
    """Call-time dependencies of the identity owner.

    ``orchestrator_client`` returns the client whose outbound identity headers
    mirror the adopted generation (it may be ``None``). ``identity_replaced``
    runs whenever an adoption or a clear changes the identity, so latches
    scoped to one exact life (the retirement admission mirror) never survive
    into the next.
    """

    agent_id: Callable[[], Optional[str]]
    pod_uid: Callable[[], Optional[str]]
    lease: Callable[[], Optional[LeaseHandle]]
    stateless_mode: Callable[[], bool]
    orchestrator_client: Callable[[], Any]
    identity_replaced: Callable[[], None]


def environment_pod_uid() -> Optional[str]:
    """The Pod UID the kubelet injected, read at call time."""

    return os.environ.get("POD_UID")


class SessionIdentityRuntime:
    """Thread, generation, attach token, contracts and local generations of
    one runtime's attached session."""

    def __init__(
        self,
        ports: SessionIdentityPorts,
        *,
        thread_id: Optional[str] = None,
    ) -> None:
        self._ports = ports
        self._thread_id: Optional[str] = thread_id
        self._session_generation: Optional[str] = None
        self._attach_token: Optional[str] = None
        self._runtime_contract = False
        self._status_contract = False
        self._process_generation: Optional[str] = None
        self._attach_generation = 0

    # --- Read views ---------------------------------------------------------

    @property
    def thread_id(self) -> Optional[str]:
        return self._thread_id

    @property
    def session_generation(self) -> Optional[str]:
        return self._session_generation

    @property
    def attach_token(self) -> Optional[str]:
        return self._attach_token

    @property
    def runtime_contract(self) -> bool:
        """Whether the exact runtime-generation contract was advertised."""

        return self._runtime_contract

    @property
    def status_contract(self) -> bool:
        """Whether the pinned status-identity contract was advertised."""

        return self._status_contract

    @property
    def process_generation(self) -> Optional[str]:
        return self._process_generation

    @property
    def attach_generation(self) -> int:
        return self._attach_generation

    def snapshot(self) -> SessionRuntimeIdentity:
        """The attached runtime identity, read now in one synchronous step."""

        return SessionRuntimeIdentity(
            thread_id=self._thread_id,
            process_generation=self._process_generation,
            session_generation=self._session_generation,
            attach_token=self._attach_token,
            agent_id=self._ports.agent_id(),
            pod_uid=self._ports.pod_uid(),
            lease=self._ports.lease(),
            attach_generation=self._attach_generation,
        )

    def fingerprint(self) -> str | None:
        """The exact local identity used by readiness and effect gates."""

        return pinned_session_ready_identity_fingerprint(
            thread_id=self._thread_id,
            runtime_generation=self._session_generation,
            agent_id=self._ports.agent_id(),
            runtime_attach_token=self._attach_token,
            pod_uid=self._ports.pod_uid(),
        )

    def retirement_identity(
        self,
    ) -> tuple[str, Optional[str], Optional[str]] | None:
        """``(thread, generation, attach token)`` of the attached life."""

        if self._thread_id is None:
            return None
        return (str(self._thread_id), self._session_generation, self._attach_token)

    def stateless_lease_token(self) -> Optional[int]:
        """The exact live claim token for this attached stateless thread."""

        if not self._ports.stateless_mode() or self._thread_id is None:
            return None
        handle = self._ports.lease()
        if (
            handle is None
            or not handle.active
            or handle.lost.is_set()
            or str(handle.unit_id) != str(self._thread_id)
        ):
            return None
        return int(handle.lease_token)

    def parent_authority(self):
        """Snapshot the exact current pinned life or stateless turn lease."""

        from shared.session_subagent_authority import (
            SessionParentAuthority,
            SessionParentAuthorityRefused,
        )

        thread_id = str(self._thread_id or "").strip()
        if not thread_id:
            raise SessionParentAuthorityRefused("parent_missing")
        lease = self._ports.lease()
        if lease is not None:
            if (
                not lease.active
                or lease.lost.is_set()
                or str(lease.unit_id or "") != thread_id
                or type(lease.lease_token) is not int
                or lease.lease_token <= 0
                or not isinstance(lease.executor_id, str)
                or not lease.executor_id
                or not isinstance(lease.pod_uid, str)
                or not lease.pod_uid
            ):
                raise SessionParentAuthorityRefused("stateless_parent_not_current")
            return SessionParentAuthority(
                execution_lane="stateless",
                parent_thread_id=thread_id,
                lease_token=lease.lease_token,
                executor_id=lease.executor_id,
                executor_pod_uid=lease.pod_uid,
            )

        agent_id = self._ports.agent_id()
        pod_uid = str(self._ports.pod_uid() or "").strip()
        generation = str(self._session_generation or "").strip()
        attach_token = str(self._attach_token or "").strip()
        if not agent_id or not pod_uid or not generation or not attach_token:
            raise SessionParentAuthorityRefused("pinned_parent_not_current")
        return SessionParentAuthority(
            execution_lane="pinned",
            parent_thread_id=thread_id,
            agent_id=agent_id,
            pod_uid=pod_uid,
            session_runtime_generation=generation,
            runtime_attach_token=attach_token,
        )

    # --- Thread binding -----------------------------------------------------

    def bind_thread(self, thread_id: Optional[str]) -> None:
        """Bind the thread this process serves (boot, attach, rollback)."""

        self._thread_id = thread_id

    def release_thread(self, *, expected: Optional[str] = None) -> bool:
        """Unbind the thread; with ``expected``, only that exact thread."""

        if expected is not None and self._thread_id != expected:
            return False
        self._thread_id = None
        return True

    # --- Session runtime generation ----------------------------------------

    def adopt(
        self,
        generation: Any,
        attach_token: Any = None,
        *,
        contract_advertised: bool,
    ) -> None:
        """Install one exact runtime identity before any attach-side await."""

        canonical_generation = canonical_runtime_generation(generation)
        canonical_token = (
            canonical_runtime_generation(attach_token)
            if attach_token is not None
            else None
        )
        if contract_advertised and (
            canonical_generation is None
            or (not self._ports.stateless_mode() and canonical_token is None)
        ):
            raise WorkspaceNotReady(
                "Pinned runtime generation contract omitted its exact generation "
                "or attach token"
            )
        if generation is not None and canonical_generation is None:
            raise WorkspaceNotReady("Pinned runtime generation is malformed")
        if attach_token is not None and canonical_token is None:
            raise WorkspaceNotReady("Pinned runtime attach token is malformed")
        self._session_generation = canonical_generation
        self._attach_token = canonical_token
        self._runtime_contract = contract_advertised
        self._ports.identity_replaced()
        client = self._ports.orchestrator_client()
        if client is not None and canonical_generation is not None:
            adopt = getattr(client, "adopt_session_runtime_identity", None)
            if not callable(adopt) or not adopt(
                canonical_generation,
                canonical_token,
                contract_advertised=contract_advertised,
            ):
                raise WorkspaceNotReady("Pinned runtime identity could not be adopted")

    def clear(
        self,
        *,
        expected_generation: str | None = None,
        expected_attach_token: str | None = None,
    ) -> bool:
        """Clear only a captured generation, never a successor's authority."""

        if (
            expected_generation is not None
            and self._session_generation != expected_generation
        ):
            return False
        if (
            expected_attach_token is not None
            and self._attach_token != expected_attach_token
        ):
            return False
        client = self._ports.orchestrator_client()
        if client is not None:
            clear = getattr(client, "clear_session_runtime_identity", None)
            if callable(clear) and not inspect.iscoroutinefunction(clear):
                clear(
                    expected_generation=expected_generation,
                    expected_attach_token=expected_attach_token,
                )
        self._session_generation = None
        self._attach_token = None
        self._runtime_contract = False
        self._ports.identity_replaced()
        return True

    def adopt_workspace_payload(
        self,
        payload: Any,
        *,
        protected_required: bool,
    ) -> str | None:
        """Fence a workspace response to this attach's exact runtime life."""

        if not isinstance(payload, dict):
            raise WorkspaceNotReady("Workspace runtime identity is unavailable")
        advertised = pinned_runtime_generation_advertised(payload)
        raw_generation = payload.get("session_runtime_generation")
        generation = canonical_runtime_generation(raw_generation)
        if raw_generation is not None and generation is None:
            raise WorkspaceNotReady("Workspace runtime generation is malformed")
        if advertised and generation is None:
            raise WorkspaceNotReady(
                "Workspace runtime generation contract omitted its generation"
            )
        if self._runtime_contract and not advertised:
            raise WorkspaceNotReady("Workspace runtime generation contract disappeared")
        if (
            self._session_generation is not None
            and generation != self._session_generation
        ):
            raise WorkspaceNotReady(
                "Workspace runtime generation changed during attach"
            )
        if self._session_generation is None and generation is not None:
            self.adopt(
                generation,
                self._attach_token,
                contract_advertised=advertised,
            )
        if protected_required and (
            not advertised or generation is None or self._session_generation is None
        ):
            raise ProtectedCloudUnavailable(
                "Protected workspace has no exact runtime generation"
            )
        return generation

    def set_status_contract(self, advertised: bool) -> None:
        """Record whether the pinned status-identity contract was advertised."""

        self._status_contract = bool(advertised)

    # --- Local generations --------------------------------------------------

    def begin_attach(self) -> int:
        """Start a new local attach generation and return it."""

        self._attach_generation += 1
        return self._attach_generation

    def mint_process_generation(self) -> str:
        """Mint this attach's input generation at the queue barrier."""

        self._process_generation = str(uuid4())
        return self._process_generation

    def clear_process_generation(self) -> None:
        self._process_generation = None
