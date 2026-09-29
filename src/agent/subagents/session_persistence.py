"""Durable ledger for subagents whose parent is a persistent session.

The session parent is named by ``parent_thread_id`` only.  Every operation
resolves a fresh exact parent authority: pinned runtimes normally return the
same immutable identity for their life, while stateless sessions supply the
current turn's lease through an authority provider.  A stateless lease is
never cached by this ledger.
"""

from __future__ import annotations

import inspect
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Mapping, Optional, Sequence
from uuid import UUID

from agent.core.thread_messages import _serialize_message_row
from shared.session_subagent_authority import (
    SessionParentAuthority,
    SessionParentAuthorityRefused,
    coerce_session_parent_authority,
    session_subagent_delivery_id,
)
from agent.subagents.ledger import is_terminal_status
from agent.subagents.persistence import (
    RestoredSubagentTranscript,
    SubagentPersistenceRefused,
    _serialize_seed_message,
    restore_subagent_messages,
)

logger = logging.getLogger(__name__)

AuthorityValue = SessionParentAuthority | Mapping[str, Any]
AuthorityProvider = Callable[[], AuthorityValue | Awaitable[AuthorityValue]]


class SessionSubagentLedger:
    """Thread-parent counterpart of :class:`DbSubagentLedger`.

    ``authority_provider`` is mandatory for a stateless parent because its
    exact queue lease changes between turns.  ``parent_authority`` is a pinned
    convenience for tests and a single attached session life.
    """

    def __init__(
        self,
        client: Any,
        postgres: Any,
        *,
        parent_thread_id: str | UUID,
        parent_context: Any = None,
        authority_provider: AuthorityProvider | None = None,
        parent_authority: AuthorityValue | None = None,
    ) -> None:
        if client is None or postgres is None:
            raise ValueError(
                "SessionSubagentLedger needs the orchestrator client and "
                "the agent-side Postgres pool"
            )
        try:
            self.parent_thread_id = str(UUID(str(parent_thread_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise ValueError(
                "SessionSubagentLedger needs an exact parent thread UUID"
            ) from exc
        if authority_provider is not None and parent_authority is not None:
            raise ValueError("provide an authority provider or a fixed authority")
        if authority_provider is None:
            if parent_authority is None:
                raise ValueError(
                    "SessionSubagentLedger needs an exact authority provider"
                )
            fixed = coerce_session_parent_authority(parent_authority)
            if fixed.execution_lane == "stateless":
                raise ValueError(
                    "a stateless session ledger needs a per-operation "
                    "authority provider"
                )
            if not fixed.for_thread(self.parent_thread_id):
                raise ValueError("fixed session authority names another thread")

            def _fixed_authority() -> SessionParentAuthority:
                return fixed

            authority_provider = _fixed_authority

        self.client = client
        self.postgres = postgres
        self.parent_context = parent_context
        self._authority_provider = authority_provider
        self._rows: Dict[str, str] = {}
        self._generations: Dict[str, str] = {}
        self._parent_threads: Dict[str, str] = {}
        self._handles: Dict[str, str] = {}
        self._background: Dict[str, bool] = {}
        self._failed: set[str] = set()

    @classmethod
    def from_context(cls, context: Any) -> Optional["SessionSubagentLedger"]:
        """Build only when the session installed every durable dependency."""

        client = getattr(context, "orchestrator_client", None)
        postgres = getattr(context, "postgres_db", None)
        parent_thread_id = getattr(context, "thread_id", None) or getattr(
            context, "_thread_id", None
        )
        provider = getattr(context, "_session_parent_authority_provider", None)
        fixed = getattr(context, "_session_parent_authority", None)
        if provider is None:
            provider = getattr(client, "session_parent_authority_provider", None)
        if fixed is None:
            fixed = getattr(client, "session_parent_authority", None)
        if (
            client is None
            or postgres is None
            or parent_thread_id is None
            or (provider is None and fixed is None)
        ):
            return None
        return cls(
            client,
            postgres,
            parent_thread_id=parent_thread_id,
            parent_context=context,
            authority_provider=provider,
            parent_authority=fixed if provider is None else None,
        )

    @property
    def rows(self) -> Dict[str, str]:
        return dict(self._rows)

    @property
    def generations(self) -> Dict[str, str]:
        return dict(self._generations)

    @property
    def failed(self) -> set[str]:
        return set(self._failed)

    def thread_id_for(self, subagent_id: str) -> Optional[str]:
        return self._rows.get(str(subagent_id))

    def runtime_generation_for(self, subagent_id: str) -> Optional[str]:
        return self._generations.get(str(subagent_id))

    async def _authority(
        self,
        *,
        parent_thread_id: str | None = None,
        run_in_background: bool = False,
    ) -> SessionParentAuthority:
        try:
            value = self._authority_provider()
            if inspect.isawaitable(value):
                value = await value
            authority = coerce_session_parent_authority(value)
        except SessionParentAuthorityRefused:
            raise
        except Exception as exc:
            raise SessionParentAuthorityRefused("invalid") from exc
        expected = parent_thread_id or self.parent_thread_id
        if not authority.for_thread(expected) or not authority.for_thread(
            self.parent_thread_id
        ):
            raise SessionParentAuthorityRefused("parent_mismatch")
        if run_in_background and authority.execution_lane == "stateless":
            raise SessionParentAuthorityRefused("stateless_background_unsupported")
        return authority

    def _validated_live_identity(
        self, row: Mapping[str, Any]
    ) -> tuple[str, str, str, str, str, bool] | None:
        try:
            thread_id = str(UUID(str(row.get("thread_id"))))
            if row.get("id") is not None and UUID(str(row.get("id"))) != UUID(
                thread_id
            ):
                return None
            generation = str(UUID(str(row.get("runtime_generation"))))
            parent_thread_id = str(UUID(str(row.get("parent_thread_id"))))
        except (TypeError, ValueError, AttributeError):
            return None
        if parent_thread_id != self.parent_thread_id or row.get(
            "parent_job_id"
        ) not in (
            None,
            "",
        ):
            return None
        handle_value = row.get("handle")
        if not isinstance(handle_value, str) or not handle_value.strip():
            return None
        handle = handle_value.strip()
        if handle != handle_value or len(handle) > 120:
            return None
        background = row.get("run_in_background")
        if not isinstance(background, bool):
            return None
        recovery_kind = row.get("recovery_kind", "live")
        live = (
            recovery_kind == "live"
            and row.get("status") in {"queued", "running"}
            and row.get("thread_status") in {"created", "active"}
        )
        terminal_foreground = (
            recovery_kind == "terminal_foreground"
            and background is False
            and is_terminal_status(row.get("status"))
            and row.get("thread_status") == "ended"
            and isinstance(row.get("parent_tool_call_id"), str)
            and bool(str(row.get("parent_tool_call_id") or "").strip())
        )
        if not live and not terminal_foreground:
            return None
        subagent_type = row.get("subagent_type")
        if not isinstance(subagent_type, str) or not subagent_type.strip():
            return None
        return (
            thread_id,
            thread_id,
            generation,
            parent_thread_id,
            handle,
            background,
        )

    def _can_adopt_live_identity(
        self, identity: tuple[str, str, str, str, str, bool]
    ) -> bool:
        child_id, thread_id, generation, parent_thread_id, handle, background = identity
        expected = (
            (self._rows, thread_id),
            (self._generations, generation),
            (self._parent_threads, parent_thread_id),
            (self._handles, handle),
            (self._background, background),
        )
        return all(mapping.get(child_id, value) == value for mapping, value in expected)

    def adopt_live(self, row: Mapping[str, Any]) -> bool:
        if not isinstance(row, Mapping):
            return False
        identity = self._validated_live_identity(row)
        if identity is None or not self._can_adopt_live_identity(identity):
            return False
        child_id, thread_id, generation, parent_thread_id, handle, background = identity
        self._rows[child_id] = thread_id
        self._generations[child_id] = generation
        self._parent_threads[child_id] = parent_thread_id
        self._handles[child_id] = handle
        self._background[child_id] = background
        self._failed.discard(child_id)
        return True

    def _parent_iteration(self, fields: Dict[str, Any]) -> Optional[int]:
        explicit = fields.get("parent_iteration")
        if explicit is not None:
            try:
                return int(explicit)
            except (TypeError, ValueError):
                return None
        value = getattr(self.parent_context, "_current_turn_count", None)
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    async def open(self, subagent_id: str, **fields: Any) -> Optional[Dict[str, str]]:
        child_key = str(subagent_id)
        background = bool(fields.get("run_in_background", False))
        if fields.get("parent_job_id") not in (None, ""):
            self._failed.add(child_key)
            raise SubagentPersistenceRefused(
                "a session child cannot carry worker-job parent authority"
            )
        parent_thread_id = str(fields.get("parent_thread_id") or "").strip()
        if parent_thread_id != self.parent_thread_id:
            self._failed.add(child_key)
            if background:
                raise SubagentPersistenceRefused(
                    f"background session child parent mismatch for {child_key}"
                )
            return None
        authority = await self._authority(run_in_background=background)
        initial_status = str(fields.get("status") or "running").strip()
        if initial_status not in {"queued", "running"}:
            raise ValueError("a subagent must open queued or running")
        parent_tool_call_id = (
            str(fields.get("parent_tool_call_id") or "").strip() or None
        )
        parent_iteration = self._parent_iteration(fields)
        parent_input_raw = fields.get("parent_input_message_id")
        parent_ai_raw = fields.get("parent_ai_message_id")
        if (
            parent_tool_call_id is None
            or parent_iteration is None
            or parent_iteration <= 0
            or not isinstance(parent_input_raw, str)
            or not parent_input_raw.strip()
            or not isinstance(parent_ai_raw, str)
            or not parent_ai_raw.strip()
        ):
            self._failed.add(child_key)
            raise SubagentPersistenceRefused(
                "a session child needs exact durable parent input, AI, call, "
                "and turn identities"
            )
        # Persistent message ids are minted as provider-safe strings and
        # coerced by the direct DB writer.  Capture the identical UUIDs here so
        # the orchestrator can verify the exact live rows transactionally.
        from agent.database.postgres_db import _coerce_row_id

        parent_input_message_id = str(_coerce_row_id(parent_input_raw.strip()))
        parent_ai_message_id = str(_coerce_row_id(parent_ai_raw.strip()))
        created = await self.client.create_session_subagent_thread(
            parent_thread_id,
            parent_authority=authority,
            subagent_id=child_key,
            handle=str(fields.get("handle") or ""),
            subagent_type=str(fields.get("subagent_type") or ""),
            parent_tool_call_id=parent_tool_call_id,
            parent_input_message_id=parent_input_message_id,
            parent_ai_message_id=parent_ai_message_id,
            isolation=str(fields.get("isolation") or "shared"),
            write_policy=str(fields.get("write_policy") or "none"),
            owned_paths=[str(path) for path in (fields.get("owned_paths") or [])],
            brief_description=str(fields.get("brief_description") or ""),
            parent_iteration=parent_iteration,
            fork=bool(fields.get("fork", False)),
            run_in_background=background,
            initial_status=initial_status,
        )
        if not isinstance(created, Mapping):
            self._failed.add(child_key)
            if background:
                raise SubagentPersistenceRefused(
                    f"background session child create refused for {child_key}"
                )
            return None
        try:
            exact_child = str(UUID(child_key))
            exact_thread = str(UUID(str(created.get("thread_id"))))
            generation = str(UUID(str(created.get("runtime_generation"))))
        except (TypeError, ValueError, AttributeError):
            exact_child = exact_thread = generation = ""
        if not exact_child or exact_child != exact_thread or not generation:
            self._failed.add(child_key)
            if background:
                raise SubagentPersistenceRefused(
                    "background session child create returned a mismatched identity "
                    f"or generation for {child_key}"
                )
            return None
        self._rows[child_key] = exact_thread
        self._generations[child_key] = generation
        self._parent_threads[child_key] = parent_thread_id
        self._handles[child_key] = str(fields.get("handle") or "")
        self._background[child_key] = background
        self._failed.discard(child_key)
        return {"thread_id": exact_thread, "runtime_generation": generation}

    async def persist_message(
        self, subagent_id: str, msg: Any, turn_number: int
    ) -> None:
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            raise SubagentPersistenceRefused(
                f"session child transcript has no durable generation for {child_key}"
            )
        authority = await self._authority()
        row = _serialize_message_row(msg, int(turn_number or 0))
        saved = await self.postgres.save_session_subagent_thread_message(
            thread_id=thread_id,
            parent_thread_id=self.parent_thread_id,
            parent_authority=authority,
            runtime_generation=generation,
            **row,
        )
        if not saved:
            raise SubagentPersistenceRefused(
                f"session child transcript generation refused for {child_key}"
            )

    async def persist_seed(self, subagent_id: str, messages: Sequence[Any]) -> bool:
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            return False
        authority = await self._authority()
        saved = await self.postgres.save_session_subagent_thread_messages(
            thread_id=thread_id,
            parent_thread_id=self.parent_thread_id,
            parent_authority=authority,
            runtime_generation=generation,
            messages=[_serialize_seed_message(message) for message in messages],
        )
        if not saved:
            raise SubagentPersistenceRefused(
                f"session child seed generation refused for {child_key}"
            )
        return True

    async def update(self, subagent_id: str, **fields: Any) -> None:
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            raise SubagentPersistenceRefused(
                f"session child lifecycle has no durable generation for {child_key}"
            )
        kwargs: Dict[str, Any] = {}
        status = fields.get("status")
        terminal = False
        if status is not None:
            kind = str(status)
            terminal = is_terminal_status(kind)
            if terminal and self._background.get(child_key, False):
                raise SubagentPersistenceRefused(
                    "background session children must terminalize with delivery"
                )
            kwargs["subagent_status"] = kind
            kwargs["status"] = (
                "ended" if terminal else "created" if kind == "queued" else "active"
            )
            kwargs["ended"] = terminal
        for key in ("outcome", "report_path", "error"):
            if fields.get(key) is not None:
                kwargs[key] = str(fields[key])
        for key in ("turns", "tokens"):
            if fields.get(key) is not None:
                try:
                    kwargs[key] = int(fields[key])
                except (TypeError, ValueError):
                    continue
        if not kwargs:
            return
        authority = await self._authority()
        updated = await self.postgres.update_session_subagent_thread(
            thread_id,
            parent_thread_id=self.parent_thread_id,
            parent_authority=authority,
            runtime_generation=generation,
            **kwargs,
        )
        if not updated:
            raise SubagentPersistenceRefused(
                f"session child lifecycle generation refused for {child_key}"
            )

    async def terminalize_and_enqueue(
        self,
        subagent_id: str,
        *,
        delivery_id: str,
        message: str,
        timestamp: Any,
        status: str,
        **fields: Any,
    ) -> Optional[Dict[str, Any]]:
        del timestamp
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            return None
        kind = str(status or "").strip()
        if not is_terminal_status(kind):
            raise ValueError("terminal delivery requires a terminal child status")
        background = self._background.get(child_key, False)
        if not background:
            raise SubagentPersistenceRefused(
                "a foreground session child must not enqueue a second result"
            )
        expected_delivery = str(session_subagent_delivery_id(thread_id, generation))
        try:
            supplied_delivery = str(UUID(str(delivery_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise SubagentPersistenceRefused(
                "session child delivery id is malformed"
            ) from exc
        if supplied_delivery != expected_delivery:
            raise SubagentPersistenceRefused(
                "session child delivery id is not generation-stable"
            )
        authority = await self._authority(run_in_background=True)
        kwargs: Dict[str, Any] = {
            "runtime_generation": generation,
            "subagent_status": kind,
            "run_in_background": True,
            "message": str(message),
        }
        for key in ("outcome", "report_path", "error"):
            if fields.get(key) is not None:
                kwargs[key] = str(fields[key])
        for key in ("turns", "tokens"):
            if fields.get(key) is not None:
                try:
                    kwargs[key] = int(fields[key])
                except (TypeError, ValueError):
                    continue
        return await self.client.terminalize_session_subagent_thread(
            self.parent_thread_id,
            thread_id,
            parent_authority=authority,
            **kwargs,
        )

    async def terminalize_foreground_orphan_and_enqueue(
        self,
        subagent_id: str,
        *,
        delivery_id: str,
        message: str,
        status: str,
        **fields: Any,
    ) -> Optional[Dict[str, Any]]:
        """End a crash-orphaned foreground child and emit one durable event.

        Normal foreground children return synchronously and must never enqueue
        a duplicate. After a session process restart that return path no
        longer exists, so this explicitly marked operation converts the
        durable partial transcript into generation-stable parent evidence.
        """

        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            return None
        if self._background.get(child_key, False):
            raise SubagentPersistenceRefused(
                "background session children use normal terminal delivery"
            )
        kind = str(status or "").strip()
        if not is_terminal_status(kind):
            raise SubagentPersistenceRefused(
                "foreground orphan recovery requires terminal status"
            )
        expected_delivery = str(session_subagent_delivery_id(thread_id, generation))
        try:
            supplied_delivery = str(UUID(str(delivery_id)))
        except (TypeError, ValueError, AttributeError) as exc:
            raise SubagentPersistenceRefused(
                "session child delivery id is malformed"
            ) from exc
        if supplied_delivery != expected_delivery:
            raise SubagentPersistenceRefused(
                "session child delivery id is not generation-stable"
            )
        if not str(message or "").strip():
            raise SubagentPersistenceRefused(
                "foreground orphan recovery requires durable evidence"
            )
        authority = await self._authority()
        kwargs: Dict[str, Any] = {
            "runtime_generation": generation,
            "subagent_status": kind,
            "run_in_background": False,
            "message": str(message),
            "foreground_orphan_recovery": True,
        }
        for key in ("outcome", "report_path", "error"):
            if fields.get(key) is not None:
                kwargs[key] = str(fields[key])
        for key in ("turns", "tokens"):
            if fields.get(key) is not None:
                try:
                    kwargs[key] = int(fields[key])
                except (TypeError, ValueError):
                    continue
        return await self.client.terminalize_session_subagent_thread(
            self.parent_thread_id,
            thread_id,
            parent_authority=authority,
            **kwargs,
        )

    async def reopen(self, subagent_id: str) -> Optional[Dict[str, Any]]:
        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        generation = self._generations.get(child_key)
        if thread_id is None or generation is None:
            return None
        background = self._background.get(child_key, False)
        authority = await self._authority(run_in_background=background)
        try:
            result = await self.client.reopen_session_subagent_thread(
                self.parent_thread_id,
                thread_id,
                parent_authority=authority,
                runtime_generation=generation,
            )
        except SessionParentAuthorityRefused:
            raise
        except Exception:
            # Reopen is server-idempotent across a lost acknowledgement: a
            # retry of G1 adopts only the pristine queued G2. Refresh parent
            # authority before the one bounded retry so stateless callers do
            # not reuse a lease that rotated with the transport failure.
            authority = await self._authority(run_in_background=background)
            result = await self.client.reopen_session_subagent_thread(
                self.parent_thread_id,
                thread_id,
                parent_authority=authority,
                runtime_generation=generation,
            )
        if result and result.get("result") == "reopened":
            try:
                successor = str(UUID(str(result.get("runtime_generation"))))
            except (TypeError, ValueError, AttributeError):
                return None
            self._generations[child_key] = successor
        return dict(result) if result else None

    async def load_messages(self, subagent_id: str) -> RestoredSubagentTranscript:
        """Load an adopted session child's exact transcript under authority."""

        child_key = str(subagent_id)
        thread_id = self._rows.get(child_key)
        if thread_id is None:
            raise SubagentPersistenceRefused(
                f"session subagent transcript identity is unknown for {child_key}"
            )
        background = self._background.get(child_key, False)
        authority = await self._authority(run_in_background=background)
        await self.postgres.session_parent_authority_current(authority)
        rows = await self.postgres.get_thread_messages_history(
            thread_id,
            limit=None,
            include_provider_raw=True,
            order_by_seq=True,
        )
        if not isinstance(rows, list):
            raise SubagentPersistenceRefused(
                "session subagent transcript is not a list"
            )
        try:
            turn_number = max(
                (int(row.get("turn_number") or 0) for row in rows), default=0
            )
        except (TypeError, ValueError, AttributeError) as exc:
            raise SubagentPersistenceRefused(
                "session subagent transcript has an invalid turn cursor"
            ) from exc
        return RestoredSubagentTranscript(
            messages=restore_subagent_messages(rows),
            turn_number=turn_number,
        )

    async def list_live(self, parent_thread_id: str) -> list[Dict[str, Any]]:
        authority = await self._authority(parent_thread_id=str(parent_thread_id))
        rows = await self.client.list_live_session_subagent_threads(
            self.parent_thread_id, parent_authority=authority
        )
        return self._adopt_listed(rows)

    async def list_live_recovery(self, parent_thread_id: str) -> Dict[str, Any]:
        """The candidates (adopted, as :meth:`list_live`) and the per-turn plans.

        ``recovery_turns`` is ``None`` when the orchestrator predates batch
        settle or sent them malformed; the caller then recovers every
        candidate on its own.
        """

        authority = await self._authority(parent_thread_id=str(parent_thread_id))
        listed = await self.client.list_live_session_subagent_recovery(
            self.parent_thread_id, parent_authority=authority
        )
        if not isinstance(listed, Mapping):
            raise SubagentPersistenceRefused("live session child roster is not a map")
        plans = listed.get("recovery_turns")
        if plans is not None and not isinstance(plans, list):
            logger.warning(
                "Ignoring session recovery plans that are not a list (thread %s)",
                self.parent_thread_id,
            )
            plans = None
        return {
            "subagents": self._adopt_listed(listed.get("subagents")),
            "recovery_turns": plans,
        }

    async def settle_batch(
        self,
        *,
        parent_input_message_id: str,
        parent_iteration: int,
        members: Sequence[Mapping[str, Any]],
    ) -> Dict[str, Any]:
        """Settle one abandoned delegation turn under a fresh exact authority."""

        authority = await self._authority()
        result = await self.client.settle_session_subagent_batch(
            self.parent_thread_id,
            parent_authority=authority,
            parent_input_message_id=str(parent_input_message_id),
            parent_iteration=int(parent_iteration),
            members=[dict(member) for member in members],
        )
        if not isinstance(result, Mapping):
            raise SubagentPersistenceRefused("session batch settle gave no verdict")
        return dict(result)

    def _adopt_listed(self, rows: Any) -> list[Dict[str, Any]]:
        if not isinstance(rows, list):
            raise SubagentPersistenceRefused("live session child roster is not a list")
        normalized: list[Dict[str, Any]] = []
        identities: list[tuple[str, str, str, str, str, bool]] = []
        seen: set[str] = set()
        for raw_row in rows:
            if not isinstance(raw_row, Mapping):
                raise SubagentPersistenceRefused(
                    "live session child roster row is malformed"
                )
            row = dict(raw_row)
            identity = self._validated_live_identity(row)
            if (
                identity is None
                or identity[0] in seen
                or not self._can_adopt_live_identity(identity)
            ):
                raise SubagentPersistenceRefused(
                    "live session child identity is malformed or conflicting"
                )
            seen.add(identity[0])
            normalized.append(row)
            identities.append(identity)
        for row in normalized:
            if not self.adopt_live(row):  # pragma: no cover - prevalidated
                raise SubagentPersistenceRefused(
                    "live session child adoption changed unexpectedly"
                )
        return normalized

    async def lookup(
        self, parent_thread_id: str, parent_tool_call_id: str
    ) -> Optional[Dict[str, Any]]:
        authority = await self._authority(parent_thread_id=str(parent_thread_id))
        call_id = str(parent_tool_call_id or "").strip()
        if not call_id:
            return None
        row = await self.postgres.get_session_subagent_thread_by_call(
            self.parent_thread_id,
            call_id,
            parent_authority=authority,
        )
        if not row:
            return None
        if (
            row.get("parent_job_id") not in (None, "")
            or str(row.get("parent_thread_id") or "") != self.parent_thread_id
            or str(row.get("parent_tool_call_id") or "").strip() != call_id
        ):
            raise SubagentPersistenceRefused(
                "session child replay row belongs to another parent"
            )
        # Unlike the worker ledger, session lookup deliberately exposes a
        # live row.  The strict runtime must distinguish "absent" from an
        # ambiguous create that committed before its receipt was lost; hiding
        # queued/running here would authorize a second child for the same
        # durable tool-call key.
        result = dict(row)
        self._adopt_terminal_lookup(result, parent_tool_call_id=call_id)
        return result

    async def lookup_handle(
        self, parent_thread_id: str, handle: str
    ) -> Optional[Dict[str, Any]]:
        """Resolve a cold terminal background child by its stable handle."""

        authority = await self._authority(parent_thread_id=str(parent_thread_id))
        row = await self.postgres.get_session_subagent_thread_by_handle(
            self.parent_thread_id,
            str(handle),
            parent_authority=authority,
        )
        if not row:
            return None
        result = dict(row)
        status = str(result.get("subagent_status") or "").strip()
        if not is_terminal_status(status):
            raise SubagentPersistenceRefused(
                f"session subagent {handle} still has a live durable generation"
            )
        call_id = str(result.get("parent_tool_call_id") or "").strip()
        if str(result.get("subagent_handle") or "").strip() != str(
            handle
        ).strip() or not self._adopt_terminal_lookup(
            result, parent_tool_call_id=call_id
        ):
            raise SubagentPersistenceRefused(
                f"session subagent {handle} is not an addressable background child"
            )
        return result

    def _adopt_terminal_lookup(
        self, row: Mapping[str, Any], *, parent_tool_call_id: str
    ) -> bool:
        """Seed the exact terminal identity needed after a parent restart."""

        metadata = row.get("metadata")
        if isinstance(metadata, (str, bytes)):
            try:
                metadata = json.loads(metadata)
            except (TypeError, ValueError):
                return False
        spawn = metadata.get("subagent") if isinstance(metadata, Mapping) else None
        try:
            child_id = str(UUID(str(row.get("id") or row.get("thread_id"))))
            generation = str(UUID(str(row.get("runtime_generation"))))
            parent_id = str(UUID(str(row.get("parent_thread_id"))))
        except (TypeError, ValueError, AttributeError):
            return False
        handle = str(row.get("subagent_handle") or row.get("handle") or "").strip()
        call_id = str(row.get("parent_tool_call_id") or "").strip()
        if (
            parent_id != self.parent_thread_id
            or row.get("parent_job_id") not in (None, "")
            or call_id != parent_tool_call_id
            or not handle
            or row.get("status") not in {"ended", None}
            or not is_terminal_status(row.get("subagent_status") or row.get("status"))
            or not isinstance(spawn, Mapping)
            or spawn.get("run_in_background") is not True
        ):
            return False
        identity = (
            child_id,
            child_id,
            generation,
            parent_id,
            handle,
            True,
        )
        if not self._can_adopt_live_identity(identity):
            return False
        self._rows[child_id] = child_id
        self._generations[child_id] = generation
        self._parent_threads[child_id] = parent_id
        self._handles[child_id] = handle
        self._background[child_id] = True
        self._failed.discard(child_id)
        return True


__all__ = ["AuthorityProvider", "SessionSubagentLedger"]
