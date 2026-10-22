"""The stateless workspace upgrade's one database transition (C1).

Runs on the control-admission transaction after it locked ``threads`` (and the
session ``run_queue`` row). It turns an idle lite Session into a stateless
container Session awaiting its first create:

* ``config_override.workspace.backend`` becomes ``sandbox``;
* ``workspace_container`` gets the pre-armed create nonce the INSERT path
  writes for container Sessions (``thread_admission.py`` ~:1203-1214), the
  upgrade's template record, and ``seed_from``;
* the old ``_workspace_binding`` moves under ``seed_from``, so the stateless
  classifier and the continuation runner's ``initial`` route accept the row
  (``session_creation_candidates.py`` ~:341).

The runtime generation is never rotated, which keeps the 0198 generation
trigger quiet; Task 1's probe pins that the 0198/0223 guard admits the rest.
A row with earlier container history, physical keys, VM evidence or a stop
marker is refused rather than repaired: those belong to End/Resume. The one
record taken over is End's stamp on a resumed lite row, which migration 0558
lets both guards read as empty for this UPDATE. A guard that still refuses the
UPDATE becomes the same typed refusal, never a 500.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any
from uuid import UUID

import asyncpg

from shared.backend_kinds import LITE_BACKENDS
from shared.session_retirement import STATELESS_STOP_KEYS
from shared.workspace_upgrade import SEED_FROM_KEY, metadata_object

logger = logging.getLogger(__name__)

UPGRADE_NOT_POSSIBLE = (
    "This Session's workspace can't be upgraded in place. "
    "Start a new Session with a container workspace."
)

#: What End's settlement leaves on every lite row it settles; Resume keeps it.
_END_STAMP = {"volume_reclaimed": False}

_HISTORY_SQL = """
SELECT EXISTS (
           SELECT 1 FROM managed_repository_workspace_creation_reservations
            WHERE owner_kind = 'thread' AND owner_id = $1
              AND scope = 'workspace_container')
    OR EXISTS (
           SELECT 1 FROM managed_repository_workspace_cleanup_intents
            WHERE owner_kind = 'thread' AND owner_id = $1
              AND scope = 'workspace_container')
    OR EXISTS (
           SELECT 1 FROM managed_repository_process_zero_receipts
            WHERE owner_kind = 'thread' AND owner_id = $1
              AND scope IN ('workspace_container', 'stateless_workspace'))
    OR EXISTS (
           SELECT 1 FROM thread_workspace_provision_intents
            WHERE thread_id = $1)
"""

_TRANSITION_SQL = """
UPDATE threads
   SET metadata = $2::jsonb, last_activity = now()
 WHERE id = $1
   AND execution_lane = 'stateless'
   AND runtime_generation = $3::uuid
   AND runtime_retirement_token IS NULL
   AND status IN ('created', 'active', 'awaiting_user')
RETURNING id
"""


class StatelessUpgradeRefused(RuntimeError):
    """A typed refusal whose message is safe to show the owner."""


def _refuse() -> StatelessUpgradeRefused:
    return StatelessUpgradeRefused(UPGRADE_NOT_POSSIBLE)


def stateless_upgrade_refusal(metadata: Any) -> str | None:
    """Why a lite row can't take the upgrade, from its metadata alone, or None.

    ``/connection`` offers the container only where this passes. Any
    ``workspace_container`` content but End's exact stamp refuses: the
    lifecycle guards treat a non-empty record without a provisioner as
    authority to retire, and only that stamp is read as empty (0558).
    """
    metadata = metadata_object(metadata)
    config_override = metadata.get("config_override")
    if not isinstance(config_override, dict) or not isinstance(
        config_override.get("workspace"), dict
    ):
        return "workspace_config_malformed"
    if config_override["workspace"].get("backend") not in LITE_BACKENDS:
        return "backend_not_lite"
    if metadata.get("vm") not in (None, {}):
        return "vm_context_present"
    if metadata.get("protected_cloud") not in (None, False):
        return "protected_cloud"
    if any(key in metadata for key in STATELESS_STOP_KEYS) or (
        "_stateless_workspace_retirement_settled" in metadata
    ):
        return "session_ending"
    workspace_context = metadata.get("workspace_container")
    if workspace_context not in (None, {}) and not (
        workspace_context == _END_STAMP
        and workspace_context["volume_reclaimed"] is False
    ):
        return "workspace_context_present"
    return None


async def begin_stateless_session_workspace_upgrade(
    conn: Any,
    thread: Mapping[str, Any],
    *,
    upgrade_config: Mapping[str, Any],
    upgrade_sources: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """CAS one locked lite row into a pending container Session; return its metadata."""

    thread_id = UUID(str(thread["id"]))
    metadata = metadata_object(thread.get("metadata"))
    refusal = stateless_upgrade_refusal(metadata)
    if refusal is None and await conn.fetchval(_HISTORY_SQL, thread_id):
        refusal = "container_history"
    if refusal is not None:
        logger.warning(
            "Stateless workspace upgrade of thread %s refused: %s", thread_id, refusal
        )
        raise _refuse()
    config_override = metadata["config_override"]
    workspace = config_override["workspace"]
    backend = workspace["backend"]

    generation = str(UUID(str(thread["runtime_generation"])))
    binding = metadata.get("_workspace_binding")
    next_metadata = dict(metadata)
    next_metadata.pop("_workspace_binding", None)
    next_metadata["config_override"] = {
        **config_override,
        "workspace": {**workspace, "backend": "sandbox"},
    }
    next_metadata["workspace_container"] = {
        "status": "pending",
        "provisioner": "k8s",
        "_runtime_creation": {
            "generation": generation,
            "mode": "create",
            "attempted": False,
            "replaces_uid": None,
        },
        "upgrade_config": dict(upgrade_config),
        "upgrade_sources": (
            dict(upgrade_sources) if upgrade_sources is not None else None
        ),
        SEED_FROM_KEY: {
            "backend": backend,
            "prefix": f"threads/{thread_id}/",
            "binding": dict(binding) if isinstance(binding, dict) and binding else None,
        },
    }
    try:
        updated = await conn.fetchval(
            _TRANSITION_SQL, thread_id, json.dumps(next_metadata), generation
        )
    except asyncpg.exceptions.CheckViolationError as exc:
        # The caller's transaction is aborted either way; a typed 409 keeps
        # the cockpit from retrying the same request id as if it were a 5xx.
        logger.warning(
            "Stateless workspace upgrade of thread %s refused by guard %s",
            thread_id,
            exc.constraint_name,
        )
        raise _refuse() from exc
    if updated is None:
        raise _refuse()
    return next_metadata


__all__ = [
    "StatelessUpgradeRefused",
    "UPGRADE_NOT_POSSIBLE",
    "begin_stateless_session_workspace_upgrade",
    "stateless_upgrade_refusal",
]
