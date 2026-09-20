"""Episode metadata inside an existing authorized owner workflow transaction.

This helper grants no owner authorization and performs no physical operation.
Caller must first acquire its existing owner/runtime/claim locks in their normal
order and prove the source transition. A source CAS loss must roll back the same
transaction. No production transition adapter invokes this foundation yet.
"""

import json
from uuid import UUID

from shared.workspace_idle_policy import (
    DEFAULT_WARM_SECONDS,
    IdlePolicyError,
    RuntimeIdentity,
    episode_document,
    read_episode,
    transition_idle_episode,
)


async def apply_idle_transition_on_conn(
    conn,
    *,
    runtime,
    event,
    expected_revision,
    expected_episode_id,
    wait_kind=None,
    wait_key=None,
    warm_seconds=DEFAULT_WARM_SECONDS,
    extension_cap=4,
):
    if not conn.is_in_transaction():
        raise IdlePolicyError("idle_transaction_required")
    if not isinstance(runtime, RuntimeIdentity) or not isinstance(
        runtime.owner_kind, str
    ):
        raise IdlePolicyError("invalid_idle_identity")
    table = {"job": "jobs", "thread": "threads"}.get(runtime.owner_kind)
    if table is None:
        raise IdlePolicyError("invalid_idle_identity")
    try:
        if (
            not isinstance(runtime.owner_id, str)
            or str(UUID(runtime.owner_id)) != runtime.owner_id
        ):
            raise ValueError
        owner_id = UUID(runtime.owner_id)
    except ValueError:
        raise IdlePolicyError("invalid_idle_identity") from None
    row = await conn.fetchrow(
        "SELECT workspace_idle_revision,workspace_idle_episode FROM "
        + table
        + " WHERE id=$1 FOR UPDATE",
        owner_id,
    )
    if row is None:
        raise IdlePolicyError("idle_owner_missing")
    if (
        type(expected_revision) is not int
        or expected_revision != row["workspace_idle_revision"]
    ):
        raise IdlePolicyError("episode_changed")
    document = row["workspace_idle_episode"]
    if isinstance(document, str):
        document = json.loads(document)
    prior = read_episode(document, revision=row["workspace_idle_revision"])
    now = await conn.fetchval("SELECT clock_timestamp()")
    result = transition_idle_episode(
        prior,
        revision=expected_revision,
        expected_episode_id=expected_episode_id,
        event=event,
        runtime=runtime,
        now=now,
        wait_kind=wait_kind,
        wait_key=wait_key,
        warm_seconds=warm_seconds,
        extension_cap=extension_cap,
    )
    if result.revision == expected_revision:
        return result
    document = episode_document(result.episode)
    updated = await conn.fetchval(
        "UPDATE "
        + table
        + " SET workspace_idle_revision=$2,workspace_idle_episode=$3::jsonb "
        "WHERE id=$1 AND workspace_idle_revision=$4 RETURNING workspace_idle_revision",
        owner_id,
        result.revision,
        json.dumps(document) if document is not None else None,
        expected_revision,
    )
    if updated != result.revision:
        raise IdlePolicyError("episode_changed")
    return result
