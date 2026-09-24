"""Owner-bounded VM creation progress for current pinned session sources."""

from __future__ import annotations

from uuid import UUID

from orchestrator.services.vm_creation_progress import vm_creation_projection


async def thread_creation_views(
    db, thread_ids: list[str], *, viewer_user_id: str, admin: bool = False,
) -> dict[str, dict]:
    """Read only exact authorized session sources, never installation capacity.

    The caller first passes the existing owner/admin gate. Rechecking user
    ownership here closes a transfer race between that gate and this read.
    The retry, waiter, VM context and pinned runtime must all refer to the
    current thread incarnation; legacy or partial identity simply has no view.
    """
    try:
        ids = [UUID(value) for value in thread_ids]
        user = UUID(viewer_user_id)
    except (TypeError, ValueError, AttributeError):
        return {}
    if not ids:
        return {}
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT t.id, t.status,
                   jsonb_build_object(
                       'request_id', c.request_id, 'stage', 'creation',
                       'state', c.state, 'reason', c.reason,
                       'ready_at', c.ready_at, 'pending', true,
                       'resume_blocked', true,
                       'resource_wait', (
                           SELECT jsonb_build_object(
                               'state', w.state, 'reason', w.reason,
                               'enqueued_at', w.enqueued_at,
                               'guest_vcpus', w.guest_vcpus,
                               'guest_memory_bytes', w.guest_memory_bytes
                           ) FROM vm_resource_waiters w
                           WHERE w.request_id=c.request_id
                             AND w.owner_kind='thread' AND w.thread_id=t.id
                             AND w.provision_generation=c.provision_generation
                             AND w.state IN ('waiting','nonfit')
                       )
                   ) AS creation
              FROM threads t
              JOIN vm_creation_retries c
                ON c.owner_kind='thread' AND c.thread_id=t.id
               AND c.thread_runtime_generation=t.runtime_generation
               AND c.thread_agent_id IS NOT DISTINCT FROM t.agent_id
               AND c.thread_attach_token IS NOT DISTINCT FROM t.runtime_attach_token
               AND c.thread_owner_user_id IS NOT DISTINCT FROM t.user_id
               AND c.thread_owner_project_id IS NOT DISTINCT FROM t.project_id
               AND c.provision_generation::text=t.metadata->'vm'->>'provision_generation'
               AND c.request_id::text=t.metadata->'vm'->>'creation_request_id'
               AND c.thread_wake_operation_id::text IS NOT DISTINCT FROM
                   t.metadata->'vm'->>'idle_wake_operation_id'
               AND c.ready_at IS NULL
             WHERE t.id=ANY($1::uuid[]) AND t.kind='session'
               AND t.execution_lane='pinned'
               AND t.status NOT IN ('ending','ended')
               AND t.runtime_retirement_token IS NULL
               AND ($3::boolean OR t.user_id=$2::uuid)
            """,
            ids, user, admin,
        )
    result = {}
    for row in rows:
        projection = vm_creation_projection(
            {"_vm_creation": row["creation"], "status": row["status"]}
        )
        if projection is not None:
            result[str(row["id"])] = projection
    return result
