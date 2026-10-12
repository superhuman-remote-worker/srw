"""Transaction adapters for accepted officer/system human handoffs.

Nominate routes without retaining locks, then take post/thread (officer only),
Job, and route locks in publication order. Delivery is always after commit.
"""

import json
import os
from uuid import UUID

from orchestrator.services.workspace_idle_events import record_human_route_wait_on_conn


async def lock_officer_drain_jobs_on_conn(conn, *, project_id, officer_thread_id):
    """Post/thread owner locks every affected Job before wake/route mutation.

    The stable post lock fences new pending-officer publishers. Constrain the
    later bulk route CAS to this exact set; never append Job locks after it.
    None preserves the legacy default-off bulk writer.
    """
    if os.getenv("WORKSPACE_IDLE_RELEASE_ENABLED", "false").lower() != "true":
        return None
    rows = await conn.fetch(
        "SELECT j.id FROM jobs j WHERE j.id IN ("
        "SELECT r.job_id FROM job_message_routes r WHERE r.project_id=$1 "
        "AND r.officer_thread_id=$2 AND r.state='pending_officer' AND r.blocking) "
        "ORDER BY j.id FOR UPDATE OF j",
        project_id,
        officer_thread_id,
    )
    return [row["id"] for row in rows]


async def record_officer_drain_on_conn(conn, *, rows, locked_job_ids):
    if locked_job_ids is None:
        return
    for row in rows:
        if row["job_id"] not in locked_job_ids:
            raise RuntimeError("officer drain Job scope changed")
        await record_human_route_wait_on_conn(
            conn, job_id=row["job_id"], route_id=row["route_id"]
        )


async def lock_handoff_source_on_conn(
    conn, *, route_id, actor_kind, officer_thread_id, officer_incarnation
):
    """Return the nominated scope only after current source and Job locks.

    The caller must include this scope in its subsequent route CAS. This helper
    consumes authenticated officer facts from the action adapter, never actor_id.
    """
    route = await conn.fetchrow(
        "SELECT job_id,project_id FROM job_message_routes WHERE route_id=$1", route_id
    )
    if route is None:
        return None
    if actor_kind == "officer":
        if type(officer_incarnation) is not int or officer_incarnation < 0:
            return None
        try:
            officer_id = UUID(str(officer_thread_id))
        except (ValueError, TypeError):
            return None
        post = await conn.fetchrow(
            "SELECT thread_id,incarnations FROM project_officers WHERE project_id=$1 FOR UPDATE",
            route["project_id"],
        )
        if post is None or post["thread_id"] != officer_id:
            return None
        incarnations = post["incarnations"]
        if isinstance(incarnations, str):
            incarnations = json.loads(incarnations)
        if (
            not isinstance(incarnations, list)
            or len(incarnations) != officer_incarnation
        ):
            return None
        thread = await conn.fetchrow(
            "SELECT project_id,status FROM threads WHERE id=$1 FOR UPDATE", officer_id
        )
        if (
            thread is None
            or thread["project_id"] != route["project_id"]
            or thread["status"] == "ended"
        ):
            return None
    elif actor_kind != "system":
        return None
    job = await conn.fetchrow(
        "SELECT id,project_id FROM jobs WHERE id=$1 FOR UPDATE", route["job_id"]
    )
    if job is None or job["project_id"] != route["project_id"]:
        return None
    return route


async def claim_human_route_sla(db, *, now, limit):
    """Bounded nominations; each accepted Job -> route CAS owns one episode."""
    if type(limit) is not int or not 1 <= limit <= 100:
        raise ValueError("route SLA limit must be between 1 and 100")
    accepted = []
    seen = []
    for _ in range(limit):
        async with db.acquire() as conn:
            async with conn.transaction():
                # Skip locked and scope-ineligible Jobs before consuming the
                # limit, so a busy prefix cannot starve unrelated questions.
                # Lock only this one Job; no route lock or multi-Job lock set.
                candidate = await conn.fetchrow(
                    "SELECT r.route_id,r.job_id,r.project_id FROM job_message_routes r "
                    "JOIN jobs j ON j.id=r.job_id AND j.project_id=r.project_id "
                    "WHERE r.state='pending_officer' AND r.blocking "
                    "AND r.officer_deadline <= $1 AND NOT(r.route_id=ANY($2::uuid[])) "
                    "ORDER BY r.officer_deadline,r.route_id LIMIT 1 "
                    "FOR UPDATE OF j SKIP LOCKED",
                    now,
                    seen,
                )
                if candidate is None:
                    break
                seen.append(candidate["route_id"])
                # Recheck the complete nomination after Job acquisition. A
                # reply may have settled the route while we waited for locks.
                row = await conn.fetchrow(
                    "UPDATE job_message_routes SET state='escalated_to_user', "
                    "transitions=transitions||jsonb_build_array(jsonb_build_object("
                    "'at',clock_timestamp()::text,'from',state,'to','escalated_to_user',"
                    "'actor_kind','system','note','officer_sla_expired')),updated_at=now() "
                    "WHERE route_id=$1 AND job_id=$2 AND project_id=$3 "
                    "AND state='pending_officer' AND blocking AND officer_deadline <= $4 "
                    "RETURNING *",
                    candidate["route_id"],
                    candidate["job_id"],
                    candidate["project_id"],
                    now,
                )
                if row is None:
                    continue
                await record_human_route_wait_on_conn(
                    conn, job_id=row["job_id"], route_id=row["route_id"]
                )
                accepted.append(db._message_route_row_to_dict(row))
    return accepted
