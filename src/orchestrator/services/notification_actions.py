"""Binding the unified feed's declared actions to server-side effects.

Extracted verbatim from ``orchestrator.main`` (R1.B07 lane N, census group
``J_notifications``; unified_notification_system.md D7). This is the **only**
place that knows what a job, a sudo request, a loop or an officer *is* — the
notification centre renders the actions a row declares and POSTs them back,
and nothing downstream of that has to learn a domain.

:func:`register_notification_actions` runs once at startup after
``notification_service.connect()``; re-registration replaces, so a reload is
harmless. The handlers call request-free operations rather than re-entering
endpoint coroutines, because ``act()`` has already proven the caller is the
row's recipient.

Three registries are populated here and each has a distinct job:

* **actions** — what a button does. The ``vm_upgrade`` deny handler still
  demands a reason (reason-less denials demonstrably cause agent retry loops),
  and ``officer_question.reply`` still refuses a non-owner.
* **source loaders** — the detail pane's payload per ``source_kind``.
* **source probes** — what makes ``not_resolved`` ask the live source, so an
  un-enumerated writer (a sweeper, a future endpoint, a direct DB edit) cannot
  cause a stale mail. "Resolved" means nobody is waiting on a human any more;
  the ``thread`` probe deliberately returns False because an officer question
  has no state machine to consult.

Every collaborator that belongs to another batch arrives on
:class:`NotificationActionDependencies` and is never re-implemented here:
``resume_job_internal`` / ``approve_job_internal`` / ``apply_vm_upgrade_decision``
and the two request models are B09's job control, ``decide_permission_request``
is B10's, ``route_inbound_reply`` and ``resolve_job_notifications`` are lane M's
operations bound with their own dependency objects, and ``sudo_gate`` is the
existing sudo authority.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from fastapi import HTTPException

from orchestrator.services.notification_api import notification_jsonable
from orchestrator.services.notification_catalog import (
    ActionContext,
    ActionResult,
    register_action,
    register_source_loader,
    register_source_probe,
)
from orchestrator.services.officer_post_policy import (
    OFFICER_NOTE_MAX_CHARS,
    format_legate_note,
)

logger = logging.getLogger(__name__)


@dataclass
class NotificationActionDependencies:
    """Collaborators the registered handlers close over.

    The object is built once, at startup, and captured by the closures this
    module registers — which is exactly why every field is either a singleton
    the application owns for its whole life or a callable that resolves its own
    dependencies per call.
    """

    store: Any
    notifier: Any
    sudo_gate: Any
    kick_officer_event_drain: Callable[[Any], None]
    deliver_officer_note: Callable[..., Awaitable[str]]
    route_inbound_reply: Callable[..., Awaitable[tuple[str, int]]]
    resolve_job_notifications: Callable[..., Awaitable[None]]
    resume_job_internal: Callable[..., Awaitable[Any]]
    approve_job_internal: Callable[..., Awaitable[Any]]
    apply_vm_upgrade_decision: Callable[..., Awaitable[Any]]
    decide_permission_request: Callable[..., Awaitable[Any]]
    job_resume_request: type
    job_approve_request: type


def register_notification_actions(
    *, dependencies: NotificationActionDependencies
) -> None:
    """Bind the unified feed's declared actions to server-side effects and
    register the per-source detail-pane loaders.

    Runs once at startup after ``notification_service.connect()``;
    re-registration replaces, so a reload is harmless. This is the only place
    that knows what a job, a sudo request or an officer *is* — the center
    renders declared actions and POSTs them back (D7). Handlers call the
    request-free ``*_internal`` helpers rather than re-entering endpoint
    coroutines; ``act()`` has already proven the caller is the recipient.
    """

    def _actor(user: dict[str, Any]) -> str:
        return str(user.get("email") or user.get("id") or "operator")

    def _navigate(path: str) -> ActionResult:
        return ActionResult(result={"navigate": path})

    async def _owned_job(ctx: ActionContext) -> tuple[str, dict[str, Any]]:
        # record() addressed the row to jobs.user_id and act() verified the
        # caller is that recipient, so the caller is the job owner.
        job_id = str(ctx.params.get("job_id") or "")
        job = await dependencies.store.get_job(job_id) if job_id else None
        if not job:
            raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found")
        return job_id, job

    async def _sudo_row(ctx: ActionContext) -> tuple[str, dict[str, Any]]:
        request_id = str(ctx.params.get("request_id") or "")
        row = (
            await dependencies.sudo_gate._get_request(request_id)
            if request_id
            else None
        )
        if not row:
            raise HTTPException(
                status_code=404, detail=f"Sudo request '{request_id}' not found"
            )
        return request_id, dict(row)

    async def _resume(ctx: ActionContext) -> ActionResult:
        job_id, job = await _owned_job(ctx)
        result = await dependencies.resume_job_internal(
            job_id,
            user=ctx.user,
            job=job,
            request=dependencies.job_resume_request(
                feedback=ctx.params.get("feedback") or None
            ),
        )
        await dependencies.resolve_job_notifications(
            job_id, user=ctx.user, hook="resume"
        )
        return ActionResult(result=dict(result))

    async def _open_job(ctx: ActionContext) -> ActionResult:
        return _navigate(f"/jobs/{ctx.params.get('job_id')}")

    # --- review_queue -----------------------------------------------------
    @register_action("review_queue", "approve")
    async def _review_approve(ctx: ActionContext) -> ActionResult:
        job_id, job = await _owned_job(ctx)
        result = await dependencies.approve_job_internal(
            job_id,
            user=ctx.user,
            job=job,
            request=dependencies.job_approve_request(
                notes=ctx.params.get("notes") or None
            ),
        )
        await dependencies.resolve_job_notifications(
            job_id, user=ctx.user, hook="approve"
        )
        return ActionResult(result=dict(result))

    register_action("review_queue", "resume")(_resume)
    register_action("review_queue", "open")(_open_job)

    # --- budget_exceeded / incident ---------------------------------------
    register_action("budget_exceeded", "resume")(_resume)
    register_action("budget_exceeded", "open")(_open_job)
    register_action("incident", "open")(_open_job)

    # --- vm_upgrade ----------------------------------------------------------
    @register_action("vm_upgrade", "approve_upgrade")
    async def _vm_approve(ctx: ActionContext) -> ActionResult:
        request_id, row = await _sudo_row(ctx)
        result = await dependencies.apply_vm_upgrade_decision(
            request_id,
            row,
            approve=True,
            upgrade=True,
            reason="",
            decided_by=_actor(ctx.user),
        )
        return ActionResult(result=dict(result))

    @register_action("vm_upgrade", "resume_without_vm")
    async def _vm_resume_without(ctx: ActionContext) -> ActionResult:
        request_id, row = await _sudo_row(ctx)
        result = await dependencies.apply_vm_upgrade_decision(
            request_id,
            row,
            approve=True,
            upgrade=False,
            reason=str(ctx.params.get("reason") or ""),
            decided_by=_actor(ctx.user),
        )
        return ActionResult(result=dict(result))

    @register_action("vm_upgrade", "deny")
    async def _vm_deny(ctx: ActionContext) -> ActionResult:
        reason = str(ctx.params.get("reason") or "").strip()
        if not reason:
            # Reason-less denials demonstrably cause agent retry loops.
            raise HTTPException(status_code=400, detail="A reason is required to deny")
        request_id, row = await _sudo_row(ctx)
        result = await dependencies.apply_vm_upgrade_decision(
            request_id,
            row,
            approve=False,
            upgrade=False,
            reason=reason,
            decided_by=_actor(ctx.user),
        )
        return ActionResult(result=dict(result))

    # --- officer -------------------------------------------------------------
    @register_action("officer_question", "reply")
    async def _officer_reply(ctx: ActionContext) -> ActionResult:
        """The one-off reply the officer lane was waiting on: the existing
        Legate note, reached from the notification instead of the card."""
        message = str(ctx.params.get("message") or "").strip()
        if not message:
            raise HTTPException(status_code=400, detail="message must not be empty")
        if len(message) > OFFICER_NOTE_MAX_CHARS:
            raise HTTPException(
                status_code=400,
                detail=f"message must be at most {OFFICER_NOTE_MAX_CHARS} characters",
            )
        project_id = ctx.params.get("project_id")
        if not project_id:
            raise HTTPException(
                status_code=409, detail="This officer notification has no project"
            )
        user_id = str(ctx.user.get("id"))
        is_admin = bool(ctx.user.get("real_is_admin") or ctx.user.get("is_admin"))
        if not is_admin:
            role = await dependencies.store.get_user_role_in_project(
                str(project_id), user_id
            )
            if role != "owner":
                raise HTTPException(
                    status_code=403,
                    detail="Only the project owner may send the officer orders",
                )
        officer = await dependencies.store.get_officer_thread_for_project(
            str(project_id)
        )
        if not officer:
            raise HTTPException(
                status_code=409,
                detail="The post is vacant — commission an officer before replying",
            )
        text = format_legate_note(ctx.user, message)
        delivered = await dependencies.deliver_officer_note(
            dependencies.store, officer, text
        )
        if delivered == "queued":
            dependencies.kick_officer_event_drain(dependencies.store)
        return ActionResult(
            result={"delivered": delivered, "thread_id": str(officer["id"])},
            resolve=True,
            resolved_by=f"user:{user_id}",
        )

    async def _open_conference(ctx: ActionContext) -> ActionResult:
        """Land the user in the officer's conference. The cockpit's launcher
        route owns create-or-resume, so this action, the project card and
        the sessions list share one path (officer_visibility_streamline.md
        §3.5). Without a project there is no post to confer with — fall back
        to the thread itself."""
        project_id = ctx.params.get("project_id")
        thread_id = ctx.params.get("thread_id")
        if project_id:
            return _navigate(f"/projects/{project_id}/officer/conference")
        return _navigate(f"/sessions/{thread_id}")

    register_action("officer_question", "open_conference")(_open_conference)
    register_action("officer_runtime", "open_conference")(_open_conference)

    # --- source loaders (detail pane payloads) --------------------------------
    @register_source_loader("job")
    async def _load_job(db: Any, job_id: str, user: dict[str, Any]) -> dict | None:
        job = await db.get_job(job_id)
        if not job:
            return None
        freeze_data = job.get("freeze_data")
        if isinstance(freeze_data, str):
            try:
                freeze_data = json.loads(freeze_data)
            except (TypeError, ValueError):
                freeze_data = None
        keep = (
            "id",
            "status",
            "description",
            "config_name",
            "project_id",
            "parent_job_id",
            "created_at",
            "updated_at",
            "completed_at",
            "error_message",
        )
        return notification_jsonable(
            {
                "kind": "job",
                "job": {k: job.get(k) for k in keep},
                "freeze_data": freeze_data,
            }
        )

    @register_source_loader("sudo_request")
    async def _load_sudo(db: Any, request_id: str, user: dict[str, Any]) -> dict | None:
        row = await dependencies.sudo_gate._get_request(request_id)
        if not row:
            return None
        return notification_jsonable({"kind": "sudo_request", "request": dict(row)})

    @register_source_loader("thread")
    async def _load_thread(
        db: Any, thread_id: str, user: dict[str, Any]
    ) -> dict | None:
        thread = await db.get_thread(thread_id)
        if not thread:
            return None
        keep = ("id", "title", "project_id", "config_name", "status", "created_at")
        return notification_jsonable(
            {"kind": "thread", "thread": {k: thread.get(k) for k in keep}}
        )

    # --- source probes (slice 2: `not_resolved` asks the live source) ---------
    # The resolve hooks stamp rows when they run; the probe is what makes an
    # un-enumerated writer (a sweeper, a future endpoint, a direct DB edit)
    # unable to cause a stale mail. "Resolved" means: nobody is waiting on a
    # human any more.

    @register_source_probe("job")
    async def _probe_job(db: Any, job_id: str) -> bool:
        job = await db.get_job(job_id)
        if not job:
            return True  # deleted: nothing left to decide
        return str(job.get("status")) not in ("pending_review", "paused", "reviewing")

    @register_source_probe("sudo_request")
    async def _probe_sudo(db: Any, request_id: str) -> bool:
        row = await dependencies.sudo_gate._get_request(request_id)
        if not row:
            return True
        return str(row["status"]) != "pending"

    @register_source_probe("thread")
    async def _probe_thread(db: Any, thread_id: str) -> bool:
        # An officer question has no state machine to consult; only an
        # explicit reply/resolve settles it.
        return False

    # --- the producers migrated in slice 3 ------------------------------------

    @register_action("agent_message", "reply")
    async def _message_reply(ctx: ActionContext) -> ActionResult:
        message = str(ctx.params.get("message") or "").strip()
        if not message:
            raise HTTPException(status_code=400, detail="message must not be empty")
        job_id = str(ctx.params.get("job_id") or "")
        thread_id = str(ctx.params.get("thread_id") or "")
        try:
            strategy, sequence = await dependencies.route_inbound_reply(
                job_id=job_id,
                thread_id=thread_id,
                message=message,
                resolver_kind="user",
                resolver_id=str(ctx.user.get("id") or ""),
            )
        except ValueError as e:
            raise HTTPException(status_code=404, detail=str(e)) from e
        return ActionResult(
            result={"delivery_strategy": strategy, "sequence": sequence},
            resolve=True,
        )

    register_action("agent_message", "open")(_open_job)

    async def _open_session(ctx: ActionContext) -> ActionResult:
        return _navigate(f"/sessions/{ctx.params.get('thread_id')}")

    register_action("session_wake", "open_session")(_open_session)

    @register_action("loop_event", "open")
    async def _open_loop(ctx: ActionContext) -> ActionResult:
        project_id = ctx.params.get("project_id")
        if project_id:
            return _navigate(f"/projects/{project_id}")
        return _navigate(f"/jobs/{ctx.params.get('job_id')}")

    @register_action("automation_disabled", "open")
    async def _open_automations(ctx: ActionContext) -> ActionResult:
        return _navigate("/automations")

    @register_action("user_registered", "open")
    async def _open_admin_users(ctx: ActionContext) -> ActionResult:
        return _navigate("/admin/users")

    @register_action("chat_executors_unavailable", "open")
    async def _open_admin_capacity(ctx: ActionContext) -> ActionResult:
        return _navigate("/admin/capacity")

    @register_action("ssh_key_added", "open")
    async def _open_ssh_keys(ctx: ActionContext) -> ActionResult:
        # ssh_key_added rows DO carry source_kind="ssh_key" and source_id
        # (see the notification call this action's rows come from, in
        # ``services/ssh_access.py``) — but no register_source_loader/
        # register_source_probe is ever registered for "ssh_key", so nothing
        # can compute source_resolved for it (M-2: this comment used to say
        # "carries no source_kind", which is wrong — the row has one, it's
        # just unprobed). This action is therefore the ONLY way one of these
        # rows ever leaves `pending` (ruling P-12) — the shared `_navigate()`
        # deliberately never sets `resolve`, since its other callers (e.g.
        # automation_disabled, user_registered) resolve through a registered
        # source probe instead, so this builds its own ActionResult.
        # `resolve=True` here is the sanctioned mechanism per
        # ActionResult.resolve's own comment above. The destination is also
        # the right product answer: it's exactly where a user goes to revoke
        # a key they did not add.
        return ActionResult(result={"navigate": "/settings/ssh-keys"}, resolve=True)

    async def _permission_decision(ctx: ActionContext, decision: str) -> ActionResult:
        # act() proved the caller is the row's recipient, i.e. the thread
        # owner the sweeper addressed it to.
        thread_id = str(ctx.params.get("thread_id") or "")
        request_id = str(ctx.params.get("request_id") or "")
        outcome = await dependencies.decide_permission_request(
            thread_id,
            request_id,
            decision,
            decided_by=str(ctx.user.get("id") or "rest_client"),
        )
        await dependencies.notifier.resolve_source(
            "permission_request", request_id, resolved_by=f"user:{ctx.user.get('id')}"
        )
        return ActionResult(result=dict(outcome), resolve=True)

    @register_action("session_permission", "approve")
    async def _permission_approve(ctx: ActionContext) -> ActionResult:
        return await _permission_decision(ctx, "approve")

    @register_action("session_permission", "deny")
    async def _permission_deny(ctx: ActionContext) -> ActionResult:
        return await _permission_decision(ctx, "deny")

    register_action("session_permission", "open_session")(_open_session)

    async def _sudo_decision(ctx: ActionContext, *, approve: bool) -> ActionResult:
        request_id, _row = await _sudo_row(ctx)
        decide = (
            dependencies.sudo_gate.approve_request
            if approve
            else dependencies.sudo_gate.deny_request
        )
        result = await decide(
            request_id,
            reason=str(ctx.params.get("reason") or ""),
            decided_by=_actor(ctx.user),
        )
        if not result:
            raise HTTPException(status_code=404, detail="Sudo request not found")
        if result.get("error"):
            raise HTTPException(status_code=409, detail=str(result["error"]))
        # _finalize_request resolves the row through the sudo_request hook.
        return ActionResult(result=dict(result))

    @register_action("sudo_request", "approve")
    async def _sudo_approve(ctx: ActionContext) -> ActionResult:
        return await _sudo_decision(ctx, approve=True)

    @register_action("sudo_request", "deny")
    async def _sudo_deny(ctx: ActionContext) -> ActionResult:
        return await _sudo_decision(ctx, approve=False)

    @register_action("sudo_request", "open")
    async def _open_sudo_source(ctx: ActionContext) -> ActionResult:
        if ctx.params.get("thread_id"):
            return _navigate(f"/sessions/{ctx.params.get('thread_id')}")
        return _navigate(f"/jobs/{ctx.params.get('job_id')}")

    @register_source_loader("message_thread")
    async def _load_message_thread(
        db: Any, thread_id: str, user: dict[str, Any]
    ) -> dict | None:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, job_id, direction, subject, message, mode, status, "
                "       read_at, created_at "
                "FROM message_log WHERE thread_id = $1 "
                "ORDER BY created_at ASC LIMIT 200",
                thread_id,
            )
        if not rows:
            return None
        return notification_jsonable(
            {
                "kind": "message_thread",
                "thread_id": thread_id,
                "job_id": rows[0]["job_id"],
                "messages": [dict(r) for r in rows],
            }
        )

    @register_source_loader("loop")
    async def _load_loop(db: Any, loop_id: str, user: dict[str, Any]) -> dict | None:
        loop = await db.get_project_loop(loop_id)
        if not loop:
            return None
        keep = ("id", "project_id", "name", "title", "status", "created_at")
        return notification_jsonable(
            {"kind": "loop", "loop": {k: loop.get(k) for k in keep if k in loop}}
        )

    @register_source_loader("automation")
    async def _load_automation(
        db: Any, automation_id: str, user: dict[str, Any]
    ) -> dict | None:
        row = await db.get_automation(automation_id)
        if not row:
            return None
        keep = ("id", "name", "enabled", "disabled_reason", "created_at")
        return notification_jsonable(
            {"kind": "automation", "automation": {k: row.get(k) for k in keep}}
        )

    @register_source_loader("user")
    async def _load_user(db: Any, user_id: str, user: dict[str, Any]) -> dict | None:
        row = await db.get_user(user_id)
        if not row:
            return None
        keep = ("id", "email", "display_name", "is_approved", "created_at")
        return notification_jsonable(
            {"kind": "user", "user": {k: row.get(k) for k in keep}}
        )

    @register_source_loader("permission_request")
    async def _load_permission_request(
        db: Any, request_id: str, user: dict[str, Any]
    ) -> dict | None:
        async with db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, thread_id, tool_name, tool_args, status, requested_at, "
                "       decided_at, decided_by "
                "FROM thread_permission_requests WHERE id = $1",
                request_id,
            )
        if not row:
            return None
        return notification_jsonable(
            {"kind": "permission_request", "request": dict(row)}
        )

    @register_source_probe("message_thread")
    async def _probe_message_thread(db: Any, thread_id: str) -> bool:
        # Answered = the thread's latest message came from the human.
        async with db.acquire() as conn:
            direction = await conn.fetchval(
                "SELECT direction FROM message_log WHERE thread_id = $1 "
                "ORDER BY created_at DESC LIMIT 1",
                thread_id,
            )
        return direction == "inbound"

    @register_source_probe("automation")
    async def _probe_automation(db: Any, automation_id: str) -> bool:
        row = await db.get_automation(automation_id)
        return True if not row else bool(row.get("enabled"))

    @register_source_probe("user")
    async def _probe_user(db: Any, user_id: str) -> bool:
        row = await db.get_user(user_id)
        return True if not row else bool(row.get("is_approved"))

    @register_source_probe("permission_request")
    async def _probe_permission_request(db: Any, request_id: str) -> bool:
        async with db.acquire() as conn:
            status = await conn.fetchval(
                "SELECT status FROM thread_permission_requests WHERE id = $1",
                request_id,
            )
        return status is None or str(status) != "pending"


__all__ = ["NotificationActionDependencies", "register_notification_actions"]
