"""60-second tick loop that materializes jobs from due cron automations.

Mounted as a background task in the orchestrator's lifespan alongside
the auto-assign dispatcher. Each tick drains any cron automations whose
``next_run_at`` has come due, advances their schedule, and (optionally)
nudges the auto-assign dispatcher so the freshly-created jobs are picked
up without waiting for its own 30s tick.

The claim → fire → advance flow runs inside a single Postgres transaction
per automation: ``fetch_next_due_cron_automation`` opens the row under
``FOR UPDATE SKIP LOCKED`` and the row stays locked through
``advance_automation_after_fire`` (or ``skip_automation_fire`` for a
catchup-window skip). This makes the dispatcher safe to run on multiple
orchestrator replicas — concurrent ticks see disjoint subsets of due
rows, never the same one.

DST handling: ``next_run_at`` is stored in UTC; the cron expression is
evaluated in the row's IANA timezone via ``zoneinfo``. croniter operates
on a timezone-aware base time and returns the next fire in the same
timezone, which is then converted back to UTC for storage. "Every day
at 09:00 Europe/Berlin" stays at 09:00 local across the spring-forward
and fall-back transitions.

Spec: knowledge-base/knowledge/features/automations_v0.md §Cron Triggers.
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from croniter import croniter

from orchestrator.services.automations import (
    AutomationWorkspaceRefused,
    create_job_from_automation,
)
from orchestrator.services.notification_service import notification_service

logger = logging.getLogger(__name__)


# Cadence is env-tunable mostly so tests can drive ticks faster than 60s.
# In prod the default is fine — the cron resolution is 1 minute, so a
# 60s tick can never miss a fire (and the catchup window covers the
# longer outages anyway).
TICK_SECONDS = int(os.getenv("AUTOMATIONS_TICK_SECONDS", "60"))

# Per-tick sanity cap. Each due automation does ~3 DB round-trips + a
# job creation. Draining hundreds in one tick would starve the rest of
# the orchestrator; if we ever legitimately hit this cap we should
# investigate before raising it.
MAX_FIRES_PER_TICK = int(os.getenv("AUTOMATIONS_MAX_FIRES_PER_TICK", "50"))


# Callback type: invoked once per tick after at least one job was created.
# Today this is wired to ``_trigger_dispatch`` so the auto-assign dispatcher
# wakes up immediately instead of waiting its own 30s.
OnJobCreated = Callable[[], Any]
# (job_row, db) -> awaitable. The application binds the Gitea/cloud
# provisioning adapter; a caller that supplies none (every existing test) fires
# automations without a dedicated repo, exactly as an outage already did.
ProvisionJobRepo = Callable[[dict[str, Any], Any], Any]


async def cron_dispatcher_loop(
    db: Any,
    shutdown_event: asyncio.Event,
    *,
    on_job_created: OnJobCreated | None = None,
    provision_repo: ProvisionJobRepo | None = None,
) -> None:
    """Run the cron tick loop until ``shutdown_event`` is set.

    Caller is responsible for passing the long-lived db handle, the optional
    ``on_job_created`` poke and the ``provision_repo`` adapter — all three are
    kept out of module state so tests can inject mocks without monkey-patching.

    R1.B07 closed this module's late ``from orchestrator.main import
    gitea_client, main_cloud_router``: this loop outlives every request, so it
    carries the provisioning adapter explicitly instead of reaching for the
    application module when a job happens to be created.
    """
    logger.info(
        "Cron automation dispatcher started (tick=%ds, max_per_tick=%d)",
        TICK_SECONDS,
        MAX_FIRES_PER_TICK,
    )
    while not shutdown_event.is_set():
        try:
            fired = await _tick(db, provision_repo=provision_repo)
        except Exception:
            logger.exception("Cron dispatcher tick raised; will retry next tick")
            fired = 0

        if fired and on_job_created is not None:
            try:
                on_job_created()
            except Exception:
                logger.exception(
                    "on_job_created callback raised; jobs will still dispatch on the auto-assign tick"
                )

        try:
            await asyncio.wait_for(shutdown_event.wait(), timeout=TICK_SECONDS)
            break
        except asyncio.TimeoutError:
            pass

    logger.info("Cron automation dispatcher stopped")


async def _tick(db: Any, *, provision_repo: ProvisionJobRepo | None = None) -> int:
    """Drain due cron automations. Returns the number processed."""
    drained = 0
    while drained < MAX_FIRES_PER_TICK:
        processed = await _process_one_due_automation(db, provision_repo=provision_repo)
        if not processed:
            break
        drained += 1
    if drained == MAX_FIRES_PER_TICK:
        logger.warning(
            "Cron tick hit MAX_FIRES_PER_TICK=%d — investigate if this recurs",
            MAX_FIRES_PER_TICK,
        )
    return drained


async def _process_one_due_automation(
    db: Any, *, provision_repo: ProvisionJobRepo | None = None
) -> bool:
    """Atomically claim, fire, and advance one due automation.

    Returns True if a row was processed (fired, skipped, or disabled),
    False when nothing is due. All DB writes for this automation share
    one transaction so SKIP LOCKED can do its job — concurrent dispatcher
    replicas see disjoint sets of due rows.

    Auto-disable notifications are deferred until after the transaction
    commits so we don't notify the owner about a disable that got rolled
    back by an unrelated failure.
    """
    pending_disable: dict[str, str] | None = None
    created_job: dict[str, Any] | None = None

    async with db.acquire() as conn:
        async with conn.transaction():
            row = await db.fetch_next_due_cron_automation(conn)
            if row is None:
                return False

            cron_expr: str = row["cron_expr"]
            tz_name: str = row["timezone"] or "UTC"
            scheduled_for: datetime = row["next_run_at"]
            catchup_window = timedelta(seconds=row["catchup_window_seconds"])
            now_utc = datetime.now(timezone.utc)
            automation_id = str(row["id"])

            # Compute the next fire ONCE; both the skip and the fire paths
            # need it to advance ``next_run_at``.
            try:
                next_run = compute_next_run_after(
                    cron_expr,
                    scheduled_for,
                    tz_name,
                )
            except Exception as exc:
                # A row with a bad cron expression should not block the
                # tick loop forever. Disable it so the owner can fix it.
                logger.exception(
                    "Automation %s has an invalid cron expression %r — disabling",
                    automation_id,
                    cron_expr,
                )
                reason = f"invalid cron expression: {exc}"
                await db.auto_disable_automation(
                    conn,
                    automation_id,
                    reason=reason,
                )
                pending_disable = _build_disable_payload(row, reason)
            else:
                # Catchup-window skip: if the scheduled time is older than the
                # grace window the orchestrator was down too long to honor it.
                # Advance to the next future tick without firing.
                lag = now_utc - scheduled_for
                if lag > catchup_window:
                    logger.warning(
                        "Automation %s missed fire (scheduled %s, lag %.1fs > catchup %.1fs) — skipping",
                        automation_id,
                        scheduled_for.isoformat(),
                        lag.total_seconds(),
                        catchup_window.total_seconds(),
                    )
                    await db.skip_automation_fire(
                        conn,
                        automation_id,
                        next_run_at=next_run,
                    )
                else:
                    # max_fires_per_day guard. fires_today_* on the row is a
                    # rolling counter — if today's date matches and we're at
                    # the cap, disable the automation. The owner is
                    # notified after commit (see _emit_auto_disable_notification).
                    fires_today_date = row.get("fires_today_date")
                    fires_today_count = int(row.get("fires_today_count") or 0)
                    max_fires = int(row.get("max_fires_per_day") or 100)
                    if (
                        fires_today_date == now_utc.date()
                        and fires_today_count >= max_fires
                    ):
                        reason = f"max_fires_per_day={max_fires} reached"
                        await db.auto_disable_automation(
                            conn,
                            automation_id,
                            reason=reason,
                        )
                        pending_disable = _build_disable_payload(row, reason)
                    else:
                        # Fire the job. If create_job_from_automation raises,
                        # the transaction rolls back and the row stays
                        # unfired; the next tick will re-claim it.
                        # At-least-once semantics.
                        try:
                            job = await create_job_from_automation(
                                db, row, trigger_kind="cron"
                            )
                        except AutomationWorkspaceRefused as exc:
                            # The owner's workspace defaults refused this
                            # fire. That is configuration, not an outage:
                            # record it where the owner reads the
                            # automation's last status and advance, so this
                            # row can't hold up every later automation.
                            logger.warning(
                                "Automation %s fire refused: %s",
                                automation_id,
                                exc.detail,
                            )
                            await db.refuse_automation_fire(
                                conn,
                                automation_id,
                                next_run_at=next_run,
                                reason=str(exc.detail),
                            )
                        else:
                            if job is None:
                                # Archived project: skip this fire without
                                # touching the automation's enabled state (it
                                # logs its own reason). Advance the schedule the
                                # same way the catch-up window does, so the row
                                # is not re-claimed on every tick forever.
                                await db.skip_automation_fire(
                                    conn,
                                    automation_id,
                                    next_run_at=next_run,
                                )
                            else:
                                await db.advance_automation_after_fire(
                                    conn,
                                    automation_id,
                                    next_run_at=next_run,
                                    scheduled_for=scheduled_for,
                                    job_id=str(job["id"]),
                                )
                                # Capture for post-commit Gitea provisioning — it
                                # must run OUTSIDE this transaction (see below).
                                created_job = job

    # Post-commit side effects — these MUST run outside the transaction
    # above. provision_job_repo does external Gitea HTTP, opens its own
    # postgres_db.acquire(), and schedules a background task; holding the
    # automation's transaction across that would pin the pooled connection
    # and the nested acquire() would contend. Best-effort: a Gitea outage
    # logs and leaves the job repo-less rather than undoing the committed
    # fire (the job still runs, just without a dedicated repo). This is the
    # parity fix so cron-spawned jobs get a workspace repo like manual jobs.
    if created_job is not None and provision_repo is not None:
        try:
            await provision_repo(created_job, db)
        except Exception:
            logger.exception(
                "cron: repo provisioning failed for job %s "
                "(job will run without a dedicated repo)",
                created_job.get("id"),
            )

    # Notification failures are logged but never re-raised — the auto-disable
    # is the user-visible safety signal; the notification is the prompt.
    if pending_disable is not None:
        await _emit_auto_disable_notification(pending_disable)

    return True


def _build_disable_payload(row: dict[str, Any], reason: str) -> dict[str, str]:
    """Capture the fields needed for the auto-disable notification.

    Built inside the transaction so the values are coherent with the
    state being committed; the actual notification fires after commit.
    """
    return {
        "user_id": str(row["owner_id"]),
        "automation_id": str(row["id"]),
        "automation_name": row.get("name") or "",
        "reason": reason,
    }


async def _emit_auto_disable_notification(payload: dict[str, str]) -> None:
    """Best-effort owner notification when an automation is auto-disabled.

    Wraps :py:meth:`NotificationService.record_automation_disabled`
    so a misconfigured notification path can't crash the dispatcher.
    """
    if not notification_service.is_available:
        return
    try:
        await notification_service.record_automation_disabled(
            user_id=payload["user_id"],
            automation_id=payload["automation_id"],
            automation_name=payload["automation_name"],
            reason=payload["reason"],
        )
    except Exception:
        logger.exception(
            "Auto-disable notification failed for automation %s "
            "(disable still applied)",
            payload["automation_id"],
        )


def compute_next_run_after(
    cron_expr: str,
    after: datetime,
    tz_name: str,
) -> datetime:
    """Return the next cron fire time strictly after ``after``, in UTC.

    The IANA ``tz_name`` is required because cron expressions are wall-
    clock by convention: "0 9 * * *" means 09:00 local time, which is
    different UTC instants either side of a DST transition. We convert
    ``after`` into the target timezone, advance via croniter, then
    convert the result back to UTC for storage.
    """
    tz = ZoneInfo(tz_name)
    base_local = after.astimezone(tz)
    next_local = croniter(cron_expr, base_local).get_next(datetime)
    # croniter >= 2.0 propagates tzinfo through get_next(datetime), but
    # older builds return a naive datetime — re-apply the tz defensively.
    if next_local.tzinfo is None:
        next_local = next_local.replace(tzinfo=tz)
    return next_local.astimezone(timezone.utc)


def compute_initial_next_run(
    cron_expr: str,
    tz_name: str,
    *,
    now: datetime | None = None,
) -> datetime:
    """Seed value for ``automations.next_run_at`` on create / cron-edit.

    ``now`` is parameterized for testability; production callers leave it
    as None and get ``datetime.now(timezone.utc)``.
    """
    base = now if now is not None else datetime.now(timezone.utc)
    return compute_next_run_after(cron_expr, base, tz_name)


def validate_cron_expr(expr: str) -> None:
    """Raise ValueError on a malformed cron expression.

    Called by the create / update API handlers so a bad expression
    surfaces as a 400 instead of crashing the dispatcher tick later.
    """
    if not croniter.is_valid(expr):
        raise ValueError(f"Invalid cron expression: {expr!r}")


def validate_timezone(tz_name: str) -> None:
    """Raise ValueError if ``tz_name`` is not a recognized IANA name.

    Same boundary check as validate_cron_expr — surface bad input at
    the API layer rather than letting it land in the DB and explode at
    fire time.
    """
    try:
        ZoneInfo(tz_name)
    except Exception as exc:
        raise ValueError(f"Unknown timezone: {tz_name!r}") from exc
