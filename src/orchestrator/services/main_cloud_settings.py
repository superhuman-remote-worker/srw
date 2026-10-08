"""Main cloud administration (admin-only): the read-only page and two
operator operations.

Extracted from ``orchestrator.main`` (R1.B04 lane M). The main cloud is
configured by Helm only (main_cloud_as_connectors.md, slice 2): the admin
connection form, its ``GET/PUT/DELETE/test/reload`` API and the DB overlay
are gone, and :func:`get_main_cloud_page` only reports. What stays, as
operator operations until their work is done, are the one-shot
instance-authority backfill and the thread-mount transport repair.

Two properties are load-bearing:

* **What leaves the process.** No read path in this module returns, logs, or
  echoes a secret value; the page shows the routing snapshot's public URL and
  never the upstream text of a failed health probe.
* **Installation authority.** ``cloud_router.active`` raising when no active
  backend instance is bound is correct and is never softened here; the
  backfill refuses to guess an installation and re-attests against the live
  installation before trusting a proof.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from fastapi import HTTPException

from orchestrator.services import thread_mount_rows
from orchestrator.services.cloud import provider_capabilities, provider_matrix
from orchestrator.services.cloud.instance_registry import (
    helm_configuration_status,
    reload_active_main_cloud_instance,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class MainCloudSettingsDependencies:
    """Collaborators for one main-cloud operation, per invocation.

    ``store`` is main's ``postgres_db`` and ``cloud_router`` its
    ``main_cloud_router``; both are rebound during ``lifespan``.

    ``thread_mount_dependencies`` is main's ``_thread_mount_dependencies``
    factory: the transport repair rebuilds mount rows through the same
    builder thread create uses, so it needs that lane's collaborators, and it
    needs them resolved per call for the same rebinding reason as the two
    above. ``replace_installation`` is the deployment's
    ``cloud.replaceInstallation``, which the page reports.
    """

    store: Any
    cloud_router: Any
    thread_mount_dependencies: Callable[[], Any]
    replace_installation: str = ""


#: How long the page waits for the active provider's health probe.
_HEALTH_TIMEOUT_SECONDS = 5.0
_HEALTH_DETAIL = re.compile(r"^(status=\d{3}|not initialized)$")


async def _health(backend: Any) -> dict[str, Any]:
    """The active provider's health, without echoing an upstream error text."""
    try:
        status = await asyncio.wait_for(
            backend.health_check(), timeout=_HEALTH_TIMEOUT_SECONDS
        )
    except Exception:
        return {"ok": False, "latency_ms": None, "detail": "unreachable"}
    detail = str(status.detail or "")
    return {
        "ok": bool(status.ok),
        "latency_ms": round(float(status.latency_ms), 1),
        "detail": detail if _HEALTH_DETAIL.fullmatch(detail) else "unreachable",
    }


async def get_main_cloud_page(
    *, dependencies: MainCloudSettingsDependencies
) -> dict[str, Any]:
    """The read-only admin Main cloud page: provider, installation, health,
    where the configuration comes from, and the provider support matrix.

    Nothing here can change the configuration (Helm owns it). The response
    holds no secret: the routing snapshot's public URL is the only address.
    """
    active = dependencies.cloud_router.active
    try:
        active_row = await dependencies.store.get_active_main_cloud_backend_instance()
    except Exception:
        active_row = None
    authority = active_row.get("authority") if isinstance(active_row, dict) else None
    backend_id = authority.backend_id if authority is not None else active.backend_id
    declared = provider_capabilities(backend_id)
    activated_at = (
        active_row.get("activated_at") if isinstance(active_row, dict) else None
    )
    helm = helm_configuration_status(authority)
    return {
        "provider": {
            "backend_id": backend_id,
            "title": declared.title if declared is not None else backend_id,
            "public_url": (
                authority.routing.get("public_url") if authority is not None else None
            ),
            "backend_instance_id": (
                authority.backend_instance_id if authority is not None else None
            ),
            "activation_revision": (
                int(active_row.get("activation_revision") or 0)
                if isinstance(active_row, dict)
                else 0
            ),
            "activated_at": (
                activated_at.isoformat() if activated_at is not None else None
            ),
            "initialized": bool(active.is_initialized),
        },
        "health": await _health(active),
        "configuration": {
            "source": "helm",
            "helm": {
                "state": helm.state,
                "backend_id": helm.backend_id,
                "detail": helm.detail,
            },
            "replace_installation": dependencies.replace_installation or None,
        },
        "matrix": provider_matrix(active=backend_id),
    }


async def backfill_main_cloud_instance_authority(
    *, apply: bool, dependencies: MainCloudSettingsDependencies
) -> dict[str, Any]:
    """Stamp pre-0186 rows with the installation they actually live on.

    Admin-only. **Dry run by default** — pass ``?apply=true`` to write.

    0186 added ``main_cloud_backend_instance_id`` nullable with no backfill, so
    rows stamped before it name a provider but no installation. The router
    fails closed on that shape, which is correct for effects: those projects
    cannot grant membership, share, or have their folder deleted, because we
    cannot say *which* installation to act on.

    This is deliberately NOT a SQL migration. The design
    (knowledge-base/knowledge/features/protected_session_lifecycle_and_mount_readiness.md)
    requires "an operator-attested single-installation mapping plus a verified
    remote proof", and a migration running inside psql at startup can obtain
    neither. Stamping without that proof would launder a guess into recorded
    authority — silently, permanently, and unquestioned by everything
    downstream. A loud refusal is recoverable; a wrong instance UUID is not.

    So the safety argument here is:

    1. the active instance is **re-attested against the live installation**
       before anything is read, so its proof reflects reality now;
    2. every provider named by an unstamped row must resolve to exactly one
       *installation* in the registry. Two registry rows for one provider are
       not automatically ambiguous — they are commonly two routing snapshots
       of the same installation, which share an
       ``installation_proof_sha256``. Two distinct **proofs** are the genuine
       ambiguity, and abort;
    3. a provider we cannot re-attest right now (not the active backend) is
       never stamped, because nothing proves where its rows live.
    """
    postgres_db = dependencies.store

    survey = await postgres_db.survey_unstamped_main_cloud_rows()
    unstamped_projects = survey["projects"]
    unstamped_threads = survey["threads"]
    if not unstamped_projects and not unstamped_threads:
        return {
            "status": "noop",
            "applied": False,
            "detail": "No rows carry a provider without its backend instance.",
            "projects": 0,
            "threads": 0,
        }

    providers = sorted(
        {str(r["main_cloud_backend"]) for r in unstamped_projects}
        | {str(r["main_cloud_backend"]) for r in unstamped_threads}
    )

    # (1) Re-attest the active installation before trusting its proof.
    try:
        reattested = await reload_active_main_cloud_instance(
            postgres_db,
            dependencies.cloud_router,
            force_rebuild=True,
        )
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"cannot verify the live installation proof: {e}",
        ) from e
    if reattested is not True:
        raise HTTPException(
            status_code=409,
            detail="active instance changed during re-attestation; retry",
        )

    active = await postgres_db.get_active_main_cloud_backend_instance()
    if not active:
        raise HTTPException(
            status_code=409,
            detail="no active main-cloud instance to attest against",
        )
    active_authority = active["authority"]

    registry = await postgres_db.list_main_cloud_backend_instances()

    # (2)+(3) Resolve one installation per provider, or refuse.
    plan: list[dict[str, Any]] = []
    for provider in providers:
        proofs = {
            a.installation_proof_sha256 for a in registry if a.backend_id == provider
        }
        if len(proofs) != 1:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"provider {provider!r} has {len(proofs)} distinct "
                    "installations in the registry; historical rows cannot be "
                    "attributed to one of them automatically. Resolve this "
                    "mapping by hand."
                ),
            )
        if provider != active_authority.backend_id:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"provider {provider!r} is not the active backend "
                    f"({active_authority.backend_id!r}), so its installation "
                    "cannot be re-attested right now. Activate it first."
                ),
            )
        if proofs != {active_authority.installation_proof_sha256}:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"provider {provider!r} registry proof does not match the "
                    "installation just attested; refusing to attribute "
                    "historical rows to it."
                ),
            )
        plan.append(
            {
                "backend_id": provider,
                "backend_instance_id": active_authority.backend_instance_id,
                "installation_proof_sha256": (
                    active_authority.installation_proof_sha256
                ),
                "projects": [
                    {
                        "id": str(r["id"]),
                        "name": r.get("name"),
                        "status": r.get("status"),
                    }
                    for r in unstamped_projects
                    if str(r["main_cloud_backend"]) == provider
                ],
                "threads": [
                    str(r["id"])
                    for r in unstamped_threads
                    if str(r["main_cloud_backend"]) == provider
                ],
            }
        )

    if not apply:
        return {
            "status": "dry_run",
            "applied": False,
            "detail": "Re-run with ?apply=true to write these stamps.",
            "plan": plan,
            "projects": len(unstamped_projects),
            "threads": len(unstamped_threads),
        }

    stamped = {"projects": 0, "threads": 0}
    for entry in plan:
        counts = await postgres_db.stamp_main_cloud_instance_authority(
            backend_id=entry["backend_id"],
            backend_instance_id=entry["backend_instance_id"],
        )
        stamped["projects"] += counts["projects"]
        stamped["threads"] += counts["threads"]
        logger.warning(
            "main-cloud instance authority backfill: stamped %d project(s) and "
            "%d thread(s) for provider %r with instance %s (proof %s)",
            counts["projects"],
            counts["threads"],
            entry["backend_id"],
            entry["backend_instance_id"],
            entry["installation_proof_sha256"],
        )

    return {
        "status": "ok",
        "applied": True,
        "plan": plan,
        "projects": stamped["projects"],
        "threads": stamped["threads"],
    }


async def repair_thread_mount_transport(
    *, apply: bool, dependencies: MainCloudSettingsDependencies
) -> dict[str, Any]:
    """Re-derive the transport of partial project mount rows.

    Admin-only. **Dry run by default** — pass ``?apply=true`` to write.

    ``thread_mounts`` rows minted while their project was still unstamped
    (pre-0186) carry a provider name but no installation and no WebDAV URL,
    and the instance-authority backfill leaves them exactly so: mount rows
    are only re-derived for a thread that has none. At delivery one such row
    is fatal to the whole set — ``_build_agent_cloud_mount`` mounts every row
    or falls back to the legacy session folder — so a stamped project still
    yields a session that writes to ``sessions/<id>``.

    Every row is rebuilt through the builder that creates rows
    (``thread_mount_rows.build_project_mount_row``) against the project's
    *stamped* installation, and written only when every transport column
    resolved. Nothing here guesses: an unstamped project is skipped with a
    pointer at the backfill, an installation this replica cannot resolve is
    skipped, and a rebuilt row that is itself partial is never written — the
    repair must not mint the shape it exists to remove. Rows are updated in
    place, so mount ids and the collision-suffixed ``target_path`` decided at
    create time survive. Re-running is a no-op.
    """
    postgres_db = dependencies.store

    partial = await postgres_db.survey_partial_thread_mounts()
    if not partial:
        return {
            "status": "noop",
            "applied": False,
            "detail": "No project mount row lacks its transport.",
            "rows": 0,
            "repairable": 0,
            "repaired": 0,
            "skipped": 0,
        }

    mount_dependencies = dependencies.thread_mount_dependencies()
    plan: list[dict[str, Any]] = []
    writes: dict[str, dict[str, Any]] = {}
    projects: dict[str, Any] = {}
    for row in partial:
        mount_id = str(row["id"])
        entry: dict[str, Any] = {
            "mount_id": mount_id,
            "thread_id": str(row["thread_id"]),
            "thread_status": row.get("thread_status"),
            "mount_kind": row.get("mount_kind"),
            "target_path": row.get("target_path"),
            "project_id": str(row["source_ref"]) if row.get("source_ref") else None,
        }
        project_id = entry["project_id"]
        if not project_id:
            plan.append({**entry, "action": "skip", "reason": "no_project"})
            continue
        if project_id not in projects:
            projects[project_id] = await postgres_db.get_project(project_id)
        project = projects[project_id]
        if not project:
            plan.append({**entry, "action": "skip", "reason": "project_missing"})
            continue
        if project.get("main_cloud_backend") and not project.get(
            "main_cloud_backend_instance_id"
        ):
            plan.append(
                {
                    **entry,
                    "action": "skip",
                    "reason": "project_unstamped",
                    "detail": "run backfill-instance-authority first",
                }
            )
            continue
        try:
            rebuilt = await thread_mount_rows.build_project_mount_row(
                project_id, project, dependencies=mount_dependencies
            )
        except Exception as e:
            logger.warning(
                "thread mount transport repair: rebuilding row %s for project "
                "%s failed: %s",
                mount_id,
                project_id,
                e,
            )
            rebuilt = None
        if rebuilt is None:
            plan.append({**entry, "action": "skip", "reason": "transport_unresolvable"})
            continue
        if rebuilt.get("mount_kind") != row.get("mount_kind"):
            plan.append(
                {
                    **entry,
                    "action": "skip",
                    "reason": "mount_kind_changed",
                    "detail": f"project now yields {rebuilt.get('mount_kind')!r}",
                }
            )
            continue
        writes[mount_id] = {
            "backend_id": str(rebuilt["backend_id"]),
            "backend_instance_id": str(rebuilt["backend_instance_id"]),
            "cloud_handle": rebuilt.get("cloud_handle"),
            "webdav_url": str(rebuilt["webdav_url"]),
            "target_user_sub": rebuilt.get("target_user_sub"),
        }
        plan.append(
            {
                **entry,
                "action": "repair",
                "backend_id": writes[mount_id]["backend_id"],
                "backend_instance_id": writes[mount_id]["backend_instance_id"],
                "webdav_url": writes[mount_id]["webdav_url"],
                "target_user_sub": bool(writes[mount_id]["target_user_sub"]),
            }
        )

    skipped = len(plan) - len(writes)
    if not apply:
        return {
            "status": "dry_run",
            "applied": False,
            "detail": "Re-run with ?apply=true to write these transports.",
            "plan": plan,
            "rows": len(plan),
            "repairable": len(writes),
            "repaired": 0,
            "skipped": skipped,
        }

    repaired = 0
    for entry in plan:
        write = writes.get(entry["mount_id"])
        if write is None:
            continue
        written = await postgres_db.repair_thread_mount_transport(
            entry["mount_id"], **write
        )
        entry["written"] = bool(written)
        repaired += int(bool(written))
    logger.warning(
        "thread mount transport repair: rewrote %d of %d partial row(s); %d skipped",
        repaired,
        len(plan),
        skipped,
    )
    return {
        "status": "ok",
        "applied": True,
        "plan": plan,
        "rows": len(plan),
        "repairable": len(writes),
        "repaired": repaired,
        "skipped": skipped,
    }
