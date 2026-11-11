"""Deployment-owned virtual-workspace transport configuration.

The persisted thread row contains only the selected backend. Credentials are
reconstructed from orchestrator environment/Secret wiring at request time and
are never copied into thread or Canvas JSONB state.

:func:`object_store_startup_warning` and :func:`check_object_store_config` were
moved here verbatim from ``orchestrator.main`` (R1.B05 lane P) because they read
exactly the env this module owns plus ``S3_ENDPOINT``: one place answers "does
this deployment have a durable object store", so the startup banner and the
virtual tier can never disagree. ``check_object_store_config`` keeps its two
outcomes — return the message (warn, the default) or raise ``RuntimeError``
(``OBJECT_STORE_REQUIRED`` set, fail closed); collapsing them would either
crash-loop every warn-mode deployment or silently drop a fail-closed operator's
guarantee.
"""

from __future__ import annotations

import os
from typing import Any, Mapping, Optional


def virtual_workspace_rclone_spec() -> dict[str, Any] | None:
    """Return the configured ``{type, config, root}`` rclone descriptor."""

    remote_type = os.environ.get("VIRTUAL_WORKSPACE_RCLONE_TYPE", "").strip()
    if not remote_type:
        return None

    root = os.environ.get("VIRTUAL_WORKSPACE_RCLONE_ROOT", "").strip()
    config: dict[str, Any] = {}
    if remote_type == "s3":
        config = {
            "provider": os.environ.get("VIRTUAL_WORKSPACE_S3_PROVIDER", "").strip()
            or "Minio",
            "access_key_id": os.environ.get("VIRTUAL_WORKSPACE_S3_ACCESS_KEY_ID", ""),
            "secret_access_key": os.environ.get(
                "VIRTUAL_WORKSPACE_S3_SECRET_ACCESS_KEY", ""
            ),
            "endpoint": os.environ.get("VIRTUAL_WORKSPACE_S3_ENDPOINT", "").strip(),
            "region": os.environ.get("VIRTUAL_WORKSPACE_S3_REGION", "").strip()
            or "us-east-1",
            "no_check_bucket": "true",
        }
    return {"type": remote_type, "config": config, "root": root}


def object_store_startup_warning(
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """One consolidated warning when EITHER object-store seam is unconfigured.

    Reads the same env the features read, so it reflects the real resolved
    config whether the store is external or the chart-bundled Garage:
    ``S3_ENDPOINT`` (snapshots / suspend-resume / IDE persistence / VM S3
    extract) and ``VIRTUAL_WORKSPACE_RCLONE_TYPE`` (the virtual/default session
    tier). Emits one loud line naming whichever seam is degraded: the two
    almost always point at the SAME store, so a half-configured deployment is
    usually a mistake, and — with instant-landing making ``virtual`` the
    default session backend — a snapshots-only config silently breaks the
    default UX. Returns None only when BOTH seams have a durable store. Replaces
    the scattered, late per-feature failures (``LiteWorkspaceConfigError`` at
    dispatch, silent snapshot no-ops) with one signal at startup. See
    knowledge-history/done/s3_object_store_bundled_fallback.md item 3.
    """
    env = os.environ if env is None else env
    s3_endpoint = (env.get("S3_ENDPOINT") or "").strip()
    rclone_type = (env.get("VIRTUAL_WORKSPACE_RCLONE_TYPE") or "").strip()

    bullets: list[str] = []
    if not s3_endpoint:
        bullets.append(
            "workspace snapshots + suspend/resume, IDE session persistence, and "
            "VM-lifecycle S3 extract are disabled (S3_ENDPOINT unset)"
        )
    if rclone_type != "s3":
        if rclone_type == "memory":
            bullets.append(
                "virtual/instant sessions run but are NON-DURABLE (in-process "
                "'memory' store — files vanish on pod restart)"
            )
        else:
            bullets.append(
                "virtual/instant sessions FAIL at dispatch "
                "(LiteWorkspaceConfigError) — if your deployment uses them"
            )

    if not bullets:
        return None

    # Design: knowledge-history/done/s3_object_store_bundled_fallback.md.
    return (
        "Object store not fully configured — "
        + "; ".join(bullets)
        + ". Fix: point at an external S3 (S3_ENDPOINT / VIRTUAL_WORKSPACE_S3_*) "
        "or enable the chart-bundled store (garage.enabled=true)."
    )


def check_object_store_config(
    env: Optional[Mapping[str, str]] = None,
) -> Optional[str]:
    """Warn (return the message) or fail-closed (raise) on an incomplete
    object-store config.

    Default (``OBJECT_STORE_REQUIRED`` unset/falsey) is warn-only, matching the
    platform's degrade-open convention. When ``OBJECT_STORE_REQUIRED`` is set
    (``true``/``1``/``yes``) and either seam lacks a durable store, raises
    ``RuntimeError`` so the orchestrator refuses to start — crash-looping until
    the operator fixes it — for deployments that prefer fail-closed over silent
    degradation. Returns None when both seams have a durable store.
    """
    env = os.environ if env is None else env
    msg = object_store_startup_warning(env)
    if msg and (env.get("OBJECT_STORE_REQUIRED") or "").lower().strip() in (
        "true",
        "1",
        "yes",
    ):
        raise RuntimeError(
            msg + " OBJECT_STORE_REQUIRED is set, so the orchestrator refuses "
            "to start with an incomplete object-store config."
        )
    return msg


__all__ = [
    "check_object_store_config",
    "object_store_startup_warning",
    "virtual_workspace_rclone_spec",
]
