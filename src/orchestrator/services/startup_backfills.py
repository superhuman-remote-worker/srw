"""Idempotent data backfills the orchestrator runs once at every startup.

R1.B11 moved these out of the application lifespan unchanged. They live at
startup rather than in ``init.py`` because ``init.py`` is not reliably run at
deploy time. Each step logs and continues on failure: a backfill problem
never stops the application from starting.
"""

from __future__ import annotations

import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


async def run_startup_backfills(store: Any) -> None:
    """Encrypt legacy datasource credentials, strip stored thread secrets and,
    on a dev cluster with ``MCP_DEV_TOKEN``, seed the admin MCP token."""

    # Encrypt any legacy plaintext datasource credentials. Idempotent — once
    # all rows are v1 ciphertexts this is a fast no-op. Runs at startup
    # (not init.py) because init.py is not reliably invoked at deploy time, and
    # this is data-integrity critical for the encryption-at-rest guarantee.
    try:
        _bf = await store.backfill_encrypt_datasource_credentials()
        if _bf["encrypted"] > 0:
            logger.info(
                "Encrypted %d legacy plaintext datasource credentials "
                "(%d skipped, %d errors)",
                _bf["encrypted"],
                _bf["skipped"],
                _bf["errors"],
            )
        elif _bf["errors"] > 0:
            logger.warning(
                "Datasource credentials backfill: %d errors (%d skipped)",
                _bf["errors"],
                _bf["skipped"],
            )
    except Exception as _e:
        logger.error("Datasource credentials backfill failed: %s", _e)

    # Strip any legacy plaintext secrets from threads.metadata.config_override.
    # Persistent-session credentials are injected in-flight at attach/resume and
    # must never be stored (see redact_config_override). Idempotent — once all
    # rows are secret-free this is a fast no-op. Runs at startup (not init.py)
    # for the same reason as the datasource backfill above.
    try:
        _sf = await store.backfill_strip_thread_config_secrets()
        if _sf["stripped"] > 0:
            logger.info(
                "Stripped secrets from %d thread config_override(s) "
                "(%d skipped, %d errors)",
                _sf["stripped"],
                _sf["skipped"],
                _sf["errors"],
            )
        elif _sf["errors"] > 0:
            logger.warning(
                "Thread config_override strip backfill: %d errors (%d skipped)",
                _sf["errors"],
                _sf["skipped"],
            )
    except Exception as _e:
        logger.error("Thread config_override strip backfill failed: %s", _e)

    # Dev-only: seed a fixed admin MCP token from MCP_DEV_TOKEN so a committed
    # .mcp.json works out of the box against a local cluster. Only fires when
    # MCP_DEV_TOKEN is set (unset in prod → no-op, no surprise auto-generated
    # token). Runs at startup (not init.py) for the same reason as the
    # backfill above — init.py is not reliably invoked at deploy time. Idempotent
    # and no-ops on a fresh DB with no admin yet; the JIT-provision path in
    # security/auth.py re-fires it the moment the admin user is first created.
    if os.environ.get("MCP_DEV_TOKEN", "").strip():
        try:
            from orchestrator.init import _seed_admin_mcp_token

            await _seed_admin_mcp_token(store)
        except Exception as _e:
            logger.warning("MCP dev token seed at startup failed (non-fatal): %s", _e)
