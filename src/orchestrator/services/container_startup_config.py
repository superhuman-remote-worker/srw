"""Default-off activation of new container startup receipts."""

from __future__ import annotations

import os


def startup_stage_activation_enabled() -> bool:
    """Permit new v1 adoption; existing v1 receipts remain active regardless."""

    return os.environ.get(
        "CONTAINER_STARTUP_STAGE_AUTHORITY_ENABLED", ""
    ).strip().lower() in {"1", "true", "yes", "on"}
