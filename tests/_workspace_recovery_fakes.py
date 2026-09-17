"""Explicit recovery collaborators for tests of unrelated lifecycle contracts."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from orchestrator.services.vm_workspace_recovery_store import CleanupPermit


def idle_recovery_store():
    """No active recovery; admit cleanup with a concrete delegation receipt."""
    return SimpleNamespace(
        unresolved_participation=AsyncMock(return_value=None),
        acquire_cleanup_permit=AsyncMock(
            return_value=CleanupPermit(allowed=True, admission_id=uuid4())
        ),
        complete_cleanup_permit=AsyncMock(return_value=True),
    )
