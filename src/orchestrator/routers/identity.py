"""HTTP adapters for authenticated identity with per-app dependencies."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from fastapi import APIRouter, Depends, Request

from orchestrator.security.auth import get_current_user
from orchestrator.services import identity

router = APIRouter()


@dataclass(frozen=True)
class IdentityDependencies:
    store: Any
    get_current_user: Callable[..., Awaitable[Any]] = get_current_user


def get_identity_dependencies(request: Request) -> IdentityDependencies:
    return request.app.state.identity_dependencies_factory()


# nosec: public auth-bootstrap (Bearer-required, intentionally serves pending-approval users)
@router.get("/api/auth/me")
async def auth_me(
    request: Request,
    *,
    dependencies: IdentityDependencies = Depends(get_identity_dependencies),
) -> dict[str, Any]:
    """Get current user from Bearer token (OIDC).

    Always returns the user record (even if not yet approved) so the cockpit
    can display a "pending approval" message instead of a blank screen.
    """
    user = await dependencies.get_current_user(request, dependencies.store)
    return {
        "user": identity.user_dict(user),
        "credential": identity.credential_dict(user),
    }
