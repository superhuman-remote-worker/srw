"""The public projection of an authenticated user.

One shape, one owner. Every API response that hands a caller "who am I"
goes through :func:`user_dict`, so a field cannot be added to the auth
bootstrap and silently missed by another identity surface.
"""

from typing import Any


def user_dict(user: dict[str, Any]) -> dict[str, Any]:
    """Build the public user dict for API responses."""
    return {
        "id": str(user["id"]),
        "display_name": user["display_name"],
        "avatar_color": user["avatar_color"],
        "email": user.get("email"),
        "default_project_id": str(user["default_project_id"])
        if user.get("default_project_id")
        else None,
        "is_admin": user.get("is_admin", False),
        "is_approved": user.get("is_approved", False),
        "can_use_vm": bool(user.get("can_use_vm", False)),
        "created_at": user["created_at"],
    }


def credential_dict(user: dict[str, Any]) -> dict[str, Any]:
    """How this request authenticated, and what that credential may do.

    A sibling of :func:`user_dict` in the auth bootstrap, never part of it:
    the user projection is the same whichever credential presents it. A
    client holding a token learns here which token it is, what it may do and
    when it stops working, without a route of its own to list tokens (tokens
    cannot reach those).

    ``method``: ``pat`` (a personal access token), ``mcp`` (an MCP token or
    the MCP server acting for one), or ``oidc`` (a Keycloak sign-in: the
    Cockpit's session or a Keycloak bearer). ``scopes`` are a personal access
    token's action scopes. ``scope`` is an MCP token's single scope string
    (``user``, ``all`` or ``project:<uuid>``). The fields that do not apply
    are null.
    """
    method = user.get("auth_method")
    held = [scope for scope in user.get("scopes") or [] if isinstance(scope, str)]
    if method == "pat":
        scopes, scope = sorted(held), None
    elif method == "mcp":
        scopes, scope = None, (held[0] if held else None)
    else:
        method, scopes, scope = "oidc", None, None
    return {
        "method": method,
        "token_name": user.get("token_name"),
        "scopes": scopes,
        "scope": scope,
        "expires_at": user.get("token_expires_at"),
    }
