"""Bearer token authentication for the MCP server.

Subclasses FastMCP's TokenVerifier to validate srw_ tokens
against the orchestrator's mcp_tokens table via an internal endpoint.
"""

import hashlib
import logging
import os

import httpx
from fastmcp.server.auth import AccessToken, TokenVerifier

logger = logging.getLogger(__name__)


class TokenVerificationUnavailable(RuntimeError):
    """The orchestrator could not answer whether a token is valid.

    Raised instead of returning None: None becomes a 401, and an OAuth client
    answers a 401 by refreshing (or, failing that, by asking the user to log
    in again). An orchestrator restart must surface as a server error the
    client retries, not as a revoked credential (G6b).
    """


class McpTokenVerifier(TokenVerifier):
    """Validate MCP bearer tokens against the orchestrator database."""

    def __init__(self):
        super().__init__()
        self._api_url = os.environ.get("COCKPIT_API_URL", "http://localhost:8085")
        self._internal_key = os.environ.get("MCP_INTERNAL_KEY", "")

    async def verify_token(self, token: str) -> AccessToken | None:
        """Verify a bearer token by calling the orchestrator's internal endpoint."""
        if not token.startswith("srw_"):
            return None

        token_hash = hashlib.sha256(token.encode()).hexdigest()

        try:
            async with httpx.AsyncClient(
                base_url=self._api_url, timeout=10.0
            ) as client:
                resp = await client.post(
                    "/api/internal/mcp-token-verify",
                    json={"token_hash": token_hash},
                    headers={"X-Internal-Key": self._internal_key},
                )
        except httpx.HTTPError as exc:
            logger.warning("MCP token verification unavailable: %s", exc)
            raise TokenVerificationUnavailable(str(exc)) from exc

        if resp.status_code >= 500:
            logger.warning(
                "MCP token verification unavailable: orchestrator answered %s",
                resp.status_code,
            )
            raise TokenVerificationUnavailable(
                f"orchestrator answered {resp.status_code}"
            )
        if resp.status_code != 200:
            logger.debug("MCP token verification failed: %s", resp.status_code)
            return None
        data = resp.json()

        return AccessToken(
            token=token_hash,
            client_id=data["user_id"],
            scopes=[data["scope"]],
        )
