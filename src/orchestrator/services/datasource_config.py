"""Pure validation/normalization of the non-secret and secret fields a
connector carries.

Extracted verbatim from ``orchestrator.main`` (R1.B03 lane D). Deliberately
free of every collaborator — no store, no clients, no logger, no application
global — because three independent callers share this authority: the
datasource CRUD surface, the project-provisioning KB vault plan, and the job
datasource payload builder. A second copy of "what may appear in a KB config"
is how the native-project marker gets stripped by one path and honored by
another.

The ``HTTPException``s raised here are the API's own 400 contract, so the
detail strings move with the code rather than being re-derived by a caller.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any
from urllib.parse import urlparse

from fastapi import HTTPException

from shared.connectors.builtin import MCP_STDIO_RETIRED
from shared.runtime.utils.ssh_key import (
    InvalidSSHKeyError,
    validate_private_key as _validate_ssh_private_key,
)


def stored_json_object(value: Any) -> dict[str, Any]:
    """A stored JSONB value as a dict.

    asyncpg hands JSONB back as text unless a codec is registered, and a test
    double hands back a dict: a JSON string is parsed, and anything that is
    not an object (junk, null, a list) is an empty dict.
    """
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def normalize_datasource_credentials(
    credentials: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Validate and normalize secret fields in a datasource credentials dict.

    Currently this means: if an ``ssh_key`` is present, run it through
    :func:`validate_private_key`, which trims surrounding whitespace,
    normalizes line endings, and ensures the single trailing newline that
    OpenSSL/libcrypto requires. Raises ``HTTPException(400)`` if the key
    fails structural validation.
    """
    if not credentials:
        return credentials
    ssh_key = credentials.get("ssh_key")
    if ssh_key is None:
        return credentials
    try:
        credentials["ssh_key"] = _validate_ssh_private_key(ssh_key)
    except InvalidSSHKeyError as exc:
        raise HTTPException(status_code=400, detail=f"Invalid ssh_key: {exc}") from exc
    return credentials


def normalize_kb_config(
    config: dict[str, Any] | None, *, stored: bool = False
) -> dict[str, Any]:
    """Validate the non-secret v1 config for an OKF KB datasource.

    ``root_path`` is relative to the configured repository root. Keep the
    accepted shape intentionally small so misspelled future-looking keys do not
    silently change indexing behavior.

    ``stored=True`` normalizes a config read back out of the database and
    carries the server-owned ``native_project_id`` marker through. User input
    is always normalized without it, so the marker cannot be hand-forged onto
    an external connector to steer it out of the sweep — and, read the other
    way, cannot be stripped off a project's own KB by editing its root path
    (that would drop the vault straight back into the external sweep and
    double-index it; knowledge-base/knowledge/features/knowledge_base_repo_separation.md §6).
    """
    from orchestrator.services.kb_datasources import NATIVE_PROJECT_CONFIG_KEY

    raw = dict(config or {})
    native_project = raw.pop(NATIVE_PROJECT_CONFIG_KEY, None) if stored else None
    unknown = sorted(set(raw) - {"root_path", "forge"})
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown KB config field(s): {', '.join(unknown)}",
        )

    value = raw.get("root_path", "")
    if value is None:
        value = ""
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="KB root_path must be a string")
    if "\x00" in value:
        raise HTTPException(status_code=400, detail="KB root_path contains NUL")

    normalized = value.strip().replace("\\", "/")
    if normalized.startswith("/") or urlparse(normalized).scheme:
        raise HTTPException(
            status_code=400,
            detail="KB root_path must be a relative repository path",
        )

    parts: list[str] = []
    for part in normalized.split("/"):
        if part in ("", "."):
            continue
        if part == "..":
            raise HTTPException(
                status_code=400,
                detail="KB root_path must not contain '..'",
            )
        parts.append(part)
    normalized_config = {"root_path": "/".join(parts)}
    forge = raw.get("forge")
    if forge is not None:
        if not isinstance(forge, str):
            raise HTTPException(status_code=400, detail="KB forge must be a string")
        from shared.runtime.services.forge import SUPPORTED_FORGES

        normalized_forge = forge.strip().lower()
        if normalized_forge not in SUPPORTED_FORGES:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unsupported KB forge {normalized_forge!r}; expected one of "
                    f"{sorted(SUPPORTED_FORGES)}"
                ),
            )
        normalized_config["forge"] = normalized_forge
    if native_project:
        normalized_config[NATIVE_PROJECT_CONFIG_KEY] = str(native_project)
    return normalized_config


def normalize_repository_config(
    config: dict[str, Any] | None, connection_url: str | None
) -> dict[str, Any]:
    """Validate and default the ``forge`` field on a repository datasource.

    Host inference only covers the two SaaS hosts. A self-hosted Gitea and a
    self-hosted GitLab are indistinguishable by URL, so those must declare
    ``forge`` explicitly rather than be guessed at.
    """
    from shared.runtime.services.forge import SUPPORTED_FORGES  # noqa: PLC0415

    out = dict(config or {})
    forge = str(out.get("forge") or "").strip().lower()

    if not forge:
        host = (urlparse(connection_url or "").hostname or "").lower()
        if host in ("github.com", "www.github.com"):
            forge = "github"
        elif host in ("gitlab.com", "www.gitlab.com"):
            forge = "gitlab"
        else:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Repository connectors on a self-hosted host must declare "
                    f"'forge' explicitly (one of {sorted(SUPPORTED_FORGES)}) — "
                    "a self-hosted Gitea and GitLab cannot be told apart by URL"
                ),
            )

    if forge not in SUPPORTED_FORGES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported forge {forge!r}; expected one of {sorted(SUPPORTED_FORGES)}",
        )

    out["forge"] = forge
    # The git swap driver's roots for a forge behind a private CA (C3).
    from orchestrator.services.connector_git_swap_delivery import (  # noqa: PLC0415
        validate_upstream_ca,
    )

    try:
        upstream_ca = validate_upstream_ca(out.get("upstream_ca"))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if upstream_ca is None:
        out.pop("upstream_ca", None)
    else:
        out["upstream_ca"] = upstream_ca
    return out


def validate_kb_repository_url(connection_url: str | None) -> str:
    """Require a safe network Git URL with no embedded credentials."""
    value = (connection_url or "").strip()
    if not value:
        raise HTTPException(
            status_code=400,
            detail="OKF Knowledge Base connectors require a repository URL",
        )
    from orchestrator.services.kb_git_source import validate_git_remote_url

    try:
        return validate_git_remote_url(value, allow_local=False)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=str(exc),
        ) from exc


def validate_kb_repository_auth(
    connection_url: str,
    credentials: dict[str, Any] | None,
) -> None:
    """Reject unsafe OKF repository transport/auth combinations pre-persist."""
    from orchestrator.services.kb_git_source import validate_git_auth_configuration

    try:
        validate_git_auth_configuration(
            connection_url,
            credentials,
            allow_local=False,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# MCP server connectors
#
# Moved verbatim from ``orchestrator.main`` (R1.B05 lane P). A stdio server is
# refused: stdio servers no longer run in the agent pod (connector drivers
# D5b); a stdio image runs as a managed MCP server instead.
#
# The refusal shape is the contract. Every rejection here is
# ``HTTPException(400)`` with a message that names the offending FIELD and
# never echoes a credential value -- ``detail`` strings are what the API
# renders, so they move with the code rather than being re-derived.
# ---------------------------------------------------------------------------


def validate_mcp_datasource(
    connection_url: str | None,
    credentials: dict[str, Any],
) -> None:
    """Validate an MCP datasource without reflecting credential values."""
    if not isinstance(credentials, dict):
        raise HTTPException(status_code=400, detail="MCP credentials must be an object")

    raw_transport = credentials.get("transport") or "http"
    if not isinstance(raw_transport, str):
        raise HTTPException(status_code=400, detail="MCP transport must be a string")
    transport = raw_transport.lower().strip()
    if transport == "stdio":
        raise HTTPException(status_code=400, detail=MCP_STDIO_RETIRED)
    if transport not in ("http", "sse"):
        raise HTTPException(
            status_code=400,
            detail="Invalid MCP transport (expected http or sse)",
        )

    unknown = sorted(set(credentials) - {"transport", "auth"})
    if unknown:
        raise HTTPException(
            status_code=400,
            detail="Unknown remote MCP credential field(s)",
        )
    value = (connection_url or "").strip()
    if not value:
        raise HTTPException(
            status_code=400,
            detail=f"{transport} MCP servers require connection_url",
        )
    parsed = urlparse(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise HTTPException(
            status_code=400,
            detail="Remote MCP connection_url must be an HTTP(S) URL",
        )
    if parsed.username is not None or parsed.password is not None:
        raise HTTPException(
            status_code=400,
            detail="Remote MCP connection_url must not embed credentials",
        )

    auth = credentials.get("auth") or {}
    if not isinstance(auth, dict):
        raise HTTPException(
            status_code=400,
            detail="MCP credentials.auth must be an object",
        )
    auth_type = auth.get("type") or "none"
    if auth_type == "bearer":
        if set(auth) - {"type", "token"}:
            raise HTTPException(
                status_code=400,
                detail="Unknown MCP bearer auth field(s)",
            )
        token = auth.get("token")
        if not isinstance(token, str) or not token:
            raise HTTPException(
                status_code=400,
                detail="MCP bearer auth requires a token",
            )
    elif auth_type == "headers":
        if set(auth) - {"type", "headers"}:
            raise HTTPException(
                status_code=400,
                detail="Unknown MCP custom-header auth field(s)",
            )
        headers = auth.get("headers") or {}
        if not isinstance(headers, dict) or not all(
            isinstance(key, str)
            and bool(key.strip())
            and "\r" not in key
            and "\n" not in key
            and isinstance(value, str)
            and "\r" not in value
            and "\n" not in value
            for key, value in headers.items()
        ):
            raise HTTPException(
                status_code=400,
                detail="MCP custom headers must map valid names to string values",
            )
    elif auth_type in ("none", ""):
        if set(auth) - {"type"}:
            raise HTTPException(
                status_code=400,
                detail="Unknown MCP no-auth field(s)",
            )
    else:
        raise HTTPException(status_code=400, detail="Invalid MCP auth type")
