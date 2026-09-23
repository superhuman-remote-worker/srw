"""Credential redaction at the agent's tool-result boundary.

A token-authenticated repository clone keeps its token in the remote URL, so
``git remote -v``, a failed fetch or push, or ``cat .git/config`` prints it.
One tool-result string then fans out to every place the incident reached: the
model's ``ToolMessage``, the audit row's result preview, the ``llm_requests``
and ``chat_history`` archives (both built from the transcript), the session
transcript row and the cockpit's ``tool_result`` frame. Redacting that string
once, where a tool loop builds the ``ToolMessage``, covers all of them. Both
loops call :func:`redact_tool_result`: the worker graph's audited tool node and
the persistent turn loop (sessions, and subagent children, which run on it).

Only the RESULT is redacted, never the call's arguments: the marker in a
``write_file`` body would be written into the file. The profile is
:func:`shared.content_redaction.sanitize_tool_output`, which leaves source code
alone — the agent edits files from what it read.

Generic shapes cannot recognize a bare 40-hex token (it looks exactly like a
commit SHA), so the credential values this agent itself placed in the workspace
are matched literally as well.

**Accepted trade-off.** In this profile a URL userinfo, scp or query value is
withheld only when it looks generated: 16+ characters with both letters and
digits. That keeps `postgres:postgres@` in a compose file and `?sig=1` in a doc
readable, and it means these pass the TOOL profile unredacted unless they are
the agent's own known values: passwords shorter than 16 characters, all-letter
or all-digit tokens, passphrases, and short `?access_token=` values. The
presentation profile (what humans and officers read) still withholds all of
them.
"""

from __future__ import annotations

import logging
from typing import Any, List, Optional
from urllib.parse import unquote, urlsplit

from shared.content_redaction import sanitize_tool_output

logger = logging.getLogger(__name__)


def workspace_secrets(tool_context: Optional[Any]) -> List[str]:
    """The credential values this agent put into its own workspace.

    Repository-datasource tokens (the clone URL embeds them as
    ``oauth2:<token>@``), the credentials of a credential-bearing workspace
    remote, and whatever the context inherited (``ToolContext.
    redaction_secrets`` — a worktree child builds a fresh workspace that knows
    none of its parent's tokens). Each token also goes in as ``user:token``,
    whose base64 is the HTTP Basic header ``curl -v -u`` prints.

    Best-effort by design: a context without a workspace, or a half-built one,
    contributes nothing rather than failing the tool call being cleaned.
    """
    secrets: List[str] = []
    inherited = getattr(tool_context, "redaction_secrets", None)
    if isinstance(inherited, (list, tuple)):
        secrets.extend(s for s in inherited if isinstance(s, str) and s)
    workspace = getattr(tool_context, "workspace_manager", None)
    if workspace is None:
        return secrets
    try:
        for meta in (getattr(workspace, "source_repo_meta", None) or {}).values():
            token = meta.get("token") if isinstance(meta, dict) else None
            if isinstance(token, str) and token:
                secrets.extend((token, f"oauth2:{token}"))
    except Exception:
        logger.debug("Repository tokens unavailable for redaction", exc_info=True)
    try:
        config = getattr(workspace, "config", None)
        remote = getattr(config, "git_remote_url", None)
        if isinstance(remote, str) and remote:
            parts = urlsplit(remote)
            if parts.password:
                password = unquote(parts.password)
                secrets.append(password)
                if parts.username:
                    secrets.append(f"{unquote(parts.username)}:{password}")
    except Exception:
        logger.debug("Workspace remote unavailable for redaction", exc_info=True)
    return secrets


def redact_tool_result(content: Any, tool_context: Optional[Any] = None) -> Any:
    """``content`` with credential values replaced, in the same shape.

    A string is cleaned whole; a list of content blocks has its text blocks
    cleaned and every other block (an image, a structured part) left as is.
    Call it AFTER image tags are extracted: base64 is never worth scanning.
    """
    if isinstance(content, str):
        if not content:
            return content
        return sanitize_tool_output(
            content, secrets=workspace_secrets(tool_context)
        ).text
    if isinstance(content, list):
        secrets = workspace_secrets(tool_context)
        cleaned: List[Any] = []
        for block in content:
            if isinstance(block, str):
                cleaned.append(sanitize_tool_output(block, secrets=secrets).text)
            elif isinstance(block, dict) and isinstance(block.get("text"), str):
                cleaned.append(
                    {
                        **block,
                        "text": sanitize_tool_output(
                            block["text"], secrets=secrets
                        ).text,
                    }
                )
            else:
                cleaned.append(block)
        return cleaned
    return content


__all__ = ["redact_tool_result", "workspace_secrets"]
