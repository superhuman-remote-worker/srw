"""Credential redaction for stored ``config_override`` documents.

Pure functions, no service or startup imports, so catalogue and inspection
modules can use them without importing application startup (the
``Catalogue and inspection operations and schemas do not import application
startup`` import-linter contract). ``orchestrator.security.access`` re-exports
them for existing callers.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any


# Key names (case-insensitive) whose VALUE is always a credential. The suffix
# match covers the ``env_keys`` block (EMBEDDING_API_KEY, OPENROUTER_API_KEY,
# VISION_API_KEY, WHISPER_API_KEY, TTS_API_KEY, CITATION_LLM_API_KEY, ...).
_SECRET_KEY_NAMES = frozenset(
    {"api_key", "password", "secret", "token", "private_key", "rclone_spec"}
)
_SECRET_KEY_SUFFIX = "_api_key"


def _is_secret_key(key: str) -> bool:
    k = key.lower()
    return k in _SECRET_KEY_NAMES or k.endswith(_SECRET_KEY_SUFFIX)


def redact_config_override(co: Any) -> Any:
    """Return a deep copy of a ``config_override`` with credential fields removed.

    Non-mutating and recursive (walks dicts and lists). Removes any key whose
    name (case-insensitive) is one of api_key/password/secret/token/private_key/
    rclone_spec, or ends with ``_api_key`` — i.e. ``llm.api_key``, phase overrides
    ``llm.{strategic,tactical,summarization}.api_key``, ``auxiliary.api_key``,
    every ``env_keys.*_API_KEY``, and ``workspace.mounts[].rclone_spec``.

    Non-secret fields are preserved verbatim (``llm.model``/``provider``/
    ``base_url``/``temperature``, ``env_keys.*_MODEL``/``*_BASE_URL``/``*_PROVIDER``,
    ``workspace.backend``, ...).

    Used at two boundaries:
    - the user-facing GET endpoints (redact before returning), and
    - persistence of ``threads.metadata.config_override`` (secrets are injected
      in-flight only — see ``_inject_thread_dispatch_credentials`` in main.py).

    Keep this pure: no DB access, no logging of values (it handles secrets).
    """
    if isinstance(co, dict):
        return {
            k: redact_config_override(v) for k, v in co.items() if not _is_secret_key(k)
        }
    if isinstance(co, list):
        return [redact_config_override(v) for v in co]
    return co


def _hidden_config_key(path: tuple[str, ...], key: str) -> bool:
    """A key :func:`redact_public_config_override` removes at ``path``.

    ``remote`` is hidden under ANY ``workspace`` dict, not only the override's
    root one: the same override also rides nested — as a manifest layer, a
    merged expert config — and the transport block is the same thing there.
    """
    return _is_secret_key(key) or (key == "remote" and path[-1:] == ("workspace",))


def _public_config_view(value: Any, path: tuple[str, ...] = ()) -> Any:
    if isinstance(value, dict):
        return {
            k: _public_config_view(v, (*path, k))
            for k, v in value.items()
            if not _hidden_config_key(path, k)
        }
    if isinstance(value, list):
        return [_public_config_view(v, (*path, "[]")) for v in value]
    return value


def redact_public_config_override(co: Any) -> Any:
    """The browser-facing view of a stored config override.

    The job API's policy (``job_projection.redact_job_config_override``):
    :func:`redact_config_override` plus the ``workspace.remote`` transport block
    (SSH coordinates injected at dispatch). JSONB arrives from asyncpg as text,
    so a string is parsed and returned as an object; one that does not parse is
    dropped rather than risk returning a raw secret.

    It walks the whole value, so it also serves for a document that EMBEDS
    overrides — a Project manifest carries the project's override verbatim in
    each Expert's ``runtime.config.layers`` — and is the identity on one with
    nothing to hide.
    """
    if isinstance(co, str):
        try:
            co = json.loads(co)
        except (json.JSONDecodeError, TypeError):
            return None
    return _public_config_view(co)


# Name parts that say WHERE a request goes (``base_url``, ``EMBEDDING_BASE_URL``,
# ``endpoint_id``, ``provider``, ``http_proxy``, ``mcp_servers`` ...). A restored
# key must never ride to an endpoint its writer could not see it bound to, so a
# change to any such key anywhere in an override restores nothing. Deliberately
# broad: a false match only means a secret has to be re-entered.
_ENDPOINT_KEY_TOKENS = frozenset(
    {
        "url",
        "urls",
        "uri",
        "host",
        "hostname",
        "endpoint",
        "endpoints",
        "proxy",
        "provider",
        "server",
        "servers",
        "address",
        "addr",
        "dsn",
        "webhook",
        "webhooks",
        "gateway",
        "domain",
        "domains",
        "ref",
    }
)


def _is_endpoint_key(key: str) -> bool:
    k = key.lower()
    return not _ENDPOINT_KEY_TOKENS.isdisjoint(
        re.split(r"[^a-z0-9]+", k)
    ) or k.endswith(
        ("url", "uri", "host", "endpoint", "api_base", "apibase", "connection_string")
    )


def _endpoint_values(value: Any, path: tuple[Any, ...] = ()) -> dict[Any, Any]:
    """Every endpoint-shaped key in ``value``, by full path (list index
    included, so a reordered list of endpoints counts as a change)."""
    found: dict[Any, Any] = {}
    if isinstance(value, dict):
        for k, v in value.items():
            if _is_endpoint_key(k):
                found[(*path, k)] = v
            found.update(_endpoint_values(v, (*path, k)))
    elif isinstance(value, list):
        for i, v in enumerate(value):
            found.update(_endpoint_values(v, (*path, i)))
    return found


def restore_hidden_config_values(incoming: Any, stored: Any) -> Any:
    """Put back what :func:`redact_public_config_override` hid, for a client
    that writes the redacted view back whole (read, flip one key, PATCH the
    override — the cockpit's project-memory toggle does exactly this).

    What it guarantees, and nothing more:

    * A hidden value is only ever put back at the exact path it was stored at.
    * Nothing is restored anywhere if any endpoint-shaped key (a name with a
      ``url`` / ``host`` / ``endpoint`` / ``provider`` / ``proxy`` / ``server``
      / ``address`` part, e.g. ``base_url``, ``EMBEDDING_BASE_URL``,
      ``endpoint_id``) anywhere in the written override differs from the stored
      one — added, removed or changed, including inside an explicitly sent
      ``workspace.remote``. The writer re-enters its secrets.
    * Otherwise a hidden value comes back only into a dict whose public view is
      unchanged, or a list element equal to a stored element's public view.
    * A hidden key the write sends itself, in any letter case and ``null``
      included, is never overwritten.

    It does not make a restored key safe against every edit: a change that is
    not an endpoint (a model, a tool list) keeps the keys of sections it did not
    touch. A project override has several writers — any co-owner — so the guard
    is what stops one who cannot read a key from re-pointing it at a host they
    control. ``stored`` may be JSONB text.
    """
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except (json.JSONDecodeError, TypeError):
            return incoming
    if _endpoint_values(incoming) != _endpoint_values(_public_config_view(stored)):
        return incoming
    return _restore_hidden(incoming, stored, ())


def _restore_hidden(incoming: Any, stored: Any, path: tuple[str, ...]) -> Any:
    if isinstance(incoming, dict) and isinstance(stored, dict):
        out = {
            k: _restore_hidden(v, stored[k], (*path, k)) if k in stored else v
            for k, v in incoming.items()
        }
        if _public_config_view(incoming, path) == _public_config_view(stored, path):
            sent = {k.lower() for k in incoming}
            for k, v in stored.items():
                if k.lower() not in sent and _hidden_config_key(path, k):
                    out[k] = copy.deepcopy(v)
        return out
    if isinstance(incoming, list) and isinstance(stored, list):
        element_path = (*path, "[]")
        unmatched = list(stored)
        out_list = []
        for item in incoming:
            match = next(
                (
                    i
                    for i, candidate in enumerate(unmatched)
                    if _public_config_view(candidate, element_path) == item
                ),
                None,
            )
            out_list.append(
                item if match is None else copy.deepcopy(unmatched.pop(match))
            )
        return out_list
    return incoming


def dropped_hidden_config_values(written: Any, stored: Any) -> list[str]:
    """Paths (``llm.api_key``, ``workspace.mounts[0].rclone_spec``) of the
    hidden values ``stored`` holds that ``written`` neither kept nor sent a
    value for — what a write of the redacted view discarded, so the caller can
    be told instead of losing a key silently.

    A key the write sends itself (any letter case) is not reported. A list
    element the write kept verbatim is not reported; one it changed is
    compared with the written element at the same index, since list elements
    have no key to line up by. A stored ``null`` is not a secret and is never
    reported. ``stored`` may be JSONB text.
    """
    if isinstance(stored, str):
        try:
            stored = json.loads(stored)
        except (json.JSONDecodeError, TypeError):
            return []
    return _dropped_hidden(written, stored, (), "")


def _dropped_hidden(
    written: Any, stored: Any, path: tuple[str, ...], label: str
) -> list[str]:
    dropped: list[str] = []
    if isinstance(stored, dict):
        kept = written if isinstance(written, dict) else {}
        sent = {k.lower() for k in kept}
        for k, v in stored.items():
            name = f"{label}.{k}" if label else k
            if _hidden_config_key(path, k):
                if v is not None and k.lower() not in sent:
                    dropped.append(name)
            else:
                dropped += _dropped_hidden(kept.get(k), v, (*path, k), name)
    elif isinstance(stored, list):
        items = written if isinstance(written, list) else []
        for i, v in enumerate(stored):
            if v in items:
                continue
            counterpart = items[i] if i < len(items) else None
            dropped += _dropped_hidden(counterpart, v, (*path, "[]"), f"{label}[{i}]")
    return dropped
