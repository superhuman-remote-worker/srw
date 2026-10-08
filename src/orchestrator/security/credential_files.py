"""Validation and normalization of credentials.files[] for credential-file datasource types.

For ``kubeconfig``, ``ssh_key``, and ``generic_file`` datasources the credentials
dict carries a list of file payloads. A kubeconfig's or generic file's land in the
workspace the shell runs in, at the same path under its home (see
``src/agent/connectors/files.py``, which refuses a target outside ``/home/srw``);
an ssh_key's private key is loaded into a workspace ssh-agent instead. This module
is the single source of truth for:

- per-file size cap (64 KB) and per-datasource count cap (5)
- ``target_path`` resolution (``~`` expansion against ``/home/srw``) and safety
  (writable mounts, blocklist of system roots and agent-managed files)
- file ``mode`` well-formedness (4-digit octal)
- ``env_var`` well-formedness (POSIX identifier)
- type-specific defaults (e.g. ssh_key private at ``~/.ssh/<slug>`` with 0600)

Used by ``src/orchestrator/main.py`` at the create/update endpoints so the agent only
ever sees a fully-normalized payload.
"""

from __future__ import annotations

import os
import re
from typing import Any

from shared.connectors.builtin import legacy_types_with_slot

#: Stored types whose credentials are a ``files[]`` list, normalized here
#: (from the driver specs). An ssh_key's first file is its private key: it is
#: validated here like any file but loaded into a workspace ssh-agent, never
#: written out.
CREDENTIAL_FILE_TYPES: frozenset[str] = legacy_types_with_slot("files")

MAX_FILES_PER_DATASOURCE = 5
MAX_FILE_BYTES = 64 * 1024  # 64 KB UTF-8

# Agent home — matches hardened_container.md and the writable PVC mount.
AGENT_HOME = "/home/srw"

# A target_path must resolve under one of these roots; everything else is the
# read-only base image.
WRITABLE_ROOTS: tuple[str, ...] = ("/home/srw", "/tmp", "/run", "/workspace")

# Roots a credential file must NEVER touch even if reachable.
BLOCKED_ROOTS: tuple[str, ...] = ("/etc", "/proc", "/sys", "/dev", "/var", "/usr")

# Specific files we manage internally and don't want callers clobbering.
BLOCKED_FILES: frozenset[str] = frozenset(
    {
        f"{AGENT_HOME}/.bashrc",
        f"{AGENT_HOME}/.bash_profile",
        f"{AGENT_HOME}/.profile",
        f"{AGENT_HOME}/workspace.md",
    }
)

_MODE_RE = re.compile(r"^0[0-7]{3}$")
_ENV_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SLUG_RE = re.compile(r"[^a-z0-9]+")


class CredentialFileValidationError(ValueError):
    """Raised when a credentials.files[] payload fails validation."""


def slugify_datasource_name(name: str) -> str:
    """Lowercase-hyphen slug for filenames and kubeconfig context prefixes."""
    s = _SLUG_RE.sub("-", (name or "").lower()).strip("-")
    return s or "unnamed"


def _expand_home(path: str) -> str:
    """Expand a leading ``~`` against the agent home, without consulting ``$HOME``."""
    if path == "~":
        return AGENT_HOME
    if path.startswith("~/"):
        return AGENT_HOME + path[1:]
    return path


def _resolve_target_path(path: str) -> str:
    """Resolve ``~`` and normalize ``.``/``..`` components; return an absolute path."""
    expanded = _expand_home(path)
    if not expanded.startswith("/"):
        raise CredentialFileValidationError(
            f"target_path must be absolute or ~-rooted, got: {path!r}"
        )
    return os.path.normpath(expanded)


def _validate_target_path(path: str) -> str:
    """Return the resolved absolute path; raise if it points anywhere unsafe."""
    if not isinstance(path, str) or not path:
        raise CredentialFileValidationError(
            "target_path is required and must be a string"
        )
    resolved = _resolve_target_path(path)

    for blocked in BLOCKED_ROOTS:
        if resolved == blocked or resolved.startswith(blocked + "/"):
            raise CredentialFileValidationError(
                f"target_path is under a blocked system root ({blocked}): {path!r}"
            )
    if resolved in BLOCKED_FILES:
        raise CredentialFileValidationError(
            f"target_path is a reserved agent-managed file: {path!r}"
        )
    under_writable = any(
        resolved == root or resolved.startswith(root + "/") for root in WRITABLE_ROOTS
    )
    if not under_writable:
        raise CredentialFileValidationError(
            f"target_path must resolve under one of {WRITABLE_ROOTS}, got: {resolved}"
        )
    return resolved


def _validate_mode(mode: Any) -> str:
    if mode is None or mode == "":
        return "0600"
    if not isinstance(mode, str) or not _MODE_RE.match(mode):
        raise CredentialFileValidationError(
            f"mode must be a 4-digit octal string like '0600', got {mode!r}"
        )
    return mode


def _validate_env_var(env_var: Any) -> str | None:
    if env_var is None or env_var == "":
        return None
    if not isinstance(env_var, str) or not _ENV_RE.match(env_var):
        raise CredentialFileValidationError(
            f"env_var must be a POSIX identifier ([A-Za-z_][A-Za-z0-9_]*), got {env_var!r}"
        )
    return env_var


def _validate_contents(contents: Any, *, label: str) -> str:
    if contents is None:
        raise CredentialFileValidationError(f"{label}: contents is required")
    if not isinstance(contents, str):
        raise CredentialFileValidationError(f"{label}: contents must be a UTF-8 string")
    if len(contents.encode("utf-8")) > MAX_FILE_BYTES:
        raise CredentialFileValidationError(
            f"{label}: contents exceed {MAX_FILE_BYTES} bytes"
        )
    return contents


def _normalize_one_file(
    entry: Any,
    *,
    idx: int,
    default_name: str,
    default_target_path: str | None,
    default_mode: str,
    default_env_var: str | None = None,
) -> dict[str, Any]:
    if not isinstance(entry, dict):
        raise CredentialFileValidationError(
            f"files[{idx}] must be an object, got {type(entry).__name__}"
        )
    label = f"files[{idx}]"
    name = entry.get("name") or default_name
    contents = _validate_contents(entry.get("contents"), label=label)
    target_path = entry.get("target_path") or default_target_path
    if not target_path:
        raise CredentialFileValidationError(f"{label}: target_path is required")
    resolved = _validate_target_path(target_path)
    mode_raw = entry.get("mode")
    mode = _validate_mode(mode_raw if mode_raw not in (None, "") else default_mode)
    env_raw = entry.get("env_var")
    env_var = _validate_env_var(
        env_raw if env_raw not in (None, "") else default_env_var
    )

    out: dict[str, Any] = {
        "name": name,
        "contents": contents,
        "target_path": resolved,
        "mode": mode,
    }
    if env_var:
        out["env_var"] = env_var
    return out


def normalize_credential_files(
    ds_type: str,
    ds_name: str,
    credentials: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Apply per-type defaults and validate the ``credentials.files[]`` payload.

    For datasource types in :data:`CREDENTIAL_FILE_TYPES` this:

    - Requires ``credentials.files`` to be a non-empty list.
    - For ``kubeconfig``: expects exactly one file; fills in
      ``target_path=~/.kube/configs/<slug>.yaml`` and ``mode=0600`` if absent.
    - For ``ssh_key``: expects 1 or 2 files (private, optional public);
      fills in ``~/.ssh/<slug>`` (0600) and ``~/.ssh/<slug>.pub`` (0644).
    - For ``generic_file``: every entry must carry its own ``target_path``;
      ``mode`` defaults to ``0600``.

    For all other types the credentials dict is returned untouched.

    Raises :class:`CredentialFileValidationError` on any validation failure.
    """
    if ds_type not in CREDENTIAL_FILE_TYPES:
        return credentials
    if not credentials or not isinstance(credentials, dict):
        raise CredentialFileValidationError(
            "credentials.files[] is required for credential-file connector types"
        )
    files = credentials.get("files")
    if not isinstance(files, list) or not files:
        raise CredentialFileValidationError(
            "credentials.files must be a non-empty list of file entries"
        )
    if len(files) > MAX_FILES_PER_DATASOURCE:
        raise CredentialFileValidationError(
            f"At most {MAX_FILES_PER_DATASOURCE} files per connector, got {len(files)}"
        )

    slug = slugify_datasource_name(ds_name)

    if ds_type == "kubeconfig":
        if len(files) != 1:
            raise CredentialFileValidationError(
                "kubeconfig connector must have exactly one file"
            )
        normalized = [
            _normalize_one_file(
                files[0],
                idx=0,
                default_name=f"{slug}.yaml",
                default_target_path=f"~/.kube/configs/{slug}.yaml",
                default_mode="0600",
                default_env_var=None,
            )
        ]
    elif ds_type == "ssh_key":
        if len(files) > 2:
            raise CredentialFileValidationError(
                "ssh_key connector has at most two files (private + optional public)"
            )
        normalized = [
            _normalize_one_file(
                files[0],
                idx=0,
                default_name=slug,
                default_target_path=f"~/.ssh/{slug}",
                default_mode="0600",
            )
        ]
        if len(files) == 2:
            normalized.append(
                _normalize_one_file(
                    files[1],
                    idx=1,
                    default_name=f"{slug}.pub",
                    default_target_path=f"~/.ssh/{slug}.pub",
                    default_mode="0644",
                )
            )
    else:  # generic_file
        normalized = [
            _normalize_one_file(
                entry,
                idx=idx,
                default_name=f"file-{idx}",
                default_target_path=None,
                default_mode="0600",
            )
            for idx, entry in enumerate(files)
        ]

    new_creds = dict(credentials)
    new_creds["files"] = normalized
    return new_creds
