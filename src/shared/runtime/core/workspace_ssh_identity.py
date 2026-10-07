"""SSH identities of external connectors, held by an ``ssh-agent`` in the workspace.

External SSH repository connectors and ``ssh_key`` connectors put a user's
private key at the workspace's disposal. Each identity is loaded into its own
dedicated ``ssh-agent`` under the managed-repository namespace
(``~/.ssh/srw-managed/sockets/<32hex>.sock``) so the agent and the user can
sign with it but cannot read it, and every terminal owner that already retires
that namespace retires these agents too.

This module holds the parts both sides of the wire share: the strict endpoint
grammar, Git URL parsing and ``known_hosts`` parsing. A shared connector writes
into other users' ``~/.ssh/config``, so every value that reaches an SSH config
line is validated here, never quoted or escaped: a newline or a ``ProxyCommand``
smuggled through a host name would run code in their workspaces.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import ipaddress
import re
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

_HOST_LABEL = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?")
_SSH_USER = re.compile(r"[A-Za-z_][A-Za-z0-9._-]{0,63}")
_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._~-]{1,255}")
_SCP_URL = re.compile(
    r"(?:(?P<user>[^@/:\s]+)@)?(?P<host>\[[^\]\s]+\]|[^:/\s\[\]]+):(?P<path>[^\s]+)"
)

#: Host key algorithms a pinned ``known_hosts`` entry may name. Certificates,
#: ``@cert-authority`` and ``@revoked`` markers are deliberately out of scope.
KNOWN_HOST_KEY_TYPES = frozenset(
    {
        "ssh-ed25519",
        "ssh-rsa",
        "ecdsa-sha2-nistp256",
        "ecdsa-sha2-nistp384",
        "ecdsa-sha2-nistp521",
        "sk-ssh-ed25519@openssh.com",
        "sk-ecdsa-sha2-nistp256@openssh.com",
    }
)
_MAX_KNOWN_HOSTS_BYTES = 64 * 1024
_MAX_KNOWN_HOST_KEYS = 32
_MAX_HOST_KEY_BLOB = 16 * 1024


class SshEndpointError(ValueError):
    """An SSH endpoint value is unsafe; the message is fit to show a user."""


def normalize_ssh_host(value: Any) -> str:
    """Return a host name or IP literal that is safe on an SSH config line."""

    text = str(value or "").strip()
    if not text or len(text) > 253:
        raise SshEndpointError("SSH host must be a DNS name or an IP address")
    candidate = text[1:-1] if text.startswith("[") and text.endswith("]") else text
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        address = None
    if address is not None:
        if getattr(address, "scope_id", None):
            raise SshEndpointError("SSH host must not carry an IPv6 zone index")
        return address.compressed
    labels = text.split(".")
    if not all(_HOST_LABEL.fullmatch(label) for label in labels):
        raise SshEndpointError("SSH host must be a DNS name or an IP address")
    return text.lower()


def normalize_ssh_user(value: Any) -> str:
    """Return a login name that is safe on an SSH config line."""

    text = str(value or "").strip()
    if not _SSH_USER.fullmatch(text):
        raise SshEndpointError(
            "SSH user must start with a letter or '_' and contain only letters, "
            "digits, '.', '_' or '-'"
        )
    return text


def normalize_ssh_port(value: Any) -> int:
    """Return an integer TCP port; booleans and free text are refused."""

    if isinstance(value, bool):
        raise SshEndpointError("SSH port must be an integer between 1 and 65535")
    if isinstance(value, str) and value.strip().isdigit():
        value = int(value.strip())
    if not isinstance(value, int) or not 1 <= value <= 65535:
        raise SshEndpointError("SSH port must be an integer between 1 and 65535")
    return value


@dataclass(frozen=True)
class SshRepositoryTarget:
    """The SSH endpoint and repository path of one Git remote."""

    host: str
    port: int
    user: str
    path: str


def _normalize_repository_path(raw: str) -> str:
    path = raw.strip().strip("/")
    segments = path.split("/") if path else []
    if not segments or not all(
        _PATH_SEGMENT.fullmatch(segment) and segment not in {".", ".."}
        for segment in segments
    ):
        raise SshEndpointError(
            "Repository path may contain only letters, digits and '.', '_', "
            "'~', '-' separated by '/'"
        )
    return "/".join(segments)


def parse_ssh_repository_url(url: Any) -> SshRepositoryTarget:
    """Resolve the SSH endpoint an SSH-key repository connector clones from.

    Accepts ``ssh://[user@]host[:port]/path``, the scp form
    ``[user@]host:path`` and, as today's clone path does, an ``http(s)`` URL
    that is cloned over SSH on port 22 as user ``git``. The scp form used to
    parse to no host at all and wrote its ``Host`` block as ``localhost``.
    """

    text = str(url or "").strip()
    if not text or any(ord(ch) < 0x21 or ord(ch) == 0x7F for ch in text):
        raise SshEndpointError("Repository URL must be a single-line SSH or HTTPS URL")
    if "://" in text:
        parsed = urlparse(text)
        if parsed.query or parsed.fragment or parsed.params:
            raise SshEndpointError("Repository URL must not carry a query or fragment")
        if parsed.password is not None:
            raise SshEndpointError("Repository URL must not embed a password")
        try:
            raw_port = parsed.port
        except ValueError as exc:
            raise SshEndpointError(
                "SSH port must be an integer between 1 and 65535"
            ) from exc
        scheme = parsed.scheme.lower()
        converted = scheme in {"http", "https"}
        if scheme in {"ssh", "git+ssh", "ssh+git"}:
            user = parsed.username or "git"
            port = raw_port or 22
        elif converted:
            if parsed.username is not None:
                raise SshEndpointError(
                    "An SSH-key repository URL must not embed credentials"
                )
            # An HTTPS URL is cloned over SSH on the forge's standard port,
            # exactly as the pre-agent clone path converted it.
            user, port = "git", 22
        else:
            raise SshEndpointError("Repository URL must use ssh, http or https")
        host = parsed.hostname or ""
        path = parsed.path
    else:
        converted = False
        match = _SCP_URL.fullmatch(text)
        if match is None:
            raise SshEndpointError("Repository URL must be an SSH or HTTPS URL")
        user = match.group("user") or "git"
        host = match.group("host")
        port = 22
        path = match.group("path")
    normalized_path = _normalize_repository_path(path)
    if converted and not normalized_path.endswith(".git"):
        normalized_path += ".git"
    return SshRepositoryTarget(
        host=normalize_ssh_host(host),
        port=normalize_ssh_port(port),
        user=normalize_ssh_user(user),
        path=normalized_path,
    )


def _host_key_entry(key_type: str, blob_b64: str, *, line_number: int) -> str:
    if key_type not in KNOWN_HOST_KEY_TYPES:
        raise SshEndpointError(
            f"known_hosts line {line_number}: unsupported key type {key_type[:40]!r}"
        )
    try:
        blob = base64.b64decode(blob_b64, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise SshEndpointError(
            f"known_hosts line {line_number}: host key is not valid base64"
        ) from exc
    name = key_type.encode("ascii")
    if (
        len(blob) > _MAX_HOST_KEY_BLOB
        or len(blob) < 4 + len(name)
        or int.from_bytes(blob[:4], "big") != len(name)
        or blob[4 : 4 + len(name)] != name
    ):
        raise SshEndpointError(
            f"known_hosts line {line_number}: host key does not match its type"
        )
    return f"{key_type} {base64.b64encode(blob).decode('ascii')}"


def _hashed_host_matches(field: str, target: str) -> bool:
    parts = field.split("|")
    if len(parts) != 4 or parts[0] or parts[1] != "1":
        return False
    try:
        salt = base64.b64decode(parts[2], validate=True)
        expected = base64.b64decode(parts[3], validate=True)
    except (binascii.Error, ValueError):
        return False
    digest = hmac.new(salt, target.encode("utf-8"), hashlib.sha1).digest()
    return hmac.compare_digest(digest, expected)


def _host_pattern_matches(pattern: str, target: str) -> bool:
    # OpenSSH patterns know only ``*`` and ``?``; ``[host]:port`` brackets are
    # literal, unlike in fnmatch.
    expression = "".join(
        ".*" if ch == "*" else "." if ch == "?" else re.escape(ch) for ch in pattern
    )
    return re.fullmatch(expression, target, flags=re.IGNORECASE) is not None


def known_hosts_field_matches(field: str, *, host: str, port: int) -> bool:
    """Whether a ``known_hosts`` host field names ``host`` on ``port``."""

    target = host if port == 22 else f"[{host}]:{port}"
    if field.startswith("|"):
        return _hashed_host_matches(field, target)
    matched = False
    for pattern in field.split(","):
        negated = pattern.startswith("!")
        if negated:
            pattern = pattern[1:]
        if pattern and _host_pattern_matches(pattern, target):
            if negated:
                return False
            matched = True
    return matched


def parse_known_hosts(
    text: Any,
    *,
    host: str | None = None,
    port: int = 22,
    require_match: bool = True,
) -> list[str]:
    """Return the ``"<type> <base64>"`` host keys of ``known_hosts`` text.

    A line may be a bare ``<type> <base64>`` pair (what a connector's Test
    returns) or a full ``known_hosts`` line. A full line must name ``host`` on
    ``port``; with ``require_match=False`` (a deployment-wide default list) a
    line for another host is skipped instead of refused. Markers, certificates
    and unknown key types are refused, so the rendered per-identity file holds
    nothing but plain keys under the identity's own alias.
    """

    raw = "" if text is None else str(text)
    if len(raw.encode("utf-8")) > _MAX_KNOWN_HOSTS_BYTES or "\x00" in raw:
        raise SshEndpointError("known_hosts must be at most 64 KiB of text")
    entries: list[str] = []
    for line_number, line in enumerate(raw.splitlines(), start=1):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        tokens = stripped.split()
        if tokens[0].startswith("@"):
            raise SshEndpointError(
                f"known_hosts line {line_number}: markers such as @cert-authority "
                "are not supported"
            )
        if tokens[0] in KNOWN_HOST_KEY_TYPES:
            if len(tokens) < 2:
                raise SshEndpointError(f"known_hosts line {line_number} has no key")
            entry = _host_key_entry(tokens[0], tokens[1], line_number=line_number)
        else:
            if len(tokens) < 3:
                raise SshEndpointError(
                    f"known_hosts line {line_number} must be '<host> <type> <key>' "
                    "or '<type> <key>'"
                )
            entry = _host_key_entry(tokens[1], tokens[2], line_number=line_number)
            if host is not None and not known_hosts_field_matches(
                tokens[0], host=host, port=port
            ):
                if require_match:
                    raise SshEndpointError(
                        f"known_hosts line {line_number} is for a different host"
                    )
                continue
        if entry not in entries:
            entries.append(entry)
    if len(entries) > _MAX_KNOWN_HOST_KEYS:
        raise SshEndpointError(
            f"known_hosts may pin at most {_MAX_KNOWN_HOST_KEYS} host keys"
        )
    return entries


__all__ = [
    "KNOWN_HOST_KEY_TYPES",
    "SshEndpointError",
    "SshRepositoryTarget",
    "known_hosts_field_matches",
    "normalize_ssh_host",
    "normalize_ssh_port",
    "normalize_ssh_user",
    "parse_known_hosts",
    "parse_ssh_repository_url",
]
