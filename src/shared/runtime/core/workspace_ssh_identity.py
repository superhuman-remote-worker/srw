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
import shlex
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, MutableMapping
from urllib.parse import urlparse
from uuid import UUID

from shared.runtime.core.managed_repository import (
    _SSH_AGENT_RETIRE_PROGRAM,
    RESERVED_SSH_HOST_PREFIX,
    ManagedRepositoryMaterializationError,
    _backend_managed_home,
    _backend_runtime_authority,
    _execute_managed_secret_command,
    managed_repository_agent_launch_command,
    managed_repository_agent_retirement_command,
    managed_ssh_namespace_setup_command,
    render_ssh_identity_config,
)
from shared.runtime.utils.ssh_key import normalize_private_key

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
    if text.lower().startswith(RESERVED_SSH_HOST_PREFIX):
        # A declared host becomes a ``Host`` line in a shared config. Named
        # like an identity alias, and read first, it would take over that
        # alias's agent and host-key settings in every workspace.
        raise SshEndpointError(
            f"SSH host must not start with {RESERVED_SSH_HOST_PREFIX!r}, "
            "which names workspace identities"
        )
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
    ``port``. With ``require_match=False`` (a deployment-wide default list)
    only full lines naming ``host`` count: a line for another host, and a bare
    pair that names no host at all, are skipped instead of refused. Markers,
    certificates and unknown key types are refused, so the rendered
    per-identity file holds nothing but plain keys under the identity's own
    alias.
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
            if not require_match:
                continue
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


# ---------------------------------------------------------------------------
# Workspace materialization
# ---------------------------------------------------------------------------

WORKSPACE_SSH_IDENTITY_VERSION = 1
WORKSPACE_SSH_IDENTITY_KINDS = frozenset({"repository", "ssh_key"})
#: Status of an identity the workspace agent holds and has proven.
IDENTITY_READY = "ready"
_FINGERPRINT = re.compile(r"SHA256:[A-Za-z0-9+/]{43}")
_MAX_EXTRA_HOSTS = 8


def workspace_ssh_identity_alias(authority_id: Any) -> str:
    """The opaque ``Host`` alias of one identity: ``srw-repo-<32hex>``."""

    return f"srw-repo-{UUID(str(authority_id)).hex}"


def workspace_ssh_identity_socket(home_path: str, authority_id: Any) -> str:
    """The identity's ``ssh-agent`` socket inside the managed namespace."""

    return (
        f"{home_path.rstrip('/')}/.ssh/srw-managed/sockets/"
        f"{UUID(str(authority_id)).hex}.sock"
    )


class WorkspaceSshIdentityError(RuntimeError):
    """One identity could not be loaded; ``code`` is credential-free."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _validated_identity(payload: Mapping[str, Any]) -> dict[str, Any]:
    """Re-check one internal payload; the orchestrator is not trusted blindly.

    Every value that reaches the rendered SSH config must already be in its
    normalized form, so nothing the agent writes differs from what was
    validated at connector create/update and again at dispatch.
    """

    invalid = WorkspaceSshIdentityError("workspace_ssh_identity_invalid")
    try:
        if int(payload.get("version")) != WORKSPACE_SSH_IDENTITY_VERSION:
            raise ValueError
        authority_id = str(UUID(str(payload["authority_id"])))
        generation = payload["generation"]
        kind = str(payload["kind"])
        alias = str(payload["alias"])
        fingerprint = str(payload["public_key_fingerprint"])
        strict = payload.get("strict_host_key_checking")
        known_hosts = list(payload.get("known_hosts") or [])
        extra_hosts = list(payload.get("extra_hosts") or [])
    except (KeyError, TypeError, ValueError) as exc:
        raise invalid from exc
    if (
        isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
        or kind not in WORKSPACE_SSH_IDENTITY_KINDS
        or alias != workspace_ssh_identity_alias(authority_id)
        or not _FINGERPRINT.fullmatch(fingerprint)
        or not isinstance(strict, bool)
        or (strict and not known_hosts)
        or len(extra_hosts) > _MAX_EXTRA_HOSTS
        or (kind == "repository" and extra_hosts)
    ):
        raise invalid
    host = payload.get("ssh_host")
    port = payload.get("ssh_port")
    user = payload.get("ssh_user")
    try:
        if host is not None and normalize_ssh_host(host) != host:
            raise invalid
        if port is not None and normalize_ssh_port(port) != port:
            raise invalid
        if user is not None and normalize_ssh_user(user) != user:
            raise invalid
        if any(normalize_ssh_host(value) != value for value in extra_hosts):
            raise invalid
        if any(
            not isinstance(entry, str) or parse_known_hosts(entry) != [entry]
            for entry in known_hosts
        ):
            raise invalid
    except SshEndpointError as exc:
        raise invalid from exc
    if kind == "repository" and (host is None or port is None or user is None):
        raise invalid
    if host is None and (port is not None or user is not None or known_hosts):
        raise invalid
    try:
        normalized_key = normalize_private_key(str(payload["private_key"]))
        if not normalized_key:
            raise ValueError
        private_key = bytearray(normalized_key.encode("utf-8"))
        del normalized_key
    except (KeyError, TypeError, ValueError) as exc:
        raise invalid from exc
    return {
        "authority_id": authority_id,
        "generation": generation,
        "kind": kind,
        "alias": alias,
        "ssh_host": host,
        "ssh_port": port,
        "ssh_user": user,
        "extra_hosts": extra_hosts,
        "known_hosts": known_hosts,
        "strict": strict,
        "private_key": private_key,
        "public_key_fingerprint": fingerprint,
    }


def _wipe(value: Any) -> None:
    if isinstance(value, bytearray):
        value[:] = b"\x00" * len(value)


def _identity_command(
    item: Mapping[str, Any],
    *,
    home_path: str,
    runtime_workspace_generation: str | None,
    runtime_incarnation: str | None,
) -> str:
    slug = UUID(item["authority_id"]).hex
    root = f"{home_path}/.ssh/srw-managed"
    known_hosts_path = f"{root}/known_hosts.d/{slug}"
    config = render_ssh_identity_config(
        alias=item["alias"],
        socket_path=workspace_ssh_identity_socket(home_path, item["authority_id"]),
        known_hosts_path=known_hosts_path,
        host=item["ssh_host"],
        port=item["ssh_port"],
        user=item["ssh_user"],
        strict_host_key_checking=item["strict"],
        host_key_alias=item["alias"],
        extra_hosts=item["extra_hosts"],
    )
    if item["strict"]:
        # A pin replaces whatever the file learned or was pinned to before.
        # Keys are filed under the alias (``HostKeyAlias``), so the pin holds
        # however the host is spelled and cannot vouch for another identity.
        content = "".join(f"{item['alias']} {entry}\n" for entry in item["known_hosts"])
        publish_known_hosts = (
            f"printf %s {shlex.quote(content)} > "
            f"{shlex.quote(known_hosts_path)}.tmp.$$; "
            f"mv -f -- {shlex.quote(known_hosts_path)}.tmp.$$ "
            f"{shlex.quote(known_hosts_path)}; "
        )
    else:
        # Trust on first use, per identity: keep what an earlier load learned.
        publish_known_hosts = f"touch {shlex.quote(known_hosts_path)}; "
    launch = managed_repository_agent_launch_command(
        home_path=home_path,
        authority_id=item["authority_id"],
        generation=int(item["generation"]),
        preserve_existing=True,
        expected_fingerprint=item["public_key_fingerprint"],
        workspace_generation=runtime_workspace_generation,
        runtime_incarnation=runtime_incarnation,
        config_content=config,
    )
    return "set -eu; umask 077; " + publish_known_hosts + launch


def materialize_workspace_ssh_identities(
    payloads: Iterable[Mapping[str, Any]] | None,
    backend: Any,
) -> dict[str, str]:
    """Load each connector identity into its own workspace ``ssh-agent``.

    Returns ``{authority_id: status}``: :data:`IDENTITY_READY`, or a
    credential-free error code for that identity alone. Unlike the managed
    repository materializer, nothing here fails the attach: an external
    connector's broken key, or a forge outage, degrades only its connector,
    and the clone that follows is that connector's probe. Every private key is
    popped from the caller's payloads and zeroed before this returns.
    """

    raw = list(payloads or [])
    if not raw:
        return {}
    result: dict[str, str] = {}
    validated: list[dict[str, Any]] = []
    try:
        for item in raw:
            try:
                validated.append(_validated_identity(item))
            except WorkspaceSshIdentityError as exc:
                authority = (
                    str(item.get("authority_id") or "")
                    if isinstance(item, Mapping)
                    else ""
                )
                result.setdefault(authority or f"invalid-{len(result)}", exc.code)
            finally:
                if isinstance(item, MutableMapping):
                    item.pop("private_key", None)
        unique: list[dict[str, Any]] = []
        for item in validated:
            if item["authority_id"] in result or any(
                other["authority_id"] == item["authority_id"] for other in unique
            ):
                result[item["authority_id"]] = "workspace_ssh_identity_duplicate"
                continue
            unique.append(item)
        if not unique:
            return result
        if not getattr(backend, "supports_shell", False):
            for item in unique:
                result[item["authority_id"]] = (
                    "workspace_ssh_identity_requires_workspace"
                )
            return result
        try:
            home_path = _backend_managed_home(backend)
            runtime_workspace_generation, runtime_incarnation = (
                _backend_runtime_authority(backend)
            )
            setup_ok = _execute_managed_secret_command(
                backend,
                managed_ssh_namespace_setup_command(home_path=home_path),
                b"",
                timeout=15,
                operation="connector SSH identity materialization",
            )
        except (ManagedRepositoryMaterializationError, NotImplementedError, OSError):
            setup_ok = False
        if not setup_ok:
            for item in unique:
                result[item["authority_id"]] = (
                    "workspace_ssh_identity_materialization_failed"
                )
            return result
        for item in unique:
            private_key = item.pop("private_key")
            try:
                command = _identity_command(
                    item,
                    home_path=home_path,
                    runtime_workspace_generation=runtime_workspace_generation,
                    runtime_incarnation=runtime_incarnation,
                )
                loaded = _execute_managed_secret_command(
                    backend,
                    command,
                    private_key,
                    timeout=30,
                    operation="connector SSH identity materialization",
                )
            except (
                ManagedRepositoryMaterializationError,
                NotImplementedError,
                OSError,
            ):
                loaded = False
            finally:
                _wipe(private_key)
                del private_key
            result[item["authority_id"]] = (
                IDENTITY_READY if loaded else "workspace_ssh_identity_load_failed"
            )
        return result
    finally:
        for item in raw:
            if isinstance(item, MutableMapping):
                item.pop("private_key", None)
        for item in validated:
            _wipe(item.pop("private_key", None))


def retire_workspace_ssh_identities(authority_ids: Iterable[str], backend: Any) -> bool:
    """Retire exactly these identities' agents, sparing every other resident.

    Used when a connector is detached from a live session, which owns its
    workspace. Its socket, receipt, config and host-key file go with it; the
    terminal owners that retire the whole namespace need no help.
    """

    slugs = sorted({UUID(str(value)).hex for value in authority_ids})
    if not slugs:
        return True
    if not getattr(backend, "supports_shell", False):
        return False
    try:
        home_path = _backend_managed_home(backend)
        runtime_workspace_generation, runtime_incarnation = _backend_runtime_authority(
            backend
        )
        command = managed_repository_agent_retirement_command(
            home_path=home_path,
            authority_ids=slugs,
            remove_configs=True,
            workspace_generation=runtime_workspace_generation,
            runtime_incarnation=runtime_incarnation,
        ).rstrip()
        command += " " + "".join(
            "rm -f -- "
            + shlex.quote(f"{home_path}/.ssh/srw-managed/known_hosts.d/{slug}")
            + "; "
            for slug in slugs
        )
        return _execute_managed_secret_command(
            backend,
            command,
            b"",
            timeout=30,
            operation="connector SSH identity retirement",
        )
    except Exception:  # best effort: every terminal owner retires the rest
        return False


def prune_workspace_ssh_identities(
    keep_authority_ids: Iterable[str], backend: Any
) -> bool:
    """Retire every connector identity this home holds except ``keep``.

    For a session, which owns its workspace: a stateless session applies a
    connector edit at its next claim's attach, never through the live
    detach, so the attach itself must retire what is no longer delivered.
    Only identities this module loaded are candidates. They are the ones
    with a ``known_hosts.d/<slug>`` file, which managed repositories and the
    IDE never write, so a managed agent in the same namespace is never
    touched. Never used for jobs, whose workspace a root and its children
    share.
    """

    keep = sorted({UUID(str(value)).hex for value in keep_authority_ids})
    if not getattr(backend, "supports_shell", False):
        return False
    try:
        home_path = _backend_managed_home(backend)
        root = f"{home_path}/.ssh/srw-managed"
        retire_one = " ".join(
            [
                "python3",
                "-c",
                shlex.quote(_SSH_AGENT_RETIRE_PROGRAM),
                "exact",
                '"$_srw_root/sockets/$_srw_slug.sock"',
            ]
        )
        command = (
            "set -eu; "
            f"_srw_root={shlex.quote(root)}; "
            f"_srw_keep=' {' '.join(keep)} '; "
            'test -d "$_srw_root/known_hosts.d" || exit 0; '
            'for _srw_path in "$_srw_root"/known_hosts.d/*; do '
            'test -e "$_srw_path" || continue; '
            "_srw_slug=${_srw_path##*/}; "
            'case "$_srw_slug" in *[!0-9a-f]*) continue;; esac; '
            'test "${#_srw_slug}" -eq 32 || continue; '
            'case "$_srw_keep" in *" $_srw_slug "*) continue;; esac; '
            f"{retire_one}; "
            'rm -f -- "$_srw_root/sockets/$_srw_slug.sock" '
            '"$_srw_root/agents/$_srw_slug.state" '
            '"$_srw_root/config.d/$_srw_slug.conf" "$_srw_path"; '
            "done"
        )
        return _execute_managed_secret_command(
            backend,
            command,
            b"",
            timeout=30,
            operation="connector SSH identity retirement",
        )
    except Exception:  # best effort: every terminal owner retires the rest
        return False


__all__ = [
    "IDENTITY_READY",
    "KNOWN_HOST_KEY_TYPES",
    "SshEndpointError",
    "SshRepositoryTarget",
    "WORKSPACE_SSH_IDENTITY_KINDS",
    "WORKSPACE_SSH_IDENTITY_VERSION",
    "WorkspaceSshIdentityError",
    "known_hosts_field_matches",
    "materialize_workspace_ssh_identities",
    "normalize_ssh_host",
    "normalize_ssh_port",
    "normalize_ssh_user",
    "parse_known_hosts",
    "parse_ssh_repository_url",
    "prune_workspace_ssh_identities",
    "retire_workspace_ssh_identities",
    "workspace_ssh_identity_alias",
    "workspace_ssh_identity_socket",
]
