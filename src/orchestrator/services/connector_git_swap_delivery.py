"""Whether the git swap driver can serve a token repository now (C3).

At bind a token repository whose URL the driver can serve (HTTPS on port
443) becomes a swap *candidate*: its payload entry keeps its URL as stored
and its forge token (the one credential a lease driver's entry carries for
the agent process), plus an empty ``git_swap`` block. The lease step
decides per entry, in the delivery's own transaction
(:func:`connector_credential_leases.deliver_connector_leases`), and never
refuses a claim, an attach or a dispatch for it:

* the workspace must reach the driver namespace: a container workspace or a
  same-cluster VM, not a VM in another cluster nor a static-pool host;
* the connector's last driver pod must not have failed to start within the
  launch back-off (refused, capacity, a start timeout, or its upstream
  unreachable or untrusted, which the driver reports at start);
* the installation must have room for a pod when the connector has none;
* the upstream must pass the reconciler's own egress check for the
  connector's project tier, and its certificate must verify against public
  roots or the connector's ``upstream_ca`` (both checked before the
  transaction, per host, and remembered for :attr:`GitSwapDeliverySettings.
  verdict_seconds`);
* the driver's image must resolve.

Otherwise the installation's fallback applies to that entry alone, and
visibly: ``token-in-url`` keeps the entry (the agent clones with the token
in the URL, as before C3) and sets ``git_swap: {"fallback": <why>}``,
which the workspace README states; ``refuse`` delivers no credential and
``git_swap: {"unavailable": <why>}``. Each connector's Test reports the
same verdict (all but the workspace's reach) and probes the upstream's TLS
without a credential. An installation without the driver changes nothing.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git
swap driver"; the C3 review (B1).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import ssl
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from orchestrator.services.connector_egress import (
    DEFAULT_CLUSTER_CIDRS,
    DEFAULT_PRIVATE_TIERS,
    EgressPolicy,
    EgressRefused,
    pin_egress,
    private_addresses_allowed,
    system_resolver,
)
from shared.connectors.builtin import GIT_SWAP_SPEC, git_swap_entry
from shared.connectors.git_swap import (
    FALLBACK_REFUSE,
    FALLBACK_TOKEN_IN_URL,
    UPSTREAM_PORT,
    UnservedUpstream,
    swap_upstream,
)

logger = logging.getLogger(__name__)

#: The driver pod stops that make a connector unservable for the back-off
#: (``connector_service_hosting``'s reasons; ``upstream_unreachable`` is the
#: driver's own report at start).
UNSERVABLE_STOPS = (
    "launch_refused",
    "launch_failed",
    "capacity",
    "start_timeout",
    "upstream_unreachable",
)
#: The most an upstream check may hold up a delivery that found no answer
#: prepared before its transaction.
INLINE_CHECK_SECONDS = 4.0
#: The upstream CA a repository connector may carry: PEM certificates only.
MAX_UPSTREAM_CA_BYTES = 64 * 1024
#: The shortest forge token the driver uses (drivers/git-swap
#: minCredentialLength).
MIN_TOKEN_LENGTH = 16


@dataclass(frozen=True)
class GitSwapDeliverySettings:
    """What the per-entry decision needs from the installation.

    ``installed`` is whether ``srw.git-swap/v1`` is installed; ``store`` the
    application's database (Test reads the launch state on it); the hosting
    fields mirror ``ServiceHostingSettings`` (the reconciler's own egress
    rule and its cap and back-off); ``vm_on_pod_network`` says whether VM
    workspaces run in this cluster.
    """

    installed: bool = False
    fallback: str = FALLBACK_TOKEN_IN_URL
    store: Any = None
    max_installation: int = 10
    launch_backoff_seconds: float = 300.0
    cluster_cidrs: tuple[str, ...] = DEFAULT_CLUSTER_CIDRS
    refused_cidrs: tuple[str, ...] = ()
    private_tiers: frozenset[str] = DEFAULT_PRIVATE_TIERS
    ipv6: bool = False
    vm_on_pod_network: Callable[[], bool] = lambda: False
    resolver: Callable[[str, bool], Any] = system_resolver
    verdict_seconds: float = 300.0
    probe_timeout_seconds: float = 5.0
    clock: Callable[[], float] = field(default=time.monotonic)


_state: dict[str, Any] = {"settings": GitSwapDeliverySettings()}
#: (host, upstream CA digest, private allowed) -> (expiry, problem or None)
_verdicts: dict[tuple[str, str, bool], tuple[float, str | None]] = {}
#: (connector, reason) already logged: every claim of a session decides again.
_NOTED: set[tuple[str, str]] = set()
_NOTED_MAX = 1024


def configure_git_swap_delivery(settings: GitSwapDeliverySettings) -> None:
    """Install the settings and forget every remembered verdict."""
    _state["settings"] = settings
    _verdicts.clear()
    _NOTED.clear()


def git_swap_delivery_settings() -> GitSwapDeliverySettings:
    return _state["settings"]


# =============================================================================
# The upstream: egress and TLS
# =============================================================================


def upstream_ca_of(config: Any) -> str | None:
    """The connector's upstream CA (PEM), if it names one."""
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except ValueError:
            return None
    if not isinstance(config, Mapping):
        return None
    value = config.get("upstream_ca")
    return value if isinstance(value, str) and value.strip() else None


def _ca_digest(ca_pem: str | None) -> str:
    return hashlib.sha256((ca_pem or "").encode()).hexdigest()[:16]


async def probe_upstream_tls(
    host: str,
    address: str,
    *,
    ca_pem: str | None = None,
    timeout: float = 5.0,
    port: int = UPSTREAM_PORT,
) -> str | None:
    """A TLS handshake with the upstream at ``address`` (no credential, no
    request): ``None`` when its certificate verifies for ``host`` against
    public roots, or only ``ca_pem`` when the connector names one; else why
    not. An upstream that does not answer is reported as such."""
    try:
        context = (
            ssl.create_default_context(cadata=ca_pem)
            if ca_pem
            else ssl.create_default_context()
        )
    except (ssl.SSLError, ValueError) as exc:
        return f"its upstream CA is not usable ({exc})"
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port, ssl=context, server_hostname=host),
            timeout,
        )
    except ssl.SSLCertVerificationError as exc:
        trust = "its upstream CA" if ca_pem else "public roots"
        hint = "" if ca_pem else "; set the connector's upstream CA"
        return (
            f"the certificate of {host} does not verify against {trust} "
            f"({exc.verify_message or exc.reason}){hint}"
        )
    except (OSError, ssl.SSLError, asyncio.TimeoutError) as exc:
        return f"{host} did not complete a TLS handshake ({type(exc).__name__})"
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 2)
    except Exception:
        pass
    return None


def _definite(problem: str | None) -> bool:
    """Whether an upstream problem decides delivery. A handshake that did
    not complete from the orchestrator's network says little about the
    driver's (its pod reports that itself, at start): only an egress refusal
    and a certificate that does not verify do."""
    return problem is not None and "did not complete a TLS handshake" not in problem


async def check_upstream(
    host: str, *, ca_pem: str | None, private_allowed: bool
) -> str | None:
    """The reconciler's egress check for the upstream host, then its TLS;
    the problem, or ``None``."""
    settings = git_swap_delivery_settings()
    policy = EgressPolicy.build(
        settings.cluster_cidrs,
        allow_private=private_allowed,
        ipv6=settings.ipv6,
        refused_cidrs=settings.refused_cidrs,
    )
    try:
        pins = await pin_egress(
            GIT_SWAP_SPEC.egress,
            {"host": host},
            policy=policy,
            resolver=settings.resolver,
        )
    except EgressRefused as exc:
        return f"its driver may not reach the upstream: {exc}"
    address = pins.hosts[0].addresses[0]
    return await probe_upstream_tls(
        host, address, ca_pem=ca_pem, timeout=settings.probe_timeout_seconds
    )


def _remembered(key: tuple[str, str, bool]) -> tuple[bool, str | None]:
    settings = git_swap_delivery_settings()
    found = _verdicts.get(key)
    if found is None or found[0] <= settings.clock():
        return False, None
    return True, found[1]


def _remember(key: tuple[str, str, bool], problem: str | None) -> None:
    settings = git_swap_delivery_settings()
    if len(_verdicts) > 4096:
        _verdicts.clear()
    _verdicts[key] = (settings.clock() + settings.verdict_seconds, problem)


async def upstream_verdict(
    host: str, *, ca_pem: str | None, private_allowed: bool, fresh: bool = False
) -> str | None:
    """:func:`check_upstream`, remembered per host, CA and tier."""
    key = (host, _ca_digest(ca_pem), private_allowed)
    if not fresh:
        known, problem = _remembered(key)
        if known:
            return problem
    problem = await check_upstream(host, ca_pem=ca_pem, private_allowed=private_allowed)
    _remember(key, problem)
    return problem


def _candidates(entries: Any) -> list[tuple[dict[str, Any], str]]:
    found = []
    for entry in entries or ():
        if not isinstance(entry, dict) or not git_swap_entry(entry):
            continue
        connector_id = str(entry.get("datasource_id") or "")
        try:
            UUID(connector_id)
        except ValueError:
            continue
        found.append((entry, connector_id))
    return found


async def prepare_git_swap_delivery(store: Any, entries: Any) -> None:
    """Check every candidate's upstream before the caller's transaction
    opens (DNS and a TLS handshake: network the delivery must not wait on).
    Never raises: the delivery decides with whatever was found."""
    settings = git_swap_delivery_settings()
    if not settings.installed:
        return
    for entry, connector_id in _candidates(entries):
        try:
            upstream = swap_upstream(entry.get("connection_url"))
            async with store.acquire() as conn:
                private = await private_addresses_allowed(
                    conn, connector_id, private_tiers=settings.private_tiers
                )
            await upstream_verdict(
                upstream.host,
                ca_pem=upstream_ca_of(entry.get("config")),
                private_allowed=private,
            )
        except UnservedUpstream:
            continue
        except Exception:
            logger.warning(
                "Checking the upstream of repository connector %s failed",
                connector_id,
                exc_info=True,
            )


# =============================================================================
# The workspace and the driver's pods
# =============================================================================


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def workspace_reach_problem(backend: str | None, provisioner: str | None) -> str | None:
    """Whether a workspace of ``backend`` reaches the driver namespace."""
    if backend == "sandbox":
        if str(provisioner or "").lower() == "docker":
            return (
                "its workspace is a static-pool host outside the cluster, which "
                "cannot reach the driver"
            )
        return None
    if backend == "vm":
        if git_swap_delivery_settings().vm_on_pod_network():
            return None
        return "its workspace is a VM in another cluster, which cannot reach the driver"
    return f"its workspace ({backend or 'unknown backend'}) cannot reach the driver"


async def owner_workspace_problem(conn: Any, owner: Any) -> str | None:
    """:func:`workspace_reach_problem` for a lease owner's workspace, read
    from its row (a child Job's owner is its parent: its workspace)."""
    if owner.kind == "job":
        from shared.workspace_contract import (
            WorkspaceContractError,
            resolve_workspace_contract,
        )

        row = await conn.fetchrow(
            "SELECT context, config_override FROM jobs WHERE id = $1::uuid",
            UUID(owner.id),
        )
        if row is None:
            return "its job is gone"
        job = {"context": row["context"], "config_override": row["config_override"]}
        try:
            backend = resolve_workspace_contract(job).assigned_backend
        except WorkspaceContractError:
            backend = None
        container = _json_object(job["context"]).get("workspace_container")
    else:
        from orchestrator.services.stateless_workspace_gate import (
            declared_thread_workspace_backend,
        )

        row = await conn.fetchrow(
            "SELECT metadata FROM threads WHERE id = $1::uuid", UUID(owner.id)
        )
        if row is None:
            return "its session is gone"
        backend = declared_thread_workspace_backend({"metadata": row["metadata"]})
        container = _json_object(row["metadata"]).get("workspace_container")
    provisioner = (
        container.get("provisioner") if isinstance(container, Mapping) else None
    )
    return workspace_reach_problem(backend, provisioner)


async def launch_problem(conn: Any, connector_id: str) -> str | None:
    """Why the connector's driver pod cannot be counted on now: its last pod
    stopped without serving (within the back-off, and since the connector
    last changed), or the installation has no room for one."""
    settings = git_swap_delivery_settings()
    serving = await conn.fetchval(
        """
        SELECT 1 FROM connector_driver_identities
         WHERE connector_id = $1 AND driver = $2
           AND credential_generation IS NOT NULL AND revoked_at IS NULL
         LIMIT 1
        """,
        UUID(connector_id),
        GIT_SWAP_SPEC.name,
    )
    if serving:
        return None
    stopped = await conn.fetchrow(
        """
        SELECT identity.revoke_reason, identity.launch_error
          FROM connector_driver_identities AS identity
          JOIN datasources AS ds ON ds.id = identity.connector_id
         WHERE identity.connector_id = $1 AND identity.driver = $2
           AND identity.credential_generation IS NOT NULL
           AND identity.revoke_reason = ANY($3::text[])
           AND identity.revoked_at > now() - make_interval(secs => $4::float8)
           AND identity.revoked_at >= COALESCE(ds.updated_at, '-infinity')
         ORDER BY identity.revoked_at DESC
         LIMIT 1
        """,
        UUID(connector_id),
        GIT_SWAP_SPEC.name,
        list(UNSERVABLE_STOPS),
        float(settings.launch_backoff_seconds),
    )
    if stopped is not None:
        detail = " ".join(str(stopped["launch_error"] or "").split())[:300]
        return (
            f"its driver pod did not start ({stopped['revoke_reason']}"
            + (f": {detail}" if detail else "")
            + ")"
        )
    live = await conn.fetchval(
        "SELECT count(*) FROM connector_driver_identities "
        "WHERE credential_generation IS NOT NULL AND removed_at IS NULL"
    )
    if int(live or 0) >= settings.max_installation:
        return (
            f"the installation already runs its cap of {settings.max_installation} "
            "driver pods (connectors.servicePods.maxInstallation)"
        )
    return None


async def _upstream_problem(
    conn: Any, entry: Mapping[str, Any], connector_id: str
) -> str | None:
    """The remembered upstream verdict, or one decided now within
    :data:`INLINE_CHECK_SECONDS` (a delivery path that prepared nothing); a
    check that does not finish in time decides nothing."""
    settings = git_swap_delivery_settings()
    upstream = swap_upstream(entry.get("connection_url"))
    private = await private_addresses_allowed(
        conn, connector_id, private_tiers=settings.private_tiers
    )
    ca_pem = upstream_ca_of(entry.get("config"))
    known, problem = _remembered((upstream.host, _ca_digest(ca_pem), private))
    if not known:
        try:
            problem = await asyncio.wait_for(
                asyncio.create_task(
                    upstream_verdict(
                        upstream.host, ca_pem=ca_pem, private_allowed=private
                    )
                ),
                INLINE_CHECK_SECONDS,
            )
        except (asyncio.TimeoutError, TimeoutError):
            return None
    return problem if _definite(problem) else None


async def git_swap_problem(
    conn: Any, entry: Mapping[str, Any], *, connector_id: str, owner: Any
) -> str | None:
    """Why the driver cannot serve this candidate for ``owner`` now, or
    ``None`` (the image is the lease step's own check)."""
    from orchestrator.services.connector_driver_ca import driver_ca
    from orchestrator.services.connector_service_images import service_image_settings

    settings = git_swap_delivery_settings()
    if not settings.installed or not service_image_settings().service_namespace:
        return "the git swap driver is not installed (connectors.drivers.gitSwap)"
    if driver_ca() is None:
        return "SRW's connector driver certificate authority is not loaded"
    try:
        swap_upstream(entry.get("connection_url"))
    except UnservedUpstream as exc:
        return f"the git swap driver cannot serve it: {exc}"
    token = (entry.get("credentials") or {}).get("token")
    if not isinstance(token, str) or len(token) < MIN_TOKEN_LENGTH:
        # The driver refuses it (it masks the token in every answer).
        return (
            f"its token is shorter than {MIN_TOKEN_LENGTH} characters, which the "
            "git swap driver does not use"
        )
    for check in (
        lambda: owner_workspace_problem(conn, owner),
        lambda: launch_problem(conn, connector_id),
        lambda: _upstream_problem(conn, entry, connector_id),
    ):
        problem = await check()
        if problem is not None:
            return problem
    return None


# =============================================================================
# The entry
# =============================================================================


def serve_through_swap(entry: dict[str, Any]) -> None:
    """Turn a candidate into the driver's entry: its clean upstream URL (the
    lease step then keeps only the forge token, for the agent process, and
    adds the lease and the block)."""
    upstream = swap_upstream(entry.get("connection_url"))
    entry["connection_url"] = upstream.url


def apply_fallback(
    entry: dict[str, Any], reason: str, *, fallback: str | None = None
) -> None:
    """Put a candidate on the installation's fallback, saying why."""
    mode = fallback or git_swap_delivery_settings().fallback
    connector = str(entry.get("datasource_id") or entry.get("name") or "?")
    refused = mode == FALLBACK_REFUSE
    note = (connector, reason)
    if note not in _NOTED:
        if len(_NOTED) >= _NOTED_MAX:
            _NOTED.clear()
        _NOTED.add(note)
        logger.warning(
            "Repository connector %s %s: %s (connectors.drivers.gitSwap.fallback=%s)",
            connector,
            "is not delivered" if refused else "delivers its token in the clone URL",
            reason,
            mode,
        )
    if refused:
        entry["credentials"] = {}
        entry["git_swap"] = {
            "unavailable": f"{reason}; this installation refuses token-in-URL delivery"
        }
    else:
        entry["git_swap"] = {"fallback": reason}


# =============================================================================
# Test
# =============================================================================


async def delivery_report(
    row: Mapping[str, Any], *, token: str | None = None
) -> dict[str, Any] | None:
    """How a token repository is delivered on this installation, for its
    Test: through the driver, or the fallback and why. Probes the upstream's
    TLS afresh (no credential) and remembers the verdict. ``None`` when the
    driver is not installed. The workspace's own reach is decided per
    delivery: container and same-cluster VM workspaces only. ``token`` is
    only measured, never sent."""
    settings = git_swap_delivery_settings()
    if not settings.installed:
        return None
    connector_id = str(row.get("id") or "")
    report: dict[str, Any] = {"driver": GIT_SWAP_SPEC.name}
    try:
        upstream = swap_upstream(row.get("connection_url"))
    except UnservedUpstream as exc:
        problem: str | None = f"the git swap driver cannot serve it: {exc}"
        upstream = None
    else:
        problem = None
    ca_pem = upstream_ca_of(row.get("config"))
    if upstream is not None and settings.store is not None:
        try:
            async with settings.store.acquire() as conn:
                private = await private_addresses_allowed(
                    conn, connector_id, private_tiers=settings.private_tiers
                )
                launch = await launch_problem(conn, connector_id)
            tls = await upstream_verdict(
                upstream.host, ca_pem=ca_pem, private_allowed=private, fresh=True
            )
        except Exception as exc:
            logger.warning("Git swap delivery report failed", exc_info=True)
            return {**report, "mode": "unknown", "reason": type(exc).__name__}
        report["upstream_tls"] = tls or (
            "verified against the connector's upstream CA"
            if ca_pem
            else "verified against public roots"
        )
        problem = (tls if _definite(tls) else None) or launch
    if problem is None and token is not None and len(token) < MIN_TOKEN_LENGTH:
        problem = (
            f"its token is shorter than {MIN_TOKEN_LENGTH} characters, which the "
            "git swap driver does not use"
        )
    if problem is None:
        report.update(mode="git-swap", reason="")
    elif settings.fallback == FALLBACK_REFUSE:
        report.update(mode="refused", reason=problem)
    else:
        report.update(mode=FALLBACK_TOKEN_IN_URL, reason=problem)
    return report


def describe(report: Mapping[str, Any]) -> str:
    """One sentence for Test's message."""
    mode = report.get("mode")
    if mode == "git-swap":
        return (
            "delivered through SRW's git swap driver (the workspace holds a lease, "
            "never the token) on container and same-cluster VM workspaces"
        )
    if mode == "refused":
        return f"NOT delivered (this installation refuses token-in-URL): {report.get('reason')}"
    if mode == FALLBACK_TOKEN_IN_URL:
        return (
            "delivered with the token in its clone URL, NOT through SRW's git swap "
            f"driver: {report.get('reason')}"
        )
    return f"delivery could not be assessed ({report.get('reason')})"


def validate_upstream_ca(value: Any) -> str | None:
    """A connector's ``upstream_ca``: PEM certificates (normalised), or
    ``None`` for none. Raises ``ValueError`` saying what is wrong."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise ValueError("upstream_ca is PEM text")
    text = value.strip() + "\n"
    if len(text.encode()) > MAX_UPSTREAM_CA_BYTES:
        raise ValueError("upstream_ca is larger than 64 KiB")
    from cryptography import x509

    try:
        certificates = x509.load_pem_x509_certificates(text.encode())
    except ValueError as exc:
        raise ValueError(f"upstream_ca is not PEM certificates ({exc})") from None
    if not certificates:
        raise ValueError("upstream_ca holds no certificate")
    if "PRIVATE KEY" in text:
        raise ValueError("upstream_ca holds a private key; it takes certificates only")
    return text


__all__ = [
    "GitSwapDeliverySettings",
    "INLINE_CHECK_SECONDS",
    "UNSERVABLE_STOPS",
    "apply_fallback",
    "check_upstream",
    "configure_git_swap_delivery",
    "delivery_report",
    "describe",
    "git_swap_delivery_settings",
    "git_swap_problem",
    "launch_problem",
    "owner_workspace_problem",
    "prepare_git_swap_delivery",
    "probe_upstream_tls",
    "serve_through_swap",
    "upstream_ca_of",
    "upstream_verdict",
    "validate_upstream_ca",
    "workspace_reach_problem",
]
