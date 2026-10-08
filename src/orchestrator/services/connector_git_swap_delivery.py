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
* where the connector has no serving pod, the upstream must pass the
  reconciler's own egress check for the connector's project tier, and its
  certificate must verify against public roots or the connector's
  ``upstream_ca``. Only a definite answer decides: an egress refusal of the
  addresses the host resolves to, or a certificate that does not verify. A
  host that does not resolve, or does not complete a handshake, from the
  orchestrator decides nothing (the driver reports its own reach at start).
  Both are checked before the transaction (:func:`prepare_git_swap_delivery`),
  per host, CA and tier: an answer that serves is remembered for
  :attr:`GitSwapDeliverySettings.verdict_seconds`, a refusal or no answer
  only briefly;
* the driver's image must resolve, and the token must be one the driver
  uses.

Otherwise the installation's fallback applies to that entry alone, and
visibly: ``token-in-url`` keeps the entry (the agent clones with the token
in the URL, as before C3) and sets ``git_swap: {"fallback": <why>}``,
which the workspace README states; ``refuse`` delivers no credential and
``git_swap: {"unavailable": <why>}``. The lease the owner held for that
connector is revoked, so a workspace wired in an earlier attach stops
using the driver too. ``<why>`` is one of a fixed set of reasons
(:data:`REASONS`): nothing the upstream controls (a certificate's names,
an error's text) reaches the README or Test; the raw detail goes to the
server log only, cleaned. Each connector's Test reports the same verdict
(all but the workspace's reach) and probes the upstream's TLS without a
credential. An installation without the driver changes nothing, but on a
``refuse`` installation Test says the repository is refused.

Design: knowledge-base/knowledge/features/connector_drivers.md, "The git
swap driver"; the C3 reviews (B1, the re-review's B2, S5, S8).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import ssl
import time
import unicodedata
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
#: prepared before its transaction: past it, nothing is decided.
INLINE_CHECK_SECONDS = 4.0
#: How long a refusal, or no answer, is remembered (an answer that serves
#: is remembered for ``verdict_seconds``): a DNS blip or a tarpit must not
#: put every repository of a host on the fallback for minutes.
BRIEF_VERDICT_SECONDS = 30.0
#: The upstream CA a repository connector may carry: PEM certificates only.
MAX_UPSTREAM_CA_BYTES = 64 * 1024
#: The shortest forge token the driver uses (drivers/git-swap
#: minCredentialLength).
MIN_TOKEN_LENGTH = 16
#: The longest raw detail kept (server log, ``launch_error``).
MAX_DETAIL = 300

#: Why a token repository is not served through the driver: the only text
#: the README and Test show (S5).
REASONS: dict[str, str] = {
    "not_installed": "the git swap driver is not installed",
    "no_authority": "SRW's connector driver certificate authority is not loaded",
    "url_not_served": "the git swap driver serves HTTPS repositories on port 443 only",
    "token_too_short": (
        f"its token is shorter than {MIN_TOKEN_LENGTH} characters, which the git "
        "swap driver does not use"
    ),
    "workspace_static_pool": (
        "its workspace is a static-pool host outside the cluster, which cannot "
        "reach the driver"
    ),
    "workspace_remote_vm": (
        "its workspace is a VM in another cluster, which cannot reach the driver"
    ),
    "workspace_unknown": "its workspace's kind is unknown, so the driver may be out of reach",
    "egress_refused": "the driver's egress policy refuses the upstream's address",
    "untrusted_certificate": (
        "the upstream's certificate does not verify (set the connector's "
        "upstream CA for a private one)"
    ),
    "upstream_ca_unusable": "the connector's upstream CA is not usable",
    "upstream_unreachable": "the driver could not reach the upstream",
    "driver_not_started": "its driver pod did not start",
    "no_room": "the installation already runs its cap of driver pods",
    "image_unavailable": "the driver's image is not usable",
    "endpoint_unavailable": "the driver's endpoint could not be prepared",
    "no_connector_id": "the entry names no connector id",
}


@dataclass(frozen=True)
class Problem:
    """Why the driver does not serve a repository: a :data:`REASONS` key,
    and the raw detail for the server log only (cleaned)."""

    reason: str
    detail: str = ""

    def __post_init__(self) -> None:
        if self.reason not in REASONS:
            raise ValueError(f"unknown git swap reason {self.reason!r}")
        object.__setattr__(self, "detail", clean_detail(self.detail))

    @property
    def text(self) -> str:
        return REASONS[self.reason]


#: An upstream check that decided nothing (no answer from the orchestrator).
UNDECIDED = "undecided"

_CONTROL = re.compile(r"\s+")


def clean_detail(text: Any, limit: int = MAX_DETAIL) -> str:
    """Raw text made safe to log or store: no control or format characters
    (an ANSI escape, a bidi override), whitespace collapsed, capped."""
    cleaned = "".join(
        ch if not unicodedata.category(ch).startswith("C") else " "
        for ch in str(text or "")
    )
    cleaned = _CONTROL.sub(" ", cleaned).strip()
    return cleaned if len(cleaned) <= limit else cleaned[: limit - 3] + "..."


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
#: (host, upstream CA digest, private allowed) -> (expiry, verdict)
_verdicts: dict[tuple[str, str, bool], tuple[float, Any]] = {}
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
    return hashlib.sha256((ca_pem or "").encode()).hexdigest()


async def probe_upstream_tls(
    host: str,
    address: str,
    *,
    ca_pem: str | None = None,
    timeout: float = 5.0,
    port: int = UPSTREAM_PORT,
) -> Problem | str | None:
    """A TLS handshake with the upstream at ``address`` (no credential, no
    request): ``None`` when its certificate verifies for ``host`` against
    public roots, or only ``ca_pem`` when the connector names one; a
    :class:`Problem` when it does not; :data:`UNDECIDED` when the upstream
    does not complete a handshake."""
    try:
        context = (
            ssl.create_default_context(cadata=ca_pem)
            if ca_pem
            else ssl.create_default_context()
        )
    except (ssl.SSLError, ValueError) as exc:
        return Problem("upstream_ca_unusable", str(exc))
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    try:
        _reader, writer = await asyncio.wait_for(
            asyncio.open_connection(address, port, ssl=context, server_hostname=host),
            timeout,
        )
    except ssl.SSLCertVerificationError as exc:
        return Problem(
            "untrusted_certificate",
            f"{host}: {exc.verify_message or exc.reason}",
        )
    except (OSError, ssl.SSLError, asyncio.TimeoutError) as exc:
        logger.info(
            "Upstream %s did not complete a TLS handshake from the orchestrator (%s)",
            host,
            type(exc).__name__,
        )
        return UNDECIDED
    writer.close()
    try:
        await asyncio.wait_for(writer.wait_closed(), 2)
    except Exception:
        pass
    return None


async def check_upstream(
    host: str, *, ca_pem: str | None, private_allowed: bool
) -> Problem | str | None:
    """The reconciler's egress check for the upstream host, then its TLS.

    ``None`` when it serves, a :class:`Problem` for a definite refusal, and
    :data:`UNDECIDED` when the host does not resolve (a timeout, SERVFAIL,
    NXDOMAIN) or answer from the orchestrator: none of those says the
    driver cannot reach it.
    """
    settings = git_swap_delivery_settings()
    try:
        answers = list(await settings.resolver(host, settings.ipv6))
    except (OSError, UnicodeError, TimeoutError, asyncio.TimeoutError) as exc:
        logger.info("Upstream %s did not resolve from the orchestrator (%s)", host, exc)
        return UNDECIDED
    if not answers:
        return UNDECIDED

    async def resolved(_host: str, _ipv6: bool) -> list[str]:
        return answers

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
            resolver=resolved,
        )
    except EgressRefused as exc:
        return Problem("egress_refused", str(exc))
    address = pins.hosts[0].addresses[0]
    return await probe_upstream_tls(
        host, address, ca_pem=ca_pem, timeout=settings.probe_timeout_seconds
    )


def _remembered(key: tuple[str, str, bool]) -> tuple[bool, Any]:
    settings = git_swap_delivery_settings()
    found = _verdicts.get(key)
    if found is None or found[0] <= settings.clock():
        return False, None
    return True, found[1]


def _remember(key: tuple[str, str, bool], verdict: Any) -> None:
    settings = git_swap_delivery_settings()
    if len(_verdicts) > 4096:
        _verdicts.clear()
    keep = settings.verdict_seconds if verdict is None else BRIEF_VERDICT_SECONDS
    _verdicts[key] = (settings.clock() + min(keep, settings.verdict_seconds), verdict)


async def upstream_verdict(
    host: str, *, ca_pem: str | None, private_allowed: bool, fresh: bool = False
) -> Problem | str | None:
    """:func:`check_upstream`, remembered per host, CA and tier."""
    key = (host, _ca_digest(ca_pem), private_allowed)
    if not fresh:
        known, verdict = _remembered(key)
        if known:
            return verdict
    verdict = await check_upstream(host, ca_pem=ca_pem, private_allowed=private_allowed)
    _remember(key, verdict)
    return verdict


def candidate_entry(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """What a stored repository row's candidate entry would be, for
    preparing a delivery before its caller takes locks (the URL, config and
    id are all the preparation reads); ``None`` for a row the driver could
    not serve."""
    if str(row.get("type") or "") != GIT_SWAP_SPEC.legacy_type:
        return None
    try:
        swap_upstream(row.get("connection_url"))
        connector = str(UUID(str(row.get("id") or row.get("datasource_id"))))
    except (UnservedUpstream, ValueError):
        return None
    return {
        "type": GIT_SWAP_SPEC.legacy_type,
        "datasource_id": connector,
        "connection_url": row.get("connection_url"),
        "config": row.get("config") or {},
        "git_swap": {},
    }


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


async def current_generation(conn: Any, connector_id: str) -> str | None:
    """The credential generation the reconciler builds the connector's pod
    with now (its clean upstream, its upstream CA, its projects' tier), read
    from the stored row as the reconciler reads it; ``None`` when the row is
    gone."""
    from orchestrator.services.connector_drivers.git_swap import (
        swap_service_connector,
    )
    from orchestrator.services.connector_service_hosting import (
        credential_generation,
    )

    row = await conn.fetchrow(
        "SELECT connection_url, config FROM datasources WHERE id = $1",
        UUID(connector_id),
    )
    if row is None:
        return None
    private = await private_addresses_allowed(
        conn, connector_id, private_tiers=git_swap_delivery_settings().private_tiers
    )
    return credential_generation(
        GIT_SWAP_SPEC,
        swap_service_connector(
            {"connection_url": row["connection_url"], "config": row["config"]}
        ),
        private_allowed=private,
    )


async def _serving(conn: Any, connector_id: str, generation: str | None = None) -> bool:
    """Whether a ready pod of the connector's current generation serves it.
    A pod of an earlier generation (an upstream CA or a tier that changed
    since) or one still starting proves nothing about the pod a delivery
    binds to now: that one's start may fail (C3 re-review 2)."""
    if generation is None:
        generation = await current_generation(conn, connector_id)
        if generation is None:
            return False
    return bool(
        await conn.fetchval(
            """
            SELECT 1 FROM connector_driver_identities
             WHERE connector_id = $1 AND driver = $2
               AND credential_generation = $3
               AND revoked_at IS NULL AND ready_at IS NOT NULL
             LIMIT 1
               FOR KEY SHARE
            """,
            UUID(connector_id),
            GIT_SWAP_SPEC.name,
            generation,
        )
    )


async def prepare_git_swap_delivery(store: Any, entries: Any) -> None:
    """Check every candidate's upstream before the caller's transaction
    opens (DNS and a TLS handshake: network the delivery must not wait on).
    A connector whose pod serves needs no check. Never raises: the delivery
    decides with whatever was found."""
    settings = git_swap_delivery_settings()
    if not settings.installed:
        return
    for entry, connector_id in _candidates(entries):
        try:
            upstream = swap_upstream(entry.get("connection_url"))
            async with store.acquire() as conn:
                if await _serving(conn, connector_id):
                    continue
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


def workspace_reach_problem(
    backend: str | None, provisioner: str | None
) -> Problem | None:
    """Whether a workspace of ``backend`` reaches the driver namespace."""
    if backend == "sandbox":
        if str(provisioner or "").lower() == "docker":
            return Problem("workspace_static_pool")
        return None
    if backend == "vm":
        if git_swap_delivery_settings().vm_on_pod_network():
            return None
        return Problem("workspace_remote_vm")
    return Problem("workspace_unknown", f"backend {backend!r}")


async def owner_workspace_problem(conn: Any, owner: Any) -> Problem | None:
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
            return Problem("workspace_unknown", "the job is gone")
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
            return Problem("workspace_unknown", "the session is gone")
        backend = declared_thread_workspace_backend({"metadata": row["metadata"]})
        container = _json_object(row["metadata"]).get("workspace_container")
    provisioner = (
        container.get("provisioner") if isinstance(container, Mapping) else None
    )
    return workspace_reach_problem(backend, provisioner)


def _stop_problem(reason: str, error: Any) -> Problem:
    detail = clean_detail(error)
    if reason == "upstream_unreachable":
        if detail.startswith("untrusted certificate"):
            return Problem("untrusted_certificate", detail)
        if detail.startswith("upstream CA unusable"):
            return Problem("upstream_ca_unusable", detail)
        return Problem("upstream_unreachable", detail)
    if reason == "capacity":
        return Problem("no_room", detail)
    return Problem("driver_not_started", f"{reason}: {detail}" if detail else reason)


#: The pods that hold a slot a new pod of the connector ($1, whose current
#: generation is $2) cannot take: every pod not removed yet, except a live
#: one idle without a binding (the reconciler stops the longest-idle such
#: pod at the cap) and the connector's own pods of an earlier generation
#: (they give way to its successor). A stopped pod still terminating holds
#: its slot: another key's start may be waiting for it, so a delivery never
#: counts on it. The reconciler's eviction (``connector_service_hosting``)
#: decides the same way.
_BUSY_PODS = """
SELECT count(*) FROM connector_driver_identities AS pod
 WHERE pod.credential_generation IS NOT NULL AND pod.removed_at IS NULL
   AND NOT (
        (pod.revoked_at IS NULL AND pod.idle_since IS NOT NULL AND NOT EXISTS (
            SELECT 1 FROM connector_credential_leases AS lease
             WHERE lease.connector_id = pod.connector_id
               AND lease.image_digest = pod.image_digest
               AND lease.revoked_at IS NULL AND lease.expires_at > now()))
        OR (pod.connector_id = $1 AND pod.credential_generation IS DISTINCT FROM $2)
   )
"""


async def launch_problem(
    conn: Any, connector_id: str, *, generation: str | None = None
) -> Problem | None:
    """Why the connector's driver pod cannot be counted on now: its last pod
    stopped without serving (within the back-off, and since the connector
    last changed), or the installation has no room for one. A ready pod of
    the connector's current generation is no problem."""
    settings = git_swap_delivery_settings()
    if generation is None:
        generation = await current_generation(conn, connector_id)
    if generation is not None and await _serving(conn, connector_id, generation):
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
        return _stop_problem(str(stopped["revoke_reason"]), stopped["launch_error"])
    busy = await conn.fetchval(_BUSY_PODS, UUID(connector_id), generation)
    if int(busy or 0) >= settings.max_installation:
        return Problem("no_room", f"cap {settings.max_installation}")
    return None


async def _upstream_problem(
    conn: Any, entry: Mapping[str, Any], connector_id: str
) -> Problem | None:
    """The remembered upstream verdict, or one decided now within
    :data:`INLINE_CHECK_SECONDS` (a delivery path that prepared nothing); a
    check that does not finish in time, or decides nothing, serves."""
    settings = git_swap_delivery_settings()
    upstream = swap_upstream(entry.get("connection_url"))
    private = await private_addresses_allowed(
        conn, connector_id, private_tiers=settings.private_tiers
    )
    ca_pem = upstream_ca_of(entry.get("config"))
    known, verdict = _remembered((upstream.host, _ca_digest(ca_pem), private))
    if not known:
        try:
            verdict = await asyncio.wait_for(
                asyncio.create_task(
                    upstream_verdict(
                        upstream.host, ca_pem=ca_pem, private_allowed=private
                    )
                ),
                INLINE_CHECK_SECONDS,
            )
        except (asyncio.TimeoutError, TimeoutError):
            return None
    return verdict if isinstance(verdict, Problem) else None


async def git_swap_problem(
    conn: Any, entry: Mapping[str, Any], *, connector_id: str, owner: Any
) -> Problem | None:
    """Why the driver cannot serve this candidate for ``owner`` now, or
    ``None`` (the image is the lease step's own check)."""
    from orchestrator.services.connector_driver_ca import driver_ca
    from orchestrator.services.connector_service_images import service_image_settings

    settings = git_swap_delivery_settings()
    if not settings.installed or not service_image_settings().service_namespace:
        return Problem("not_installed")
    if driver_ca() is None:
        return Problem("no_authority")
    try:
        swap_upstream(entry.get("connection_url"))
    except UnservedUpstream as exc:
        return Problem("url_not_served", str(exc))
    token = (entry.get("credentials") or {}).get("token")
    if not isinstance(token, str) or len(token) < MIN_TOKEN_LENGTH:
        # The driver refuses it (it masks the token in every answer).
        return Problem("token_too_short")
    problem = await owner_workspace_problem(conn, owner)
    if problem is not None:
        return problem
    generation = await current_generation(conn, connector_id)
    if generation is not None and await _serving(conn, connector_id, generation):
        # The current generation's pod serves: it proved its upstream (and
        # its upstream CA) at start.
        return None
    problem = await launch_problem(conn, connector_id, generation=generation)
    if problem is not None:
        return problem
    return await _upstream_problem(conn, entry, connector_id)


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
    entry: dict[str, Any], problem: Problem, *, fallback: str | None = None
) -> None:
    """Put a candidate on the installation's fallback, saying why (one of
    :data:`REASONS`; the raw detail is logged, never delivered)."""
    mode = fallback or git_swap_delivery_settings().fallback
    connector = str(entry.get("datasource_id") or entry.get("name") or "?")
    refused = mode == FALLBACK_REFUSE
    note = (connector, problem.reason)
    if note not in _NOTED:
        if len(_NOTED) >= _NOTED_MAX:
            _NOTED.clear()
        _NOTED.add(note)
        logger.warning(
            "Repository connector %s %s: %s%s (connectors.drivers.gitSwap.fallback=%s)",
            connector,
            "is not delivered" if refused else "delivers its token in the clone URL",
            problem.text,
            f" [{problem.detail}]" if problem.detail else "",
            mode,
        )
    if refused:
        entry["credentials"] = {}
        entry["git_swap"] = {
            "unavailable": f"{problem.text}; this installation refuses token-in-URL delivery"
        }
    else:
        entry["git_swap"] = {"fallback": problem.text}


# =============================================================================
# Test
# =============================================================================


def _tls_text(verdict: Any, ca_pem: str | None) -> str:
    if verdict is None:
        return (
            "verified against the connector's upstream CA"
            if ca_pem
            else "verified against public roots"
        )
    if isinstance(verdict, Problem):
        return verdict.text
    return "not checked: the upstream did not answer from the orchestrator"


async def delivery_report(
    row: Mapping[str, Any], *, token: str | None = None
) -> dict[str, Any] | None:
    """How a token repository is delivered on this installation, for its
    Test: through the driver, or the fallback and why (a :data:`REASONS`
    text). Probes the upstream's TLS afresh (no credential) and remembers the
    verdict. Without the driver: ``None`` (nothing changed since before C3),
    except on a ``refuse`` installation, where the repository is refused.
    The workspace's own reach is decided per delivery: container and
    same-cluster VM workspaces only. ``token`` is only measured, never
    sent."""
    settings = git_swap_delivery_settings()
    report: dict[str, Any] = {"driver": GIT_SWAP_SPEC.name}
    if not settings.installed:
        if settings.fallback != FALLBACK_REFUSE:
            return None
        return {**report, "mode": "refused", "reason": REASONS["not_installed"]}
    connector_id = str(row.get("id") or "")
    problem: Problem | None = None
    try:
        upstream = swap_upstream(row.get("connection_url"))
    except UnservedUpstream as exc:
        problem = Problem("url_not_served", str(exc))
        upstream = None
    if problem is None and token is not None and len(token) < MIN_TOKEN_LENGTH:
        problem = Problem("token_too_short")
    ca_pem = upstream_ca_of(row.get("config"))
    if upstream is not None and settings.store is not None:
        try:
            async with settings.store.acquire() as conn:
                private = await private_addresses_allowed(
                    conn, connector_id, private_tiers=settings.private_tiers
                )
                launch = await launch_problem(conn, connector_id)
            verdict = await upstream_verdict(
                upstream.host, ca_pem=ca_pem, private_allowed=private, fresh=True
            )
        except Exception as exc:
            logger.warning("Git swap delivery report failed", exc_info=True)
            return {**report, "mode": "unknown", "reason": type(exc).__name__}
        report["upstream_tls"] = _tls_text(verdict, ca_pem)
        problem = (
            problem or (verdict if isinstance(verdict, Problem) else None) or launch
        )
    if problem is None:
        report.update(mode="git-swap", reason="")
    elif settings.fallback == FALLBACK_REFUSE:
        report.update(mode="refused", reason=problem.text)
    else:
        report.update(mode=FALLBACK_TOKEN_IN_URL, reason=problem.text)
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


# =============================================================================
# The upstream CA
# =============================================================================

_PEM_BLOCK = re.compile(
    r"-----BEGIN ([A-Z0-9 ]+)-----\r?\n(.*?)\r?\n-----END \1-----", re.DOTALL
)


def validate_upstream_ca(value: Any) -> str | None:
    """A connector's ``upstream_ca``: PEM certificates only, re-serialised
    one after another, or ``None`` for none. Anything else in the text (a
    public key, a private key, a CRL, a block of another kind, text between
    or around the blocks) is refused, as the driver refuses it. Raises
    ``ValueError`` saying what is wrong."""
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        raise ValueError("upstream_ca is PEM text")
    if len(value.encode()) > MAX_UPSTREAM_CA_BYTES:
        raise ValueError("upstream_ca is larger than 64 KiB")
    from cryptography import x509
    from cryptography.hazmat.primitives import serialization

    text = value.strip()
    certificates: list[str] = []
    position = 0
    for match in _PEM_BLOCK.finditer(text):
        between = text[position : match.start()]
        if between.strip():
            raise ValueError("upstream_ca holds text that is not a PEM certificate")
        kind = match.group(1)
        if kind != "CERTIFICATE":
            raise ValueError(
                f"upstream_ca holds a {kind} block; it takes CERTIFICATE blocks only"
            )
        try:
            certificate = x509.load_pem_x509_certificate(match.group(0).encode())
        except ValueError as exc:
            raise ValueError(
                f"upstream_ca holds a certificate that does not parse ({exc})"
            ) from None
        certificates.append(
            certificate.public_bytes(serialization.Encoding.PEM).decode("ascii")
        )
        position = match.end()
    if text[position:].strip():
        raise ValueError("upstream_ca holds text that is not a PEM certificate")
    if not certificates:
        raise ValueError("upstream_ca holds no certificate")
    return "".join(certificates)


__all__ = [
    "BRIEF_VERDICT_SECONDS",
    "GitSwapDeliverySettings",
    "INLINE_CHECK_SECONDS",
    "MIN_TOKEN_LENGTH",
    "Problem",
    "REASONS",
    "UNDECIDED",
    "UNSERVABLE_STOPS",
    "apply_fallback",
    "candidate_entry",
    "check_upstream",
    "clean_detail",
    "configure_git_swap_delivery",
    "current_generation",
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
