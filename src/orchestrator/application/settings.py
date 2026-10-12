"""Deployment feature gates, read once when an application is built.

These were module-level flags in ``orchestrator.main``; every value keeps its
environment variable, default and parsing. ``create_app()`` reads them at the
same moment the former module did (application construction, which the
entrypoint performs at import), so a rolling restart is still the way to change
them. Nothing in the application writes to a built ``DeploymentSettings``; a
test that needs another gate builds its own settings or replaces one field on
its own application's settings.
"""

from __future__ import annotations

import ipaddress
import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from orchestrator.services.connector_drivers.workspace_ssh import (
    WORKSPACE_SSH_KNOWN_HOSTS_ENV,
)
from orchestrator.services.connector_egress import (
    DEFAULT_CLUSTER_CIDRS,
    DEFAULT_PRIVATE_TIERS,
)
from shared.connectors.git_swap import (
    FALLBACK_REFUSE,
    FALLBACK_TOKEN_IN_URL,
    FALLBACKS,
)
from shared.run_queue import LANE_PINNED, LANE_STATELESS

logger = logging.getLogger(__name__)

#: The session lanes whose parents may fan out (parallel_subagents.md §12).
SESSION_SUBAGENT_FANOUT_LANES_ENV = "SESSION_SUBAGENT_FANOUT_LANES"
_SESSION_SUBAGENT_FANOUT_LANE_NAMES = frozenset({LANE_STATELESS, LANE_PINNED})


#: The credential lease exchange's dedicated port (slice C2).
CONNECTOR_LEASE_EXCHANGE_PORT_ENV = "CONNECTOR_LEASE_EXCHANGE_PORT"
#: The exchange server's second listener: the deny target of a driver pod's
#: start-up wait (connectors.servicePods.canaryPort, D5).
CONNECTOR_LEASE_CANARY_PORT_ENV = "CONNECTOR_LEASE_CANARY_PORT"
_MAIN_PORT = 8085


def _enabled(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).lower() in ("true", "1", "yes")


def parse_exchange_port(
    raw: str | None, *, name: str = CONNECTOR_LEASE_EXCHANGE_PORT_ENV
) -> int | None:
    """The exchange port from ``CONNECTOR_LEASE_EXCHANGE_PORT`` (or the
    exchange server's canary port from ``name``).

    Unset, empty or ``0`` is off. A value that is not a port, or is the main
    API port, is off with a warning: the exchange must never share a port
    with the rest of the API.
    """
    value = (raw or "").strip()
    if not value or value == "0":
        return None
    try:
        port = int(value)
    except ValueError:
        port = -1
    if not 1 <= port <= 65535 or port == _MAIN_PORT:
        logger.warning(
            "%s=%r is not a dedicated port; it is off",
            name,
            raw,
        )
        return None
    return port


def parse_canary_port(raw: str | None, *, exchange_port: int | None) -> int | None:
    """The exchange server's canary port (``CONNECTOR_LEASE_CANARY_PORT``):
    a port of its own, never the exchange's or the API's."""
    port = parse_exchange_port(raw, name=CONNECTOR_LEASE_CANARY_PORT_ENV)
    if port is not None and port == exchange_port:
        logger.warning(
            "%s is the exchange port; the canary is off",
            CONNECTOR_LEASE_CANARY_PORT_ENV,
        )
        return None
    return port


def parse_positive_number(
    name: str, raw: str | None, *, default: float, minimum: float
) -> float:
    """A number from an environment value, at least ``minimum``.

    Unset or empty is ``default``; a value that is not a number is
    ``default`` with a warning, never an import-time crash.
    """
    value = (raw or "").strip()
    if not value:
        return default
    try:
        number = float(value)
    except ValueError:
        logger.warning("%s=%r is not a number; using %s", name, raw, default)
        return default
    return max(minimum, number)


def parse_json_object(name: str, raw: str | None) -> dict[str, Any]:
    """A JSON object from an environment value; ``{}`` (with a warning when
    set) for anything else."""
    value = (raw or "").strip()
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except ValueError:
        parsed = None
    if not isinstance(parsed, dict):
        logger.warning("%s is not a JSON object; ignoring it", name)
        return {}
    return parsed


def parse_name_list(raw: str | None) -> frozenset[str]:
    """A comma-separated list of names (hosts, CIDRs); blanks are dropped."""
    return frozenset(item.strip() for item in (raw or "").split(",") if item.strip())


def _port(raw: Any) -> int | None:
    try:
        port = int(str(raw).strip())
    except ValueError:
        return None
    return port if 1 <= port <= 65535 else None


def parse_gitea_endpoints(environ: Mapping[str, str]) -> frozenset[str]:
    """SRW's own Gitea as ``host:port`` endpoints, from the variables the
    chart sets for it: the in-cluster and browser-facing API URLs
    (``GITEA_INTERNAL_URL``, ``GITEA_URL``) and the internal and external
    SSH endpoints (``GITEA_SSH_INTERNAL_HOST``/``_PORT``, the host defaulting
    to the API's as the Gitea client's does, and ``GITEA_SSH_EXTERNAL_HOST``/
    ``_PORT``). A connector's Test may reach exactly these at a private or
    cluster address, each with its port. Unset, empty, port ``0`` (SSH off)
    or unparsable adds nothing."""
    endpoints: set[str] = set()

    def add(host: str | None, port: int | None) -> None:
        host = (host or "").strip().lower().rstrip(".")
        if host and port is not None:
            endpoints.add(f"{host}:{port}")

    api_hosts: list[str] = []
    for name in ("GITEA_INTERNAL_URL", "GITEA_URL"):
        try:
            parsed = urlsplit(environ.get(name, "").strip())
            port = parsed.port or {"https": 443, "http": 80}.get(parsed.scheme)
        except ValueError:
            continue
        if parsed.hostname:
            api_hosts.append(parsed.hostname)
        add(parsed.hostname, port)
    add(
        environ.get("GITEA_SSH_INTERNAL_HOST", "").strip()
        or (api_hosts[0] if api_hosts else None),
        _port(environ.get("GITEA_SSH_INTERNAL_PORT", "2222")),
    )
    add(
        environ.get("GITEA_SSH_EXTERNAL_HOST", ""),
        _port(environ.get("GITEA_SSH_EXTERNAL_PORT", "22")),
    )
    return frozenset(endpoints)


#: The longest a delivery waits for a bind: under the agent's 30 s request
#: to the orchestrator, with room for the rest of the answer.
MAX_BIND_WAIT_SECONDS = 25.0


def parse_repository_list(raw: str | None) -> tuple[str, ...]:
    """Image repositories from a JSON list (``connectors.drivers.
    trustedRepositories``). Anything that is not a list of non-empty strings
    trusts nothing, with a warning: a typo never widens trust."""
    value = (raw or "").strip()
    if not value:
        return ()
    try:
        parsed = json.loads(value)
    except ValueError:
        parsed = None
    if not isinstance(parsed, list) or not all(
        isinstance(item, str) and item.strip() for item in parsed
    ):
        logger.warning(
            "CONNECTOR_DRIVER_TRUSTED_REPOSITORIES is not a JSON list of "
            "repositories; no driver repository is trusted"
        )
        return ()
    return tuple(item.strip() for item in parsed)


def parse_cidr_list(
    raw: str | None, *, default: tuple[str, ...] = DEFAULT_CLUSTER_CIDRS
) -> tuple[str, ...]:
    """Networks from a comma-separated list (the cluster's pod and service
    ranges unless ``default`` says otherwise).

    Unset or empty is ``default`` (k3s's defaults). A malformed entry is
    dropped with a warning; when none is left the defaults stand, so a typo
    never opens the cluster's own ranges to driver pods.
    """
    cidrs: list[str] = []
    for item in sorted(parse_name_list(raw)):
        try:
            cidrs.append(str(ipaddress.ip_network(item, strict=False)))
        except ValueError:
            logger.warning("CIDR %r is not a network; ignoring it", item)
    return tuple(cidrs) or default


def parse_session_subagent_fanout_lanes(raw: str | None) -> frozenset[str]:
    """The lanes named by ``SESSION_SUBAGENT_FANOUT_LANES``.

    A comma-separated subset of ``stateless`` and ``pinned``, case and
    whitespace insensitive; unset or empty is off. An unknown name is ignored
    with a warning and never widens the set, so a typo fails closed.
    """
    lanes: set[str] = set()
    for token in (raw or "").split(","):
        name = token.strip().lower()
        if not name:
            continue
        if name in _SESSION_SUBAGENT_FANOUT_LANE_NAMES:
            lanes.add(name)
        else:
            logger.warning(
                "%s names unknown lane %r; ignoring it",
                SESSION_SUBAGENT_FANOUT_LANES_ENV,
                name,
            )
    return frozenset(lanes)


def parse_git_swap_fallback(raw: str | None) -> str:
    """``CONNECTOR_GIT_SWAP_FALLBACK``: what a token repository gets when the
    git swap driver cannot serve it. Unset or empty is ``token-in-url`` (the
    delivery before C3); a value that is neither is ``refuse`` with a
    warning, so a typo never puts a token in a workspace."""
    value = (raw or "").strip().lower()
    if not value:
        return FALLBACK_TOKEN_IN_URL
    if value in FALLBACKS:
        return value
    logger.warning(
        "CONNECTOR_GIT_SWAP_FALLBACK=%r is not one of %s; refusing token "
        "repositories the git swap driver cannot serve",
        raw,
        ", ".join(FALLBACKS),
    )
    return FALLBACK_REFUSE


@dataclass
class DeploymentSettings:
    #: Auto-assignment toggle (default on).
    auto_assign_enabled: bool
    #: Session admission reads the same default-off gate that renders the
    #: generic stateless executor Deployment. Helm supplies this explicitly to
    #: the orchestrator; an unset/local process preserves pinned behavior.
    stateless_session_enabled: bool
    #: Worker jobs remain pinned unless this independent admission gate is
    #: opened. Session-pool enablement is intentionally not sufficient: worker
    #: rollout and rollback have different safety gates and capacity
    #: requirements.
    stateless_worker_enabled: bool
    #: Worker-lane defaulting is subordinate to admission. If this is enabled
    #: while worker admission remains disabled, omitted root jobs silently stay
    #: pinned.
    stateless_worker_default_enabled: bool
    #: Gate-3 completion commands ship dark. Helm wires this value explicitly;
    #: local/tests may opt in without changing the legacy path.
    completion_commands_enabled: bool
    #: Reorders only newly accepted completion commands. The admission decision
    #: is persisted on the command row so a config flip or rolling restart
    #: cannot change the ordering contract of work already in flight. Requires
    #: ``completion_commands_enabled`` (the lifespan refuses the combination).
    completion_status_reorder_enabled: bool
    #: First-rollout safety fence for the interim dedicated Officer-pod owner.
    #: Read-only drift observation and the authorized manual recycle stay
    #: available while automatic drift/missing-pod mutation is dark.
    persistent_agent_reconciliation_enabled: bool
    #: Dark-by-default, admin-only deployed verification seam for the exact
    #: current Officer runtime binding; no lookup or hot-path work unless
    #: explicitly enabled through Helm.
    officer_runtime_verification_enabled: bool
    #: BP-01 release fence. The owner-facing control may ship while the wider
    #: unattended-release scorecard is open, but a stored/manual JSON edit must
    #: not make the money-spending tick live. ``false`` always stays writable so
    #: an operator can stand a century down during rollback or an incident.
    officer_auto_pull_release_enabled: bool
    #: Local-only crash-recovery proof hook. Production/chart defaults keep
    #: this at zero; a positive value makes the accept -> force-delete window
    #: deterministic.
    completion_finalizer_inline_delay_seconds: float
    #: Session lanes whose parents may run several ``delegate_agent`` calls
    #: from one response (parallel_subagents.md §12, D5; default none). The
    #: orchestrator evaluates it for the session's lane at every stateless
    #: claim and pinned attach and advertises the boolean beside the
    #: batch-settle capability, so it is never frozen into a session: turning
    #: a lane off reaches every stateless session at its next claim, while
    #: batches already in flight are still settled by the recovery path.
    session_subagent_fanout_lanes: frozenset[str] = frozenset()
    #: The deployment's default SSH host-key pins
    #: (``orchestrator.workspaceSshKnownHosts``): known_hosts lines
    #: an SSH connector without a pin of its own is checked against.
    #: Empty means such connectors trust a host on first use.
    workspace_ssh_known_hosts: str = ""
    #: The credential lease exchange's own port
    #: (``orchestrator.connectorLeases.exchangePort``, slice C2); ``None``
    #: serves no exchange. Never the main port.
    connector_lease_exchange_port: int | None = None
    #: The exchange server's canary listener (``connectors.servicePods.
    #: canaryPort``); ``None`` serves the exchange alone.
    connector_lease_canary_port: int | None = None
    #: Install the development lease probe driver (``srw.lease-probe/v1``,
    #: ``orchestrator.connectorLeases.probeDriver``). Off by default.
    connector_lease_probe_enabled: bool = False
    #: Seconds a credential lease lives after its last renewal
    #: (``orchestrator.connectorLeases.ttlSeconds``; decision 9: 900).
    connector_lease_ttl_seconds: int = 900
    #: Seconds between lease sweeps
    #: (``orchestrator.connectorLeases.sweepIntervalSeconds``); the sweeper
    #: never waits longer than a quarter of the TTL.
    connector_lease_sweep_seconds: float = 60.0
    #: Driver image resolution (``connectors.drivers.registry``, D5):
    #: registries reached over plain HTTP, registries that may resolve to
    #: private or cluster addresses, extra bearer-token hosts, how long a
    #: resolution is reused, and the deadline of one lookup.
    connector_driver_registry_insecure_hosts: frozenset[str] = frozenset()
    connector_driver_registry_private_hosts: frozenset[str] = frozenset()
    connector_driver_registry_token_hosts: frozenset[str] = frozenset()
    connector_driver_resolve_cache_seconds: float = 60.0
    connector_driver_resolve_timeout_seconds: float = 10.0
    #: Service-plane driver hosting (``connectors.servicePods``, D5): on or
    #: off, and the operator's word that the cluster enforced the start-up
    #: probe harness (shown in the matrix's installation column).
    connector_service_pods_enabled: bool = False
    connector_service_enforcement_verified: bool = False
    #: Driver pod egress (D5, "Reachability"): the cluster's real pod and
    #: service ranges (always refused), the project network tiers that may
    #: reach private addresses, and whether AAAA answers are pinned too.
    connector_service_cluster_cidrs: tuple[str, ...] = DEFAULT_CLUSTER_CIDRS
    connector_service_private_tiers: frozenset[str] = DEFAULT_PRIVATE_TIERS
    connector_service_ipv6: bool = False
    #: Ranges no driver pod may reach even where private addresses are
    #: allowed (the cluster's nodes and load balancers), and this pod's own
    #: address: hosting is refused unless it lies inside the cluster ranges.
    connector_service_refused_cidrs: tuple[str, ...] = ()
    connector_service_pod_ip: str = ""
    connector_service_node_ip: str = ""
    #: SRW's static driver shim image (``connectors.drivers.shim.image``):
    #: the canary wait, the shim install and every driver's command.
    connector_driver_shim_image: str = ""
    #: Development only: install ``srw.echo-service/v1`` running this image
    #: reference (``connectors.drivers.echo``); empty installs nothing.
    connector_echo_driver_image: str = ""
    #: Managed MCP servers to install (D5a, D5b), driver name -> image
    #: reference (``connectors.drivers.managedMcp``, ``mcpTest``,
    #: ``mcpStdioTest`` and ``mcpStdioProbe``), and SRW's front image every
    #: such pod runs beside
    #: the server, which also carries the stdio bridge
    #: (``connectors.drivers.mcpFront.image``, pinned by digest).
    connector_managed_mcp_images: dict[str, str] = field(default_factory=dict)
    connector_mcp_front_image: str = ""
    #: The git swap driver (C3): its image reference
    #: (``connectors.drivers.gitSwap.image``; empty installs nothing), what a
    #: token repository it cannot serve gets (``...gitSwap.fallback``), and
    #: where SRW's connector driver certificate authority is mounted
    #: (``connectors.drivers.ca``), which signs its pods' certificates.
    connector_git_swap_image: str = ""
    connector_git_swap_fallback: str = FALLBACK_TOKEN_IN_URL
    connector_driver_ca_dir: str = ""
    #: Provider-minted credentials (C5, ``connectors.providerMinting``):
    #: whether kubeconfig TokenRequest minting and GitHub App repository
    #: connectors are offered (and their revoke sweep runs), and the
    #: provider hosts (``host[:port]``) the operator trusts at a private or
    #: cluster address, e.g. ``kubernetes.default.svc``.
    connector_provider_minting_enabled: bool = True
    connector_provider_minting_private_hosts: frozenset[str] = frozenset()
    #: SRW's own Gitea (``host:port``, :func:`parse_gitea_endpoints`): a
    #: connector's Test reaches these at a private or cluster address with
    #: no operator listing, exactly these ports; a mint never does.
    connector_test_gitea_endpoints: frozenset[str] = frozenset()
    #: Where service pods run and how the leader reconciles them
    #: (``connectors.servicePods``): the driver and release namespaces, the
    #: installation cap, the idle and start timeouts, the pass interval, the
    #: lease exchange's in-cluster host, the orchestrator pods' labels (the
    #: driver policy's exchange peer) and the driver resource defaults.
    connector_service_namespace: str = ""
    connector_service_release_namespace: str = ""
    connector_service_max_installation: int = 10
    connector_service_idle_seconds: float = 600.0
    connector_service_start_timeout_seconds: float = 180.0
    connector_service_reconcile_seconds: float = 15.0
    connector_service_exchange_host: str = ""
    connector_service_orchestrator_labels: dict[str, str] = field(default_factory=dict)
    connector_service_resources: dict[str, Any] = field(default_factory=dict)
    #: The active main-cloud instance id an operator confirms Helm may
    #: replace with a *different* installation (``cloud.replaceInstallation``).
    #: Empty: Helm's description replaces only the same installation; see
    #: ``services.cloud.instance_registry``.
    main_cloud_replace_installation: str = ""
    #: Re-pinning serving pods (D5a): seconds between re-resolutions of a
    #: pod's pinned egress hosts (0 never re-resolves), and how long a pod
    #: replaced after a re-pin runs on once its replacement serves.
    connector_service_reresolve_seconds: float = 300.0
    connector_service_repin_drain_seconds: float = 30.0
    #: Registered driver images (D6): the operator's trusted repositories
    #: (``connectors.drivers.trustedRepositories``, a path-boundary match)
    #: and whether custom images may have privilege anyway
    #: (``connectors.customDrivers.privileged``, off by default).
    connector_driver_trusted_repositories: tuple[str, ...] = ()
    connector_custom_drivers_privileged: bool = False
    #: Bind-time driver pods (D6): the installation cap (the namespace's
    #: Terminating pod quota is the backstop), each pod's
    #: activeDeadlineSeconds, how long a session's attach or claim waits for
    #: a bind (the bind goes on; never past the agent's 30 s request, so at
    #: most :data:`MAX_BIND_WAIT_SECONDS`), and a user's live spec pods.
    connector_bind_time_max_pods: int = 10
    connector_bind_time_deadline_seconds: float = 120.0
    connector_bind_time_wait_seconds: float = 20.0
    connector_bind_time_spec_pods_per_user: int = 2

    def session_subagent_fanout(self, lane: str | None) -> bool:
        """Whether a session on ``lane`` may fan out right now."""
        return lane in self.session_subagent_fanout_lanes

    @classmethod
    def from_environment(cls) -> DeploymentSettings:
        return cls(
            auto_assign_enabled=_enabled("AUTO_ASSIGN_ENABLED", "true"),
            stateless_session_enabled=_enabled("STATELESS_SESSION_ENABLED"),
            stateless_worker_enabled=_enabled("STATELESS_WORKER_ENABLED"),
            stateless_worker_default_enabled=_enabled(
                "STATELESS_WORKER_DEFAULT_ENABLED"
            ),
            completion_commands_enabled=_enabled("COMPLETION_COMMANDS_ENABLED"),
            completion_status_reorder_enabled=_enabled(
                "COMPLETION_STATUS_REORDER_ENABLED"
            ),
            persistent_agent_reconciliation_enabled=_enabled(
                "PERSISTENT_AGENT_RECONCILIATION_ENABLED"
            ),
            officer_runtime_verification_enabled=_enabled(
                "OFFICER_RUNTIME_VERIFICATION_ENABLED"
            ),
            officer_auto_pull_release_enabled=_enabled(
                "OFFICER_AUTO_PULL_RELEASE_ENABLED"
            ),
            completion_finalizer_inline_delay_seconds=max(
                0.0,
                float(os.environ.get("COMPLETION_FINALIZER_INLINE_DELAY_SECONDS", "0")),
            ),
            session_subagent_fanout_lanes=parse_session_subagent_fanout_lanes(
                os.environ.get(SESSION_SUBAGENT_FANOUT_LANES_ENV)
            ),
            workspace_ssh_known_hosts=os.environ.get(WORKSPACE_SSH_KNOWN_HOSTS_ENV, ""),
            connector_lease_exchange_port=parse_exchange_port(
                os.environ.get(CONNECTOR_LEASE_EXCHANGE_PORT_ENV)
            ),
            connector_lease_canary_port=parse_canary_port(
                os.environ.get(CONNECTOR_LEASE_CANARY_PORT_ENV),
                exchange_port=parse_exchange_port(
                    os.environ.get(CONNECTOR_LEASE_EXCHANGE_PORT_ENV)
                ),
            ),
            connector_lease_probe_enabled=_enabled("CONNECTOR_LEASE_PROBE_ENABLED"),
            connector_lease_ttl_seconds=int(
                parse_positive_number(
                    "CONNECTOR_LEASE_TTL_SECONDS",
                    os.environ.get("CONNECTOR_LEASE_TTL_SECONDS"),
                    default=900,
                    minimum=60,
                )
            ),
            connector_lease_sweep_seconds=parse_positive_number(
                "CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS",
                os.environ.get("CONNECTOR_LEASE_SWEEP_INTERVAL_SECONDS"),
                default=60.0,
                minimum=5.0,
            ),
            connector_driver_registry_insecure_hosts=parse_name_list(
                os.environ.get("CONNECTOR_DRIVER_REGISTRY_INSECURE_HOSTS")
            ),
            connector_driver_registry_private_hosts=parse_name_list(
                os.environ.get("CONNECTOR_DRIVER_REGISTRY_PRIVATE_HOSTS")
            ),
            connector_driver_registry_token_hosts=parse_name_list(
                os.environ.get("CONNECTOR_DRIVER_REGISTRY_TOKEN_HOSTS")
            ),
            # At least a second: a bind inside its caller's transaction
            # applies the decision made just before it opened.
            connector_driver_resolve_cache_seconds=parse_positive_number(
                "CONNECTOR_DRIVER_RESOLVE_CACHE_SECONDS",
                os.environ.get("CONNECTOR_DRIVER_RESOLVE_CACHE_SECONDS"),
                default=60.0,
                minimum=1.0,
            ),
            connector_driver_resolve_timeout_seconds=parse_positive_number(
                "CONNECTOR_DRIVER_RESOLVE_TIMEOUT_SECONDS",
                os.environ.get("CONNECTOR_DRIVER_RESOLVE_TIMEOUT_SECONDS"),
                default=10.0,
                minimum=1.0,
            ),
            connector_service_pods_enabled=_enabled("CONNECTOR_SERVICE_PODS_ENABLED"),
            connector_service_enforcement_verified=_enabled(
                "CONNECTOR_SERVICE_ENFORCEMENT_VERIFIED"
            ),
            connector_service_cluster_cidrs=parse_cidr_list(
                os.environ.get("CONNECTOR_SERVICE_CLUSTER_CIDRS")
            ),
            connector_service_private_tiers=(
                parse_name_list(os.environ.get("CONNECTOR_SERVICE_PRIVATE_TIERS"))
                if os.environ.get("CONNECTOR_SERVICE_PRIVATE_TIERS") is not None
                else DEFAULT_PRIVATE_TIERS
            ),
            connector_service_ipv6=_enabled("CONNECTOR_SERVICE_IPV6"),
            connector_service_refused_cidrs=parse_cidr_list(
                os.environ.get("CONNECTOR_SERVICE_REFUSED_CIDRS"), default=()
            ),
            connector_service_pod_ip=os.environ.get(
                "CONNECTOR_SERVICE_POD_IP", ""
            ).strip(),
            connector_service_node_ip=os.environ.get(
                "CONNECTOR_SERVICE_NODE_IP", ""
            ).strip(),
            connector_driver_shim_image=os.environ.get(
                "CONNECTOR_DRIVER_SHIM_IMAGE", ""
            ).strip(),
            connector_echo_driver_image=os.environ.get(
                "CONNECTOR_ECHO_DRIVER_IMAGE", ""
            ).strip(),
            connector_managed_mcp_images={
                str(name): str(reference).strip()
                for name, reference in parse_json_object(
                    "CONNECTOR_MANAGED_MCP_IMAGES",
                    os.environ.get("CONNECTOR_MANAGED_MCP_IMAGES"),
                ).items()
                if isinstance(reference, str) and reference.strip()
            },
            connector_mcp_front_image=os.environ.get(
                "CONNECTOR_MCP_FRONT_IMAGE", ""
            ).strip(),
            connector_git_swap_image=os.environ.get(
                "CONNECTOR_GIT_SWAP_IMAGE", ""
            ).strip(),
            connector_git_swap_fallback=parse_git_swap_fallback(
                os.environ.get("CONNECTOR_GIT_SWAP_FALLBACK")
            ),
            connector_driver_ca_dir=os.environ.get(
                "CONNECTOR_DRIVER_CA_DIR", ""
            ).strip(),
            connector_provider_minting_enabled=_enabled(
                "CONNECTOR_PROVIDER_MINTING_ENABLED", "true"
            ),
            connector_provider_minting_private_hosts=frozenset(
                host.lower()
                for host in parse_name_list(
                    os.environ.get("CONNECTOR_PROVIDER_MINTING_PRIVATE_HOSTS")
                )
            ),
            connector_test_gitea_endpoints=parse_gitea_endpoints(os.environ),
            connector_service_namespace=os.environ.get(
                "CONNECTOR_SERVICE_NAMESPACE", ""
            ).strip(),
            connector_service_release_namespace=os.environ.get(
                "CONNECTOR_SERVICE_RELEASE_NAMESPACE", ""
            ).strip(),
            connector_service_max_installation=int(
                parse_positive_number(
                    "CONNECTOR_SERVICE_MAX_INSTALLATION",
                    os.environ.get("CONNECTOR_SERVICE_MAX_INSTALLATION"),
                    default=10,
                    minimum=1,
                )
            ),
            connector_service_idle_seconds=parse_positive_number(
                "CONNECTOR_SERVICE_IDLE_SECONDS",
                os.environ.get("CONNECTOR_SERVICE_IDLE_SECONDS"),
                default=600.0,
                minimum=0.0,
            ),
            connector_service_start_timeout_seconds=parse_positive_number(
                "CONNECTOR_SERVICE_START_TIMEOUT_SECONDS",
                os.environ.get("CONNECTOR_SERVICE_START_TIMEOUT_SECONDS"),
                default=180.0,
                minimum=30.0,
            ),
            connector_service_reconcile_seconds=parse_positive_number(
                "CONNECTOR_SERVICE_RECONCILE_SECONDS",
                os.environ.get("CONNECTOR_SERVICE_RECONCILE_SECONDS"),
                default=15.0,
                minimum=5.0,
            ),
            connector_service_exchange_host=os.environ.get(
                "CONNECTOR_SERVICE_EXCHANGE_HOST", ""
            ).strip(),
            connector_service_orchestrator_labels={
                str(key): str(value)
                for key, value in parse_json_object(
                    "CONNECTOR_SERVICE_ORCHESTRATOR_LABELS",
                    os.environ.get("CONNECTOR_SERVICE_ORCHESTRATOR_LABELS"),
                ).items()
            },
            connector_service_resources=parse_json_object(
                "CONNECTOR_SERVICE_RESOURCES",
                os.environ.get("CONNECTOR_SERVICE_RESOURCES"),
            ),
            main_cloud_replace_installation=os.environ.get(
                "MAIN_CLOUD_REPLACE_INSTALLATION", ""
            ).strip(),
            connector_service_reresolve_seconds=parse_positive_number(
                "CONNECTOR_SERVICE_RERESOLVE_SECONDS",
                os.environ.get("CONNECTOR_SERVICE_RERESOLVE_SECONDS"),
                default=300.0,
                minimum=0.0,
            ),
            connector_service_repin_drain_seconds=parse_positive_number(
                "CONNECTOR_SERVICE_REPIN_DRAIN_SECONDS",
                os.environ.get("CONNECTOR_SERVICE_REPIN_DRAIN_SECONDS"),
                default=30.0,
                minimum=0.0,
            ),
            connector_driver_trusted_repositories=parse_repository_list(
                os.environ.get("CONNECTOR_DRIVER_TRUSTED_REPOSITORIES")
            ),
            # Opt-in only: a garbage value never grants privilege.
            connector_custom_drivers_privileged=(
                os.environ.get("CONNECTOR_CUSTOM_DRIVERS_PRIVILEGED", "")
                .strip()
                .lower()
                in ("1", "true", "yes", "on")
            ),
            connector_bind_time_max_pods=int(
                parse_positive_number(
                    "CONNECTOR_BIND_TIME_MAX_PODS",
                    os.environ.get("CONNECTOR_BIND_TIME_MAX_PODS"),
                    default=10,
                    minimum=0,
                )
            ),
            connector_bind_time_deadline_seconds=parse_positive_number(
                "CONNECTOR_BIND_TIME_DEADLINE_SECONDS",
                os.environ.get("CONNECTOR_BIND_TIME_DEADLINE_SECONDS"),
                default=120.0,
                minimum=30.0,
            ),
            connector_bind_time_wait_seconds=min(
                MAX_BIND_WAIT_SECONDS,
                parse_positive_number(
                    "CONNECTOR_BIND_TIME_WAIT_SECONDS",
                    os.environ.get("CONNECTOR_BIND_TIME_WAIT_SECONDS"),
                    default=20.0,
                    minimum=0.0,
                ),
            ),
            connector_bind_time_spec_pods_per_user=int(
                parse_positive_number(
                    "CONNECTOR_BIND_TIME_SPEC_PODS_PER_USER",
                    os.environ.get("CONNECTOR_BIND_TIME_SPEC_PODS_PER_USER"),
                    default=2,
                    minimum=1,
                )
            ),
        )


__all__ = [
    "CONNECTOR_LEASE_CANARY_PORT_ENV",
    "CONNECTOR_LEASE_EXCHANGE_PORT_ENV",
    "MAX_BIND_WAIT_SECONDS",
    "SESSION_SUBAGENT_FANOUT_LANES_ENV",
    "DeploymentSettings",
    "parse_canary_port",
    "parse_cidr_list",
    "parse_exchange_port",
    "parse_json_object",
    "parse_name_list",
    "parse_positive_number",
    "parse_repository_list",
    "parse_session_subagent_fanout_lanes",
]
