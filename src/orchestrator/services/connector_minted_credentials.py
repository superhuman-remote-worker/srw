"""Provider-minted connector credentials: mint, deliver, renew, revoke (C5).

Some providers mint short-lived credentials themselves, so no proxy is
needed and the lease is real at the provider ("Three ways to give an agent
ephemeral authority", item 1). SRW mints two kinds, from its own process,
with a credential the connector holds and that never leaves SRW:

* **Kubernetes TokenRequest** for a kubeconfig connector with a
  ``token_request`` config (``shared.connectors.token_request``): a token for
  the target ServiceAccount, bound to a Secret SRW creates per credential.
  The workspace receives a kubeconfig with that token only, through D1d's
  credential-file delivery. Revoke deletes the Secret.
* **GitHub App installation tokens** for a repository connector whose
  credentials name ``auth_method: github_app``
  (``shared.connectors.github_app``): one repository, ``contents: read`` or
  ``write`` by access level. Where the git swap driver (C3) serves the
  repository, the lease exchange hands the token to the driver as the
  upstream credential (:func:`minted_lease_upstream`) and the workspace
  holds a lease only; otherwise the installation's C3 fallback applies
  visibly (the token in the clone URL, or nothing). Revoke is
  ``DELETE /installation/token``.

Every provider call is a ``provider_http`` call: one deadline, a capped
answer, an address the connector's projects may reach (pinned once
resolved), and a fixed reason when it fails.

**Records.** ``connector_minted_credentials`` holds one row per credential:
its owner (the workspace-owning execution, as for C2's leases: a child Job
on its parent's workspace uses the parent's; ``test`` for Test), connector,
provider, access level and a digest of the minting inputs, the token and
what its revoke needs (the bound Secret and the minting credential, or the
API base) as ``APP_ENCRYPTION_KEY`` ciphertexts. A row is written *before*
the provider call (``minting``), so a crash mid-mint leaves a record the
sweep revokes. Owners and connectors carry no foreign key: a revoke outlives
them. Once revoked (or given up), a row keeps no secret: its token and the
minting credential are dropped from it.

**Delivery.** One row per owner and connector is ``live``. Every delivery
(a claim, an attach, a dispatch) hands out the live credential while more
than half of its lifetime is left, so a stateless session's turns and a
pod recycle receive the same token; past that, the delivery's preparation
mints afresh and the old one becomes ``superseded``: still valid at the
provider until its own expiry (another work item on the same workspace may
still hold it), then revoked by the sweep. A change of access level or of
the connector's minting inputs revokes the old one at once instead.
:func:`prepare_minted_entries` (or :func:`prepare_thread_minted`) mints
before the delivery's transaction, waiting at most the caller's bound: a
dispatch or a job's claim passes ``0`` and never waits on a provider (the
mint runs in the background, for a later dispatch); :func:`deliver_minted_entries`
fills the entries inside the transaction and never calls a provider: a
connector its preparation could not mint for is skipped with the
preparation's reason (a session's notice; a job waits for a provider that
did not answer, or fails on one that refused, as D6's binds do), and one no
preparation looked at (a live update) is minted in the background for a
later delivery (a live credential past half its life, still valid, is
handed out meanwhile). At most one mint per owner and connector runs in a
process, and a recent failure is not tried again until it is stale
(:data:`PREPARED_OUTCOME_SECONDS`, :data:`TRANSIENT_OUTCOME_SECONDS`). Two
mints for one owner and connector in different processes are serialised:
the second one's token is revoked, never dropped.

**Access.** A credential is minted at the most restrictive of the
connector's own ``read_only`` (a public connector's is always set), its
project link's (``project_read_only``, every link of a session's projects)
and the driver's level for it: preparation, delivery and the exchange read
it the same way, so a read-only connector never gets a write token.

**Renewal reaches a running execution only where SRW delivers again**: a
stateless session at every turn's claim, a stateless job at every worker
batch's claim, a pinned session at an attach, a pod recycle or a live
connector update, a job at a re-dispatch after a pause. Nothing pushes a
fresh file into a pinned execution mid-run: a single pinned run or turn
that outlasts ``expiration_seconds`` (Kubernetes) or an hour (GitHub, token
in URL) loses access until its next delivery. Through the git swap driver
the exchange mints again whenever the driver asks, so a GitHub App
connector served by the driver never lapses while its lease lives; a token
an earlier fallback put in the workspace's clone URL is revoked once the
driver serves the connector.

**Revocation** is requested inside the terminal transactions C2's revoke
points already run (End, cancel, delete, completion, a live detach, a
connector delete: ``connector_credential_leases`` calls
:func:`revoke_owner_credentials` and :func:`revoke_connector_credentials`),
and right after a connector's minting inputs change
(:func:`connector_changed`). A request is an UPDATE to ``revoking`` that
commits with the decision and NOTIFYs; the leader's
:func:`connector_minted_credential_sweeper` then makes the provider calls,
a few at a time and one provider host at a time (a dead host takes one
slot, never the pass), each bounded, retrying with backoff up to an hour
apart until the credential has expired. A Secret SRW could not delete by then is
``abandoned`` (logged and audited); so is a record SRW cannot read. The same
sweep revokes credentials whose execution is terminal or gone (a backstop),
expired ones (a paused job's lapse: its bound Secret is deleted so nothing
is left), and mints a crash abandoned. It holds its LISTEN connection only
while there is something to revoke.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three ways
to give an agent ephemeral authority", "The lease service", "The git swap
driver", "Today's types as drivers"; slice C5.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from orchestrator.services.connector_credential_leases import (
    LeaseOwner,
    lease_sweep_seconds,
    record_lease_event,
)
from orchestrator.services.connector_drivers.provider_http import (
    MintedToken,
    ProviderError,
    provider_network,
)
from orchestrator.services.connector_drivers.token_request import (
    delivered_kubeconfig_text,
    parse_minting_kubeconfig,
)
from orchestrator.services.datasource_config import stored_json_object
from shared.connectors.builtin import (
    KUBECONFIG_SPEC,
    REPOSITORY_SPEC,
    driver_spec_for_row,
    git_swap_entry,
)
from shared.connectors.contract import effective_access
from shared.connectors.github_app import (
    CONFIG_KEY as GITHUB_APP_KEY,
    TOKEN_USERNAME,
    GitHubAppConfigError,
    parse_github_app,
    uses_github_app,
)
from shared.connectors.leases import last_four
from shared.connectors.token_request import (
    CONFIG_KEY as TOKEN_REQUEST_KEY,
    MintingKubeconfig,
    TokenRequestConfigError,
    secret_name,
    token_request_options,
)

logger = logging.getLogger(__name__)

PROVIDER_KUBERNETES = "kubernetes"
PROVIDER_GITHUB_APP = "github_app"
#: The key a bound payload entry names its provider under (non-secret); the
#: delivery removes it.
MINTED_KEY = "minted"
#: A credential is handed out again while more than this share of its
#: lifetime is left, and at least :data:`MIN_REMAINING_SECONDS`.
RENEW_FRACTION = 0.5
MIN_REMAINING_SECONDS = 60
#: The most a delivery's preparation waits for one connector's mint.
PREPARE_MINT_SECONDS = 20.0
#: How long a preparation's failure answers a delivery that finds nothing.
PREPARED_OUTCOME_SECONDS = 120.0
#: How long a transient failure (the provider did not answer) is remembered.
TRANSIENT_OUTCOME_SECONDS = 30.0
#: The shortest time between two Tests of one connector by one user.
TEST_MINT_INTERVAL_SECONDS = 10.0
#: How long shutdown waits for background mints to record what they made.
SHUTDOWN_SETTLE_SECONDS = 20.0
#: A ``minting`` row older than this was abandoned (a crash, a cancel).
MINT_ABANDON_SECONDS = 300
REVOKES_PER_PASS = 20
#: Provider revokes one pass runs at once, and the most one may take.
REVOKE_CONCURRENCY = 4
REVOKE_ROW_SECONDS = 30.0
#: A revoke is retried while the credential is valid at the provider; past
#: its expiry (or with none: a mint that never answered) it is given up
#: after this many attempts in all.
MAX_REVOKE_ATTEMPTS = 12
REVOKE_RETRY_BASE_SECONDS = 30.0
REVOKE_RETRY_MAX_SECONDS = 3600.0
RETENTION_DAYS = 30
PRUNE_SECONDS = 3600.0
#: Revoke requests NOTIFY this channel at commit; the leader's sweep LISTENs.
REVOKE_CHANNEL = "srw_connector_minted_revoke"
_TERMINAL_JOB_STATUSES = ("completed", "failed", "cancelled")
_ACTIVE = "('minting', 'live', 'superseded')"
_DONE = "('revoked', 'abandoned')"


class MintFailure(Exception):
    """A credential could not be minted. ``permanent``: the provider (or the
    connector's config) refused it, so trying again changes nothing until
    the connector changes."""

    def __init__(self, message: str, *, permanent: bool) -> None:
        super().__init__(message)
        self.permanent = permanent


@dataclass(frozen=True)
class MintedRuntime:
    """What a delivery that found nothing prepared mints with, in the
    background: the application's store."""

    store: Any


_state: dict[str, Any] = {"runtime": None, "enabled": True}
#: (owner kind, owner id, connector id) -> (when, the preparation's failure)
_prepared: dict[tuple[str, str, str], tuple[float, MintFailure]] = {}
_PREPARED_MAX = 4096
_background: set[asyncio.Task[Any]] = set()
#: (owner kind, owner id, connector id) -> the mint running for it.
_inflight: dict[tuple[str, str, str], asyncio.Task[Any]] = {}
#: (requester, connector id) -> when its last Test minted.
_test_mints: dict[tuple[str, str], float] = {}


def configure_minted_credentials(
    runtime: MintedRuntime | None, *, enabled: bool = True
) -> None:
    """Install the runtime, and whether this installation offers minting
    (``connectors.providerMinting.enabled``)."""
    _state["runtime"] = runtime
    _state["enabled"] = bool(enabled)
    _prepared.clear()
    _test_mints.clear()


def minted_runtime() -> MintedRuntime | None:
    return _state["runtime"]


def minting_enabled() -> bool:
    return bool(_state["enabled"])


DISABLED_DETAIL = (
    "Provider-minted credentials (TokenRequest, GitHub App) are off on this "
    "deployment (connectors.providerMinting.enabled)"
)


@dataclass(frozen=True)
class MintedCredential:
    """A live credential of one owner and connector. ``token`` is secret."""

    id: str
    provider: str
    token: str = field(repr=False)
    expires_at: datetime
    access: str
    material: Mapping[str, Any] = field(repr=False)


# =============================================================================
# Which rows and entries mint
# =============================================================================


def row_provider(row: Any) -> str | None:
    """The provider a stored (decrypted) connector row mints with, if any."""
    get = getattr(row, "get", None)
    if not callable(get):
        return None
    config = stored_json_object(get("config"))
    kind = get("type")
    if (
        kind == KUBECONFIG_SPEC.legacy_type
        and config.get(TOKEN_REQUEST_KEY) is not None
    ):
        return PROVIDER_KUBERNETES
    if kind == REPOSITORY_SPEC.legacy_type and uses_github_app(get("credentials")):
        return PROVIDER_GITHUB_APP
    return None


def minted_marker(entry: Any) -> Mapping[str, Any] | None:
    """A bound payload entry's minting marker (its provider and connector),
    or ``None``."""
    if not isinstance(entry, Mapping):
        return None
    marker = entry.get(MINTED_KEY)
    if not isinstance(marker, Mapping):
        return None
    if marker.get("provider") not in (PROVIDER_KUBERNETES, PROVIDER_GITHUB_APP):
        return None
    try:
        UUID(str(marker.get("connector_id")))
    except ValueError:
        return None
    return marker


def kubeconfig_marker(row: Mapping[str, Any], credentials: Any) -> dict[str, Any]:
    """What a minting kubeconfig connector's entry carries instead of its
    file: the provider, the connector and where the delivered file lands
    (its contents arrive at delivery)."""
    files = credentials.get("files") if isinstance(credentials, Mapping) else None
    first = files[0] if isinstance(files, list) and files else {}
    target = {
        key: first[key]
        for key in ("name", "target_path", "mode", "env_var")
        if isinstance(first, Mapping) and isinstance(first.get(key), str)
    }
    return {
        "provider": PROVIDER_KUBERNETES,
        "connector_id": str(row.get("id") or ""),
        "read_only": connector_read_only(row),
        "file": target,
    }


def github_app_marker(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "provider": PROVIDER_GITHUB_APP,
        "connector_id": str(row.get("id") or ""),
        "read_only": connector_read_only(row),
    }


def connector_read_only(row: Mapping[str, Any]) -> bool:
    """The connector's own read-only rule: its ``read_only``, always set on
    a public one (checked too, should a row lack it)."""
    return bool(row.get("read_only")) or bool(row.get("is_global"))


def clamped_access(access: str, *, read_only: bool) -> str:
    return "ReadOnly" if read_only else access


# =============================================================================
# Plans: what one connector mints with
# =============================================================================


@dataclass(frozen=True)
class _Plan:
    provider: str
    digest: str
    #: The provider's host (``host[:port]``): the sweep's fairness key.
    host: str
    #: What delivery and revoke need, encrypted on the row (secret).
    material: dict[str, Any] = field(repr=False)
    kubernetes: Any = field(default=None, repr=False)
    github: Any = field(default=None, repr=False)


def _digest(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _secret_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _host_of(url: str) -> str:
    from urllib.parse import urlsplit

    return urlsplit(url).netloc.lower()


def plan_for(row: Mapping[str, Any]) -> _Plan:
    """What ``row`` (a decrypted connector) mints with; ``MintFailure``
    (permanent) for a connector SRW cannot mint for."""
    provider = row_provider(row)
    config = stored_json_object(row.get("config"))
    credentials = stored_json_object(row.get("credentials"))
    if provider == PROVIDER_KUBERNETES:
        try:
            options = token_request_options(config)
            files = credentials.get("files")
            contents = (
                files[0].get("contents")
                if isinstance(files, list) and files and isinstance(files[0], Mapping)
                else None
            )
            minting = parse_minting_kubeconfig(contents)
        except TokenRequestConfigError as exc:
            raise MintFailure(str(exc), permanent=True) from None
        assert options is not None
        inputs = {
            "server": minting.server,
            "ca": minting.ca_pem,
            "tls_server_name": minting.tls_server_name,
            "token": _secret_digest(minting.token),
            "context_namespace": minting.context_namespace,
            **options.as_config(),
        }
        material = {
            "server": minting.server,
            "ca": minting.ca_pem,
            "tls_server_name": minting.tls_server_name,
            "token": minting.token,
            "namespace": options.namespace,
            "context_namespace": minting.context_namespace,
            "name": minting.cluster_name,
        }
        return _Plan(
            PROVIDER_KUBERNETES,
            _digest(inputs),
            _host_of(minting.server),
            material,
            kubernetes=(minting, options),
        )
    if provider == PROVIDER_GITHUB_APP:
        from orchestrator.services.connector_drivers.github_app import (
            normalize_private_key,
        )
        from orchestrator.services.connector_git_swap_delivery import upstream_ca_of

        try:
            options = parse_github_app(config, row.get("connection_url"))
            key = normalize_private_key(credentials.get("private_key"))
        except (GitHubAppConfigError, ValueError) as exc:
            raise MintFailure(str(exc), permanent=True) from None
        ca = upstream_ca_of(config)
        inputs = {
            "api_base": options.api_base,
            "app_id": options.app_id,
            "installation_id": options.installation_id,
            "repository": f"{options.owner}/{options.repository}",
            "ca": ca,
            "key": _secret_digest(key),
        }
        material = {
            "api_base": options.api_base,
            "repository": f"{options.owner}/{options.repository}",
            "ca": ca,
        }
        return _Plan(
            PROVIDER_GITHUB_APP,
            _digest(inputs),
            _host_of(options.api_base),
            material,
            github=(options, key, ca),
        )
    raise MintFailure("The connector mints no credential", permanent=True)


# =============================================================================
# Rows
# =============================================================================


def _encrypt(value: str) -> str:
    from orchestrator.security.crypto import encrypt

    return encrypt(value)


def _decrypt(ciphertext: Any) -> str | None:
    from orchestrator.security.crypto import DecryptionError, decrypt

    if not isinstance(ciphertext, str) or not ciphertext:
        return None
    try:
        return decrypt(ciphertext)
    except (DecryptionError, RuntimeError, ValueError, TypeError):
        return None


def _material(row: Mapping[str, Any]) -> dict[str, Any] | None:
    """A row's decrypted material, or ``None`` when it does not read."""
    text = _decrypt(row["material_ciphertext"])
    try:
        value = json.loads(text) if text else None
    except ValueError:
        value = None
    return value if isinstance(value, dict) else None


def _stripped(material: Mapping[str, Any] | None) -> str:
    """A done row's material: what it named, never the minting credential."""
    kept = {key: value for key, value in (material or {}).items() if key != "token"}
    return _encrypt(json.dumps(kept))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def fresh(expires_at: Any, minted_at: Any, *, now: datetime | None = None) -> bool:
    """Whether a credential is handed out again: more than half of its
    lifetime and at least a minute left."""
    if not isinstance(expires_at, datetime) or not isinstance(minted_at, datetime):
        return False
    now = now or _now()
    expires, minted = _aware(expires_at), _aware(minted_at)
    left = (expires - now).total_seconds()
    lifetime = (expires - minted).total_seconds()
    return left >= MIN_REMAINING_SECONDS and left > lifetime * RENEW_FRACTION


def _usable(row: Any, access: str, digest: str | None = None) -> bool:
    if row is None:
        return False
    if digest is not None and row["config_digest"] != digest:
        return False
    if row["provider"] == PROVIDER_GITHUB_APP and row["access"] != access:
        return False
    return fresh(row["expires_at"], row["minted_at"])


_LIVE = """
SELECT id, provider, access, config_digest, material_ciphertext,
       token_ciphertext, expires_at, minted_at, delivered_at
  FROM connector_minted_credentials
 WHERE owner_kind = $1 AND owner_id = $2 AND connector_id = $3
   AND status = 'live'
"""


async def _live_row(conn: Any, owner: LeaseOwner, connector: UUID, *, lock=False):
    return await conn.fetchrow(
        _LIVE + (" FOR UPDATE" if lock else ""), owner.kind, UUID(owner.id), connector
    )


def _credential(row: Mapping[str, Any]) -> MintedCredential | None:
    token = _decrypt(row["token_ciphertext"])
    material = _material(row)
    if not token or material is None:
        return None
    return MintedCredential(
        id=str(row["id"]),
        provider=str(row["provider"]),
        token=token,
        expires_at=_aware(row["expires_at"]),
        access=str(row["access"]),
        material=material,
    )


_OWNER_ACCEPTS = {
    "job": f"""
        SELECT 1 FROM jobs
         WHERE id = $1 AND status::text NOT IN {_TERMINAL_JOB_STATUSES!r}
    """,
    "thread": """
        SELECT 1 FROM threads
         WHERE id = $1 AND status::text <> 'ended'
           AND NOT (runtime_retirement_token IS NOT NULL
                    AND runtime_retirement_authorized_at IS NOT NULL)
    """,
}


async def _owner_accepts(conn: Any, owner: LeaseOwner) -> bool:
    return await conn.fetchval(_OWNER_ACCEPTS[owner.kind], UUID(owner.id)) is not None


async def _private_allowed(conn: Any, connector_id: str) -> bool:
    """Whether the connector's projects may reach private addresses (their
    network tier, as a driver pod's egress is decided)."""
    from orchestrator.services.connector_egress import private_addresses_allowed

    return await private_addresses_allowed(
        conn, connector_id, private_tiers=provider_network().private_tiers
    )


async def _notify(conn: Any) -> None:
    """Wake the leader's sweep when this transaction commits (a NOTIFY is
    sent at commit, never on a rollback); in a savepoint, so a failed
    NOTIFY never aborts the decision it follows."""
    try:
        async with conn.transaction():
            await conn.execute("SELECT pg_notify($1, '')", REVOKE_CHANNEL)
    except Exception:
        logger.debug("Waking the minted-credential sweep failed", exc_info=True)


def _detail(**fields: Any) -> str:
    return " ".join(f"{key}={value}" for key, value in fields.items() if value)


# =============================================================================
# Minting
# =============================================================================


async def _mint(
    plan: _Plan,
    credential_id: UUID,
    material: dict[str, Any],
    *,
    owner_label: str,
    connector_id: str,
    access: str,
) -> MintedToken:
    allow_private = bool(material.get("allow_private"))
    if plan.provider == PROVIDER_KUBERNETES:
        from orchestrator.services.connector_drivers.token_request import mint_token

        minting, options = plan.kubernetes
        return await mint_token(
            minting,
            options,
            secret=material["secret"]["name"],
            credential_id=credential_id,
            annotations={"srw.io/owner": owner_label, "srw.io/connector": connector_id},
            allow_private=allow_private,
        )
    from orchestrator.services.connector_drivers.github_app import (
        mint_installation_token,
    )

    options, key, ca = plan.github
    return await mint_installation_token(
        options, key, access, ca_pem=ca, allow_private=allow_private
    )


async def _insert_minting(
    conn: Any,
    *,
    credential_id: UUID,
    owner_kind: str,
    owner_id: UUID,
    connector: UUID,
    plan: _Plan,
    access: str,
    material: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO connector_minted_credentials
            (id, owner_kind, owner_id, connector_id, provider, provider_host,
             access, config_digest, material_ciphertext)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        """,
        credential_id,
        owner_kind,
        owner_id,
        connector,
        plan.provider,
        plan.host,
        access,
        plan.digest,
        _encrypt(json.dumps(material)),
    )


def _material_for(plan: _Plan, credential_id: UUID, *, allow_private: bool) -> dict:
    material = {**plan.material, "allow_private": bool(allow_private)}
    if plan.provider == PROVIDER_KUBERNETES:
        material["secret"] = {"name": secret_name(credential_id), "uid": None}
    return material


async def ensure_minted(
    store: Any,
    *,
    owner: LeaseOwner,
    connector_id: str,
    access: str,
    row: Mapping[str, Any] | None = None,
) -> MintedCredential:
    """The live credential of ``owner`` for the connector, minting one when
    there is none fit to hand out again (see the module docstring).

    ``row`` is the decrypted connector (read from ``store`` when omitted).
    Every write runs on ``store``'s own connections and commits by itself,
    so a delivery whose transaction rolls back never loses the record of
    what was minted at the provider. Never called inside a delivery's
    transaction. Raises :class:`MintFailure`.
    """
    if not minting_enabled():
        raise MintFailure(DISABLED_DETAIL, permanent=True)
    try:
        connector = UUID(str(connector_id))
    except ValueError:
        raise MintFailure("The entry names no connector", permanent=True) from None
    if row is None:
        row = await store.get_datasource(str(connector))
        if row is None:
            raise MintFailure("The connector no longer exists", permanent=True)
    plan = plan_for(row)
    access = clamped_access(access, read_only=connector_read_only(row))
    async with store.acquire() as conn:
        current = await _live_row(conn, owner, connector)
        if _usable(current, access, plan.digest):
            credential = _credential(current)
            if credential is not None:
                return credential
        if not await _owner_accepts(conn, owner):
            raise MintFailure(
                "The execution no longer accepts credentials", permanent=True
            )
        credential_id = uuid4()
        material = _material_for(
            plan,
            credential_id,
            allow_private=await _private_allowed(conn, str(connector)),
        )
        await _insert_minting(
            conn,
            credential_id=credential_id,
            owner_kind=owner.kind,
            owner_id=UUID(owner.id),
            connector=connector,
            plan=plan,
            access=access,
            material=material,
        )
    try:
        minted = await _mint(
            plan,
            credential_id,
            material,
            owner_label=f"{owner.kind}:{owner.id}",
            connector_id=str(connector),
            access=access,
        )
    except ProviderError as exc:
        await _settle_failed_mint(store, credential_id, plan, material, exc)
        raise MintFailure(str(exc), permanent=not exc.transient) from None
    if plan.provider == PROVIDER_KUBERNETES:
        material["secret"]["uid"] = minted.handle
    recorded = await _record_mint(
        store,
        credential_id=credential_id,
        owner=owner,
        connector=connector,
        plan=plan,
        access=access,
        material=material,
        minted=minted,
    )
    if recorded is None:
        raise MintFailure("The execution no longer accepts credentials", permanent=True)
    return recorded


async def _settle_failed_mint(
    store: Any,
    credential_id: UUID,
    plan: _Plan,
    material: Mapping[str, Any],
    exc: ProviderError,
) -> None:
    """A refused mint's record. A Kubernetes one that may have left its
    bound Secret (the create's answer was lost, or the clean-up failed) is
    revoked by name by the sweep, at once; one whose Secret was refused,
    never reached the server or was deleted again is done. A GitHub one
    minted nothing, unless GitHub granted more than asked and the token
    could not be revoked at once: that token is kept, for the sweep to
    revoke until it expires."""
    from orchestrator.services.connector_drivers.provider_http import UnrevokedToken

    message = str(exc)[:500]
    try:
        async with store.acquire() as conn:
            if isinstance(exc, UnrevokedToken):
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoking', revoke_requested_at = now(),
                           revoke_reason = 'overbroad', revoke_next_at = now(),
                           revoke_error = $2, token_ciphertext = $3,
                           token_last_four = $4, expires_at = $5, minted_at = now()
                     WHERE id = $1 AND status = 'minting'
                    """,
                    credential_id,
                    message,
                    _encrypt(exc.minted.token),
                    last_four(exc.minted.token),
                    exc.minted.expires_at,
                )
                await _notify(conn)
            elif plan.provider == PROVIDER_KUBERNETES and getattr(
                exc, "left_secret", True
            ):
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoking', revoke_requested_at = now(),
                           revoke_reason = 'mint_failed', revoke_next_at = now(),
                           revoke_error = $2, minted_at = now()
                     WHERE id = $1 AND status = 'minting'
                    """,
                    credential_id,
                    message,
                )
                await _notify(conn)
            else:
                # Nothing is left at the provider: done, with no secret.
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoked', revoke_requested_at = now(),
                           revoke_reason = 'mint_failed', revoked_at = now(),
                           revoke_error = $2, material_ciphertext = $3
                     WHERE id = $1 AND status = 'minting'
                    """,
                    credential_id,
                    message,
                    _stripped(material),
                )
    except Exception:
        logger.warning(
            "Recording a failed mint %s failed; the sweep retires it",
            credential_id,
            exc_info=True,
        )


def _owner_lock_key(owner: LeaseOwner, connector: UUID) -> str:
    return f"srw-minted:{owner.kind}:{owner.id}:{connector}"


async def _record_mint(
    store: Any,
    *,
    credential_id: UUID,
    owner: LeaseOwner,
    connector: UUID,
    plan: _Plan,
    access: str,
    material: dict[str, Any],
    minted: MintedToken,
) -> MintedCredential | None:
    """Make a fresh mint the live credential, unless another delivery's
    mint won (that one is returned, and ours is revoked) or the execution
    ended (or detached the connector) meanwhile: then the mint is recorded
    for its revoke and ``None`` is returned. Two records for one owner and
    connector are serialised by an advisory lock, so the second always sees
    the first's live row."""
    token_ciphertext = _encrypt(minted.token)
    material_ciphertext = _encrypt(json.dumps(material))
    recorded_args = (
        credential_id,
        material_ciphertext,
        token_ciphertext,
        last_four(minted.token),
        minted.expires_at,
    )
    async with store.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))",
                _owner_lock_key(owner, connector),
            )
            mine = await conn.fetchrow(
                "SELECT status FROM connector_minted_credentials WHERE id = $1 "
                "FOR UPDATE",
                credential_id,
            )
            current = await _live_row(conn, owner, connector, lock=True)
            accepts = await _owner_accepts(conn, owner)
            if mine is None or mine["status"] != "minting" or not accepts:
                # A revoke request reached the row while it was minted (an
                # End, a detach): it is revoked with what the provider made.
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoking', revoked_at = NULL,
                           revoke_requested_at = COALESCE(revoke_requested_at, now()),
                           revoke_reason = COALESCE(revoke_reason, 'execution_ended'),
                           revoke_next_at = now(),
                           material_ciphertext = $2, token_ciphertext = $3,
                           token_last_four = $4, expires_at = $5, minted_at = now()
                     WHERE id = $1
                    """,
                    *recorded_args,
                )
                await _notify(conn)
                # Committed with the transaction; the caller refuses.
                return None
            winner = (
                _credential(current) if _usable(current, access, plan.digest) else None
            )
            if winner is not None:
                await _retire_raced(conn, recorded_args)
                return winner
            if current is not None:
                changed = current["config_digest"] != plan.digest or (
                    current["provider"] == PROVIDER_GITHUB_APP
                    and current["access"] != access
                )
                if changed:
                    # Another access level or other minting inputs: the old
                    # credential must not stay usable until it expires.
                    await conn.execute(
                        """
                        UPDATE connector_minted_credentials
                           SET status = 'revoking', revoke_requested_at = now(),
                               revoke_reason = 'replaced', revoke_next_at = now()
                         WHERE id = $1
                        """,
                        current["id"],
                    )
                    await _notify(conn)
                else:
                    # Renewed: the old one stays valid until it expires (a
                    # work item on the same workspace may still hold it).
                    await conn.execute(
                        """
                        UPDATE connector_minted_credentials
                           SET status = 'superseded', superseded_at = now()
                         WHERE id = $1
                        """,
                        current["id"],
                    )
            import asyncpg

            raced = False
            try:
                async with conn.transaction():
                    await conn.execute(
                        """
                        UPDATE connector_minted_credentials
                           SET status = 'live', material_ciphertext = $2,
                               token_ciphertext = $3, token_last_four = $4,
                               expires_at = $5, minted_at = now()
                         WHERE id = $1
                        """,
                        *recorded_args,
                    )
            except asyncpg.UniqueViolationError:
                # A live row the lock did not see (it cannot, short of a
                # missed lock): ours is revoked, never dropped. The refusal
                # is raised once that has committed.
                await _retire_raced(conn, recorded_args)
                raced = True
            else:
                await record_lease_event(
                    conn,
                    event_type="connector_minted_credential_issued",
                    resource_type="connector_minted_credential",
                    resource_id=str(credential_id),
                    detail=_detail(
                        owner=f"{owner.kind}:{owner.id}",
                        connector=str(connector),
                        provider=plan.provider,
                        access=access,
                        expires_at=minted.expires_at.isoformat(),
                        token_last_four=last_four(minted.token),
                        renewed=current["id"] if current is not None else None,
                    ),
                )
    if raced:
        raise MintFailure(
            "another delivery minted this credential at the same time; it "
            "arrives at the next delivery",
            permanent=False,
        )
    return MintedCredential(
        id=str(credential_id),
        provider=plan.provider,
        token=minted.token,
        expires_at=_aware(minted.expires_at),
        access=access,
        material=material,
    )


async def _retire_raced(conn: Any, recorded_args: tuple) -> None:
    """Another delivery's mint is the live one: ours is recorded with its
    token, to be revoked."""
    await conn.execute(
        """
        UPDATE connector_minted_credentials
           SET status = 'revoking', revoke_requested_at = now(),
               revoke_reason = 'mint_raced', revoke_next_at = now(),
               material_ciphertext = $2, token_ciphertext = $3,
               token_last_four = $4, expires_at = $5, minted_at = now()
         WHERE id = $1
        """,
        *recorded_args,
    )
    await _notify(conn)


# =============================================================================
# Test: mint once, revoke at once, record both
# =============================================================================


def _test_slot(requester: str | None, connector: UUID) -> None:
    """One Test mint per user and connector at a time, and at most one
    every :data:`TEST_MINT_INTERVAL_SECONDS` (this process's count)."""
    key = (str(requester or ""), str(connector))
    now = time.monotonic()
    last = _test_mints.get(key)
    if last is not None and now - last < TEST_MINT_INTERVAL_SECONDS:
        wait = int(TEST_MINT_INTERVAL_SECONDS - (now - last)) + 1
        raise MintFailure(
            f"This connector was tested a moment ago; test again in {wait} s",
            permanent=False,
        )
    if len(_test_mints) >= _PREPARED_MAX:
        _test_mints.clear()
    _test_mints[key] = now


async def mint_for_test(
    row: Mapping[str, Any], access: str = "ReadOnly", *, requester: str | None = None
) -> tuple[MintedCredential, Any]:
    """Mint one credential for a connector's Test, recorded under the owner
    ``test`` (the connector itself), so a revoke that fails is retried by
    the sweep. Returns the credential and a coroutine function that revokes
    it (and records the outcome). Without a store (no runtime), the mint is
    not recorded. ``requester`` (the user testing) is limited to one Test
    of the connector every :data:`TEST_MINT_INTERVAL_SECONDS`. Raises
    :class:`MintFailure`."""
    if not minting_enabled():
        raise MintFailure(DISABLED_DETAIL, permanent=True)
    plan = plan_for(row)
    connector = UUID(str(row.get("id")))
    _test_slot(requester, connector)
    credential_id = uuid4()
    runtime = minted_runtime()
    store = runtime.store if runtime is not None else None
    allow_private = False
    if store is not None:
        async with store.acquire() as conn:
            allow_private = await _private_allowed(conn, str(connector))
    material = _material_for(plan, credential_id, allow_private=allow_private)
    if store is not None:
        async with store.acquire() as conn:
            await _insert_minting(
                conn,
                credential_id=credential_id,
                owner_kind="test",
                owner_id=connector,
                connector=connector,
                plan=plan,
                access=access,
                material=material,
            )
    try:
        minted = await _mint(
            plan,
            credential_id,
            material,
            owner_label="test",
            connector_id=str(connector),
            access=access,
        )
    except ProviderError as exc:
        if store is not None:
            await _settle_failed_mint(store, credential_id, plan, material, exc)
        raise MintFailure(str(exc), permanent=not exc.transient) from None
    if plan.provider == PROVIDER_KUBERNETES:
        material["secret"]["uid"] = minted.handle
    credential = MintedCredential(
        id=str(credential_id),
        provider=plan.provider,
        token=minted.token,
        expires_at=_aware(minted.expires_at),
        access=access,
        material=material,
    )
    if store is not None:
        async with store.acquire() as conn:
            await conn.execute(
                """
                UPDATE connector_minted_credentials
                   SET status = 'revoking', revoke_requested_at = now(),
                       revoke_reason = 'test', revoke_next_at = now()
                           + make_interval(secs => $6::float8),
                       material_ciphertext = $2, token_ciphertext = $3,
                       token_last_four = $4, expires_at = $5, minted_at = now()
                 WHERE id = $1
                """,
                credential_id,
                _encrypt(json.dumps(material)),
                _encrypt(minted.token),
                last_four(minted.token),
                minted.expires_at,
                REVOKE_ROW_SECONDS,
            )

    async def revoke() -> bool:
        """Revoke now; ``False`` leaves it to the sweep."""
        try:
            await asyncio.wait_for(
                _provider_revoke(plan.provider, material, minted.token, None),
                REVOKE_ROW_SECONDS,
            )
        except (ProviderError, asyncio.TimeoutError, TimeoutError):
            if store is not None:
                async with store.acquire() as conn:
                    await conn.execute(
                        "UPDATE connector_minted_credentials SET revoke_next_at = "
                        "now() WHERE id = $1 AND status = 'revoking'",
                        credential_id,
                    )
                    await _notify(conn)
            return False
        if store is not None:
            async with store.acquire() as conn:
                await _mark_done(conn, credential_id, material, status="revoked")
        return True

    return credential, revoke


# =============================================================================
# Preparation and delivery
# =============================================================================


def _entry_access(entry: Mapping[str, Any], marker: Mapping[str, Any]) -> str:
    """The level a minting entry is minted at: the driver's level for it
    (clamped by its project link), clamped by the connector's own rule."""
    spec = driver_spec_for_row(entry) or REPOSITORY_SPEC
    return clamped_access(
        effective_access(entry, spec) or "ReadOnly",
        read_only=bool(marker.get("read_only")),
    )


def _refused_by_swap(entry: Mapping[str, Any]) -> bool:
    block = entry.get("git_swap")
    return isinstance(block, Mapping) and "unavailable" in block


def _key(owner: LeaseOwner, connector_id: str) -> tuple[str, str, str]:
    return (owner.kind, owner.id, str(connector_id).lower())


def _remember(owner: LeaseOwner, connector_id: str, result: Any) -> None:
    key = _key(owner, connector_id)
    if isinstance(result, MintFailure):
        if len(_prepared) >= _PREPARED_MAX:
            _prepared.clear()
        _prepared[key] = (time.monotonic(), result)
    else:
        _prepared.pop(key, None)


def _remembered(owner: LeaseOwner, connector_id: str) -> MintFailure | None:
    found = _prepared.get(_key(owner, connector_id))
    if found is None:
        return None
    since, failure = found
    stale = PREPARED_OUTCOME_SECONDS if failure.permanent else TRANSIENT_OUTCOME_SECONDS
    return None if time.monotonic() - since > stale else failure


def forget_connector(connector_id: Any) -> None:
    """A connector changed: what this process remembers of its mints'
    failures no longer holds."""
    wanted = str(connector_id).lower()
    for key in [key for key in _prepared if key[2] == wanted]:
        _prepared.pop(key, None)


async def _mint_and_remember(
    store: Any, owner: LeaseOwner, connector_id: str, access: str
) -> Any:
    try:
        result: Any = await ensure_minted(
            store, owner=owner, connector_id=connector_id, access=access
        )
    except MintFailure as exc:
        result = exc
        logger.warning(
            "Minting connector %s's credential for %s %s: %s",
            connector_id,
            owner.kind,
            owner.id,
            exc,
        )
    except Exception as exc:  # a store failure: the delivery says "later"
        logger.warning(
            "Minting connector %s's credential failed", connector_id, exc_info=True
        )
        result = MintFailure(
            f"the credential could not be prepared ({type(exc).__name__})",
            permanent=False,
        )
    _remember(owner, connector_id, result)
    return result


def _start_mint(
    store: Any, owner: LeaseOwner, connector_id: str, access: str
) -> asyncio.Task[Any]:
    """The mint of ``owner``'s credential for the connector: the one already
    running in this process, or a new one. Never cancelled by a waiter (a
    provider may already have minted, and only the mint records what it
    made); each provider call has its own deadline."""
    key = _key(owner, connector_id)
    loop = asyncio.get_running_loop()
    running = _inflight.get(key)
    if running is not None and not running.done() and running.get_loop() is loop:
        return running
    task = loop.create_task(_mint_and_remember(store, owner, connector_id, access))
    _inflight[key] = task
    _background.add(task)

    def settled(done: asyncio.Task[Any]) -> None:
        _background.discard(done)
        if _inflight.get(key) is done:
            del _inflight[key]
        if not done.cancelled() and done.exception() is not None:
            logger.info("A background mint failed: %s", done.exception())

    task.add_done_callback(settled)
    return task


async def _prepare_one(
    store: Any,
    owner: LeaseOwner,
    connector_id: str,
    access: str,
    *,
    wait: float | None,
) -> None:
    """Start (or join) the mint, unless a recent one failed, and wait for it
    at most ``wait`` seconds (``None``: :data:`PREPARE_MINT_SECONDS`; ``0``:
    not at all)."""
    if _remembered(owner, connector_id) is not None:
        # The delivery reports it; no new provider call until it is stale.
        return
    task = _start_mint(store, owner, connector_id, access)
    seconds = PREPARE_MINT_SECONDS if wait is None else min(wait, PREPARE_MINT_SECONDS)
    if seconds <= 0:
        return
    done, _ = await asyncio.wait({task}, timeout=seconds)
    if not done:
        _remember(
            owner,
            connector_id,
            MintFailure(
                "minting the credential took too long; it arrives at a later delivery",
                permanent=False,
            ),
        )


async def prepare_minted_entries(
    store: Any,
    entries: Sequence[Any] | None,
    *,
    owner: LeaseOwner,
    wait: float | None = None,
) -> None:
    """Mint what the delivery of ``entries`` will hand out, before its
    caller opens a transaction, waiting at most ``wait`` seconds for each
    (``None``: :data:`PREPARE_MINT_SECONDS`; ``0``, a dispatch or a job's
    claim: never, the mint runs in the background for a later delivery).
    A failure is remembered for the delivery. Never raises."""
    wanted = [
        (marker, entry)
        for entry in entries or ()
        if (marker := minted_marker(entry)) is not None and not _refused_by_swap(entry)
    ]
    if not wanted:
        return
    await asyncio.gather(
        *(
            _prepare_one(
                store,
                owner,
                str(marker["connector_id"]),
                _entry_access(entry, marker),
                wait=wait,
            )
            for marker, entry in wanted
        )
    )


_THREAD_TARGETS = f"""
SELECT d.id,
       COALESCE(d.read_only, false) OR COALESCE(d.is_global, false)
       OR COALESCE((SELECT BOOL_OR(pd.read_only) FROM project_datasources AS pd
                     WHERE pd.datasource_id = d.id
                       AND pd.project_id = ANY($2::uuid[])), false) AS read_only
  FROM datasources AS d
 WHERE d.id = ANY($1::uuid[])
   AND ((d.type = '{KUBECONFIG_SPEC.legacy_type}' AND d.config ? '{TOKEN_REQUEST_KEY}')
        OR (d.type = '{REPOSITORY_SPEC.legacy_type}' AND d.config ? '{GITHUB_APP_KEY}'))
"""


def _uuids(values: Sequence[Any]) -> list[UUID]:
    found = []
    for value in values:
        try:
            found.append(UUID(str(value)))
        except ValueError:
            continue
    return found


async def prepare_thread_minted(store: Any, thread_id: str) -> None:
    """:func:`prepare_minted_entries` for a session's stored selection, for
    the delivery paths that build their payload under the thread's
    datasource lock. The access level is read as the delivery reads it
    (``resolve_datasources_for_thread`` and the entry's marker): read-only
    when the connector is, or any link of it to the session's projects is.
    Never raises."""
    from orchestrator.services.thread_mount_rows import durable_project_ids

    try:
        owner = LeaseOwner.thread(str(UUID(str(thread_id))))
        thread = await store.get_thread(owner.id)
        if not thread:
            return
        metadata = stored_json_object(thread.get("metadata"))
        selected = _uuids(metadata.get("datasource_ids") or [])
        if not selected:
            return
        mounts = await store.list_thread_mounts(owner.id)
        projects = _uuids(durable_project_ids(thread, legacy_mounts=mounts))
        async with store.acquire() as conn:
            targets = await conn.fetch(_THREAD_TARGETS, selected, projects)
        if not targets:
            return
        await asyncio.gather(
            *(
                _prepare_one(
                    store,
                    owner,
                    str(target["id"]),
                    "ReadOnly" if target["read_only"] else "ReadWrite",
                    wait=None,
                )
                for target in targets
            )
        )
    except Exception:
        logger.warning(
            "Preparing the minted credentials of thread %s failed",
            thread_id,
            exc_info=True,
        )


def _mint_in_background(owner: LeaseOwner, connector_id: str, access: str) -> None:
    """A delivery found nothing prepared (a live update): mint for the next
    delivery, on the store's own connections, never this delivery's (one
    mint per owner and connector at a time)."""
    runtime = minted_runtime()
    if runtime is None:
        return
    _start_mint(runtime.store, owner, connector_id, access)


def _still_valid(row: Any, access: str) -> bool:
    """A live credential past half its life, with at least a minute left:
    handed out while its renewal runs in the background."""
    if row is None:
        return False
    if row["provider"] == PROVIDER_GITHUB_APP and row["access"] != access:
        return False
    expires_at = row["expires_at"]
    return (
        isinstance(expires_at, datetime)
        and (_aware(expires_at) - _now()).total_seconds() >= MIN_REMAINING_SECONDS
    )


async def _deliverable(
    conn: Any, owner: LeaseOwner, connector_id: str, access: str
) -> MintedCredential:
    """The live credential a delivery hands out, read on its own
    connection; never a provider call (see the module docstring). Without
    one its preparation made fresh, a mint starts in the background (unless
    a preparation just failed) and a live credential that is still valid
    is handed out meanwhile."""
    live = await _live_row(conn, owner, UUID(connector_id))
    if _usable(live, access):
        credential = _credential(live)
        if credential is not None:
            return credential
    failure = _remembered(owner, connector_id)
    if failure is None:
        _mint_in_background(owner, connector_id, access)
    if _still_valid(live, access):
        credential = _credential(live)
        if credential is not None:
            return credential
    if failure is not None:
        raise failure
    raise MintFailure(
        "the credential is being minted; it arrives at a later delivery",
        permanent=False,
    )


def _fill(
    entry: dict[str, Any], marker: Mapping[str, Any], minted: MintedCredential
) -> None:
    if minted.provider == PROVIDER_KUBERNETES:
        material = minted.material
        target = dict(marker.get("file") or {})
        target["contents"] = delivered_kubeconfig_text(
            server=str(material.get("server") or ""),
            ca_pem=material.get("ca"),
            tls_server_name=material.get("tls_server_name"),
            token=minted.token,
            context_namespace=material.get("context_namespace"),
            name=str(material.get("name") or "cluster"),
        )
        entry["credentials"] = {"files": [target]}
    else:
        # The installation's token-in-URL fallback: the one-hour,
        # one-repository token, never the App's key, under GitHub's
        # documented username for it.
        entry["credentials"] = {
            "auth_method": "token",
            "token": minted.token,
            "username": TOKEN_USERNAME,
            # Only a minted credential names its username (the agent honours
            # it for these alone).
            "minted": True,
        }


def _notice(exc: MintFailure) -> str:
    if exc.permanent:
        return f"Not delivered: {exc}"
    return (
        f"Not delivered yet: {exc} (SRW tries again; it arrives at a later "
        "attach or turn)"
    )


async def _retire_exposed(conn: Any, owner: LeaseOwner, connector_id: str) -> None:
    """The git swap driver now serves a GitHub App connector whose live
    token an earlier fallback put in the workspace's clone URL: revoke it,
    so the exchange mints one the workspace never saw."""
    await _request_revoke(
        conn,
        "m.owner_kind = $1 AND m.owner_id = $2 AND m.connector_id = $3 "
        "AND m.delivered_at IS NOT NULL",
        [owner.kind, UUID(owner.id), UUID(connector_id)],
        reason="exposed_before_swap",
    )


async def deliver_minted_entries(
    conn: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> int:
    """Fill every minting entry of a delivery, in place, inside its
    transaction (``conn``); returns how many were filled. Never calls a
    provider.

    A kubeconfig entry gets its file with the delivered kubeconfig; a GitHub
    App repository not served by the git swap driver gets the minted token
    (the installation's token-in-URL fallback; one the installation refuses
    gets nothing). One the driver serves already carries its lease: the
    exchange mints for it (and a token a fallback exposed before is
    revoked). A credential that is not ready skips a session's connector
    with a notice (its ``cli_hint``, the workspace README's line) and
    refuses a job's delivery, for a retry (:class:`BindTimePending`) or for
    good (:class:`BindTimeRefused`).
    """
    from orchestrator.services.connector_bind_time import (
        BindTimePending,
        BindTimeRefused,
    )

    wanted = [
        entry
        for entry in entries or ()
        if isinstance(entry, dict) and minted_marker(entry) is not None
    ]
    wanted.sort(key=lambda entry: str(minted_marker(entry)["connector_id"]).lower())
    filled = 0
    for entry in wanted:
        marker = dict(entry.pop(MINTED_KEY))
        connector_id = str(marker["connector_id"])
        if marker["provider"] == PROVIDER_GITHUB_APP:
            if git_swap_entry(entry):
                await _retire_exposed(conn, owner, connector_id)
                continue
            if _refused_by_swap(entry):
                continue
        label = str(entry.get("name") or connector_id)
        try:
            if not minting_enabled():
                raise MintFailure(DISABLED_DETAIL, permanent=True)
            minted = await _deliverable(
                conn, owner, connector_id, _entry_access(entry, marker)
            )
        except MintFailure as exc:
            if owner.kind == "thread":
                entry["credentials"] = {}
                entry["cli_hint"] = _notice(exc)
                continue
            if exc.permanent:
                raise BindTimeRefused(f"Connector {label}: {exc}") from None
            raise BindTimePending(f"Connector {label}: {exc}") from None
        _fill(entry, marker, minted)
        if minted.provider == PROVIDER_GITHUB_APP:
            await conn.execute(
                "UPDATE connector_minted_credentials SET delivered_at = "
                "COALESCE(delivered_at, now()) WHERE id = $1",
                UUID(minted.id),
            )
        filled += 1
    return filled


async def settle_background_mints(timeout: float | None = None) -> None:
    """Wait for the background mints this process started to record what
    they made: at shutdown, before the store closes, for at most
    ``timeout`` seconds (a mint still running then is cut off; its
    ``minting`` record is swept as an abandoned mint), and in tests."""
    loop = asyncio.get_running_loop()
    deadline = None if timeout is None else loop.time() + timeout
    while True:
        running = [task for task in _background if task.get_loop() is loop]
        if not running:
            return
        left = None if deadline is None else deadline - loop.time()
        if left is not None and left <= 0:
            logger.warning(
                "%d background mints still run at shutdown; their records are "
                "swept as abandoned mints",
                len(running),
            )
            return
        await asyncio.wait(running, timeout=left)


async def minted_lease_upstream(
    store: Any,
    row: Mapping[str, Any],
    *,
    owner: LeaseOwner,
    access: str,
) -> MintedCredential:
    """The installation token the lease exchange hands the git swap driver
    for a GitHub App connector's lease (minted again past half its hour)."""
    return await ensure_minted(
        store, owner=owner, connector_id=str(row.get("id")), access=access, row=row
    )


# =============================================================================
# Revoke requests (inside the caller's transaction)
# =============================================================================


async def _request_revoke(
    conn: Any, where: str, args: Sequence[Any], *, reason: str
) -> int:
    """Move the credentials ``where`` selects (on ``m``) to ``revoking``;
    a row still minting is revoked by its minter once the provider
    answered."""
    reason_at = len(args) + 1
    rows = await conn.fetch(
        f"""
        UPDATE connector_minted_credentials AS m
           SET status = 'revoking', revoke_requested_at = now(),
               revoke_reason = ${reason_at}::text, revoke_next_at = now()
         WHERE m.status IN {_ACTIVE} AND ({where})
        RETURNING m.id
        """,
        *args,
        reason,
    )
    if rows:
        await _notify(conn)
    return len(rows)


async def revoke_owner_credentials(
    conn: Any,
    *,
    kind: str,
    owner_id: Any,
    reason: str,
    connector_ids: Sequence[str] | None = None,
) -> int:
    """Request the revoke of an execution's credentials (of ``connector_ids``
    only, when given: a live detach), with the caller's decision. An id
    that is no uuid owns nothing here."""
    owners = _uuids([owner_id])
    if not owners:
        return 0
    args: list[Any] = [kind, owners[0]]
    where = "m.owner_kind = $1 AND m.owner_id = $2"
    if connector_ids is not None:
        ids = _uuids(connector_ids)
        if not ids:
            return 0
        args.append(ids)
        where += " AND m.connector_id = ANY($3::uuid[])"
    return await _request_revoke(conn, where, args, reason=reason)


async def revoke_connector_credentials(
    conn: Any, *, connector_id: Any, reason: str
) -> int:
    """Request the revoke of every credential of one connector."""
    connectors = _uuids([connector_id])
    if not connectors:
        return 0
    return await _request_revoke(conn, "m.connector_id = $1", connectors, reason=reason)


async def connector_changed(conn: Any, connector_id: Any) -> int:
    """A connector's minting inputs changed: what was minted with the old
    ones is revoked, and each execution's next delivery mints anew."""
    forget_connector(connector_id)
    return await revoke_connector_credentials(
        conn, connector_id=connector_id, reason="connector_changed"
    )


_OWNER_ENDED = f"""(
    m.owner_kind = 'test'
    OR (m.owner_kind = 'job' AND NOT EXISTS (
        SELECT 1 FROM jobs AS j
         WHERE j.id = m.owner_id
           AND j.status::text NOT IN {_TERMINAL_JOB_STATUSES!r}))
    OR (m.owner_kind = 'thread' AND NOT EXISTS (
        SELECT 1 FROM threads AS t
         WHERE t.id = m.owner_id AND t.status::text <> 'ended'
           AND NOT (t.runtime_retirement_token IS NOT NULL
                    AND t.runtime_retirement_authorized_at IS NOT NULL)))
)"""


async def revoke_terminal_owner_credentials(conn: Any, *, owner: LeaseOwner) -> int:
    """The idempotent backstop at a workspace's teardown: request the revoke
    of an execution's credentials only when it is terminal or gone."""
    owners = _uuids([owner.id])
    if not owners:
        return 0
    return await _request_revoke(
        conn,
        f"m.owner_kind = $1 AND m.owner_id = $2 AND {_OWNER_ENDED}",
        [owner.kind, owners[0]],
        reason="execution_terminal",
    )


# =============================================================================
# The sweep
# =============================================================================


@dataclass
class SweepReport:
    ended: int = 0
    expired: int = 0
    abandoned: int = 0
    revoked: int = 0
    retried: int = 0
    given_up: int = 0
    pruned: int = 0

    def any(self) -> bool:
        return any(vars(self).values())


class _Unreadable(Exception):
    """A record SRW cannot revoke from: retrying changes nothing."""


def _backoff(attempt: int) -> float:
    return min(
        REVOKE_RETRY_BASE_SECONDS * 2 ** max(0, attempt - 1), REVOKE_RETRY_MAX_SECONDS
    )


async def _provider_revoke(
    provider: str,
    material: Mapping[str, Any],
    token: str | None,
    expires_at: datetime | None,
) -> None:
    """Revoke one credential at its provider; ``ProviderError`` when it
    could not, :class:`_Unreadable` when the record cannot say how."""
    allow_private = bool(material.get("allow_private"))
    if provider == PROVIDER_GITHUB_APP:
        if not token or (expires_at is not None and _aware(expires_at) <= _now()):
            return  # nothing minted, or GitHub no longer accepts it
        if not material.get("api_base"):
            raise _Unreadable("the record names no API base")
        from orchestrator.services.connector_drivers.github_app import (
            revoke_installation_token,
        )

        await revoke_installation_token(
            str(material["api_base"]),
            token,
            ca_pem=material.get("ca"),
            allow_private=allow_private,
        )
        return
    from orchestrator.services.connector_drivers.token_request import (
        delete_bound_secret,
    )

    secret = material.get("secret") if isinstance(material.get("secret"), dict) else {}
    if (
        not secret.get("name")
        or not material.get("server")
        or not material.get("token")
    ):
        raise _Unreadable("the record names no bound Secret or minting credential")
    minting = MintingKubeconfig(
        server=str(material["server"]),
        ca_pem=material.get("ca"),
        tls_server_name=material.get("tls_server_name"),
        token=str(material["token"]),
        context_namespace=None,
        cluster_name="",
    )
    await delete_bound_secret(
        minting,
        namespace=str(material.get("namespace") or ""),
        name=str(secret["name"]),
        uid=secret.get("uid"),
        allow_private=allow_private,
    )


async def _mark_done(
    conn: Any,
    credential_id: Any,
    material: Mapping[str, Any] | None,
    *,
    status: str,
    error: str | None = None,
    attempts: int | None = None,
) -> bool:
    """A row reaches ``revoked`` or ``abandoned``: it keeps no token and no
    minting credential from then on."""
    done = await conn.fetchval(
        """
        UPDATE connector_minted_credentials
           SET status = $2, revoked_at = now(), token_ciphertext = NULL,
               material_ciphertext = $3, revoke_error = $4,
               revoke_attempts = COALESCE($5, revoke_attempts)
         WHERE id = $1 AND status IN ('revoking', 'minting')
        RETURNING id
        """,
        credential_id,
        status,
        _stripped(material),
        error,
        attempts,
    )
    return done is not None


async def _give_up(
    store: Any,
    row: Mapping[str, Any],
    material: Mapping[str, Any] | None,
    *,
    why: str,
    attempts: int,
    report: SweepReport,
) -> None:
    """Stop revoking: a GitHub token past its expiry is dead (``revoked``);
    a Kubernetes Secret SRW could not delete, or a record it cannot read, is
    ``abandoned``. Logged and audited."""
    dead = (
        row["provider"] == PROVIDER_GITHUB_APP
        and isinstance(row["expires_at"], datetime)
        and _aware(row["expires_at"]) <= _now()
    )
    status = "revoked" if dead else "abandoned"
    logger.warning(
        "Giving up revoking minted credential %s (%s, connector %s) after %d "
        "attempts: %s%s",
        row["id"],
        row["provider"],
        row["connector_id"],
        attempts,
        why,
        "" if dead else "; what it named may be left at the provider",
    )
    async with store.acquire() as conn:
        await _mark_done(
            conn, row["id"], material, status=status, error=why[:500], attempts=attempts
        )
        await record_lease_event(
            conn,
            event_type="connector_minted_revoke_abandoned",
            resource_type="connector_minted_credential",
            resource_id=str(row["id"]),
            detail=_detail(
                connector=str(row["connector_id"]),
                provider=row["provider"],
                reason=row["revoke_reason"],
                attempts=attempts,
                status=status,
                error=why[:200],
            ),
        )
    report.given_up += 1


#: Provider errors the record's own inputs cause (its CA does not load):
#: another attempt with the same record changes nothing.
_NEVER_RETRIED = frozenset({"ca_unusable"})


async def _retry_or_give_up(
    store: Any,
    row: Mapping[str, Any],
    material: Mapping[str, Any] | None,
    why: str,
    attempts: int,
    report: SweepReport,
) -> str:
    """A revoke that failed and may pass later. While the credential is
    valid at the provider SRW keeps trying (at most an hour apart); once it
    has expired (or was never minted), what is left is housekeeping: a
    GitHub token is dead, a bound Secret gets the attempts that remain."""
    expires = row["expires_at"]
    live = isinstance(expires, datetime) and _aware(expires) > _now()
    if not live and (
        row["provider"] == PROVIDER_GITHUB_APP or attempts >= MAX_REVOKE_ATTEMPTS
    ):
        await _give_up(store, row, material, why=why, attempts=attempts, report=report)
        return "given_up"
    async with store.acquire() as conn:
        await conn.execute(
            """
            UPDATE connector_minted_credentials
               SET revoke_attempts = $2, revoke_error = $3,
                   revoke_next_at = now() + make_interval(secs => $4::float8)
             WHERE id = $1 AND status = 'revoking'
            """,
            row["id"],
            attempts,
            why[:500],
            _backoff(attempts),
        )
    report.retried += 1
    logger.warning(
        "Revoking minted credential %s (%s) failed, attempt %d: %s",
        row["id"],
        row["provider"],
        attempts,
        why,
    )
    return "retried"


async def _revoke_one(store: Any, row: Mapping[str, Any], report: SweepReport) -> str:
    """One revoke: ``revoked``, ``retried`` (later, with backoff) or
    ``given_up``."""
    material = _material(row)
    attempts = int(row["revoke_attempts"]) + 1
    try:
        if material is None:
            raise _Unreadable("the record's material does not decrypt")
        token = _decrypt(row["token_ciphertext"])
        if row["token_ciphertext"] and token is None:
            raise _Unreadable("the record's token does not decrypt")
        await asyncio.wait_for(
            _provider_revoke(row["provider"], material, token, row["expires_at"]),
            REVOKE_ROW_SECONDS,
        )
    except _Unreadable as exc:
        await _give_up(
            store, row, material, why=str(exc), attempts=attempts, report=report
        )
        return "given_up"
    except ProviderError as exc:
        if exc.reason in _NEVER_RETRIED:
            await _give_up(
                store, row, material, why=str(exc), attempts=attempts, report=report
            )
            return "given_up"
        return await _retry_or_give_up(store, row, material, str(exc), attempts, report)
    except (asyncio.TimeoutError, TimeoutError):
        return await _retry_or_give_up(
            store, row, material, "the revoke took too long", attempts, report
        )
    async with store.acquire() as conn:
        done = await _mark_done(conn, row["id"], material, status="revoked")
        if done and row["revoke_reason"] not in ("expired", "test"):
            await record_lease_event(
                conn,
                event_type="connector_minted_credential_revoked",
                resource_type="connector_minted_credential",
                resource_id=str(row["id"]),
                detail=_detail(
                    owner=f"{row['owner_kind']}:{row['owner_id']}",
                    connector=str(row["connector_id"]),
                    provider=row["provider"],
                    reason=row["revoke_reason"],
                    token_last_four=row["token_last_four"],
                ),
            )
    if done:
        report.revoked += 1
    return "revoked"


async def pending(store: Any) -> bool:
    """Whether any credential may still need a provider call."""
    async with store.acquire() as conn:
        # Each half on its partial index: done rows are never scanned.
        return bool(
            await conn.fetchval(
                "SELECT EXISTS (SELECT 1 FROM connector_minted_credentials "
                "WHERE status IN ('minting', 'revoking')) "
                "OR EXISTS (SELECT 1 FROM connector_minted_credentials "
                "WHERE status IN ('live', 'superseded'))"
            )
        )


async def prune(store: Any) -> int:
    """Delete done rows past :data:`RETENTION_DAYS`."""
    async with store.acquire() as conn:
        pruned = await conn.fetch(
            f"""
            DELETE FROM connector_minted_credentials
             WHERE status IN {_DONE}
               AND revoked_at < now() - make_interval(days => $1::int)
            RETURNING id
            """,
            RETENTION_DAYS,
        )
    return len(pruned)


async def sweep_minted_once(store: Any, *, prune_too: bool = True) -> SweepReport:
    """One pass: request the revoke of credentials whose execution ended or
    is gone, of expired ones and of abandoned mints; then revoke what is due
    at the provider (at most :data:`REVOKES_PER_PASS`,
    :data:`REVOKE_CONCURRENCY` at once, each bounded), and prune done rows
    past :data:`RETENTION_DAYS`."""
    report = SweepReport()
    async with store.acquire() as conn:
        report.ended = await _request_revoke(
            conn,
            f"m.status IN ('live', 'superseded') AND {_OWNER_ENDED}",
            [],
            reason="execution_ended",
        )
        report.expired = await _request_revoke(
            conn,
            "m.status IN ('live', 'superseded') AND m.expires_at <= now()",
            [],
            reason="expired",
        )
        report.abandoned = await _request_revoke(
            conn,
            "m.status = 'minting' "
            "AND m.created_at < now() - make_interval(secs => $1::int)",
            [MINT_ABANDON_SECONDS],
            reason="mint_abandoned",
        )
        # A row still minting (its provider call in flight) is left to the
        # minter, which records what the provider made and keeps it revoking.
        # Fair across provider hosts: each host's longest-due first, then
        # each host's next, so one host's backlog never fills the window.
        due = await conn.fetch(
            """
            SELECT * FROM (
                SELECT id, owner_kind, owner_id, connector_id, provider,
                       provider_host, material_ciphertext, token_ciphertext,
                       token_last_four, expires_at, revoke_reason,
                       revoke_attempts, revoke_next_at, created_at,
                       row_number() OVER (
                           PARTITION BY provider_host
                           ORDER BY revoke_next_at NULLS FIRST, created_at, id
                       ) AS host_rank
                  FROM connector_minted_credentials
                 WHERE status = 'revoking'
                   AND COALESCE(revoke_next_at, now()) <= now()
                   AND (minted_at IS NOT NULL
                        OR created_at < now() - make_interval(secs => $1::int))
            ) AS ranked
             ORDER BY host_rank, revoke_next_at NULLS FIRST, created_at, id
             LIMIT $2
            """,
            MINT_ABANDON_SECONDS,
            REVOKES_PER_PASS,
        )
    by_host: dict[str, list[Mapping[str, Any]]] = {}
    for row in due:
        by_host.setdefault(str(row["provider_host"]), []).append(row)
    gate = asyncio.Semaphore(REVOKE_CONCURRENCY)

    async def host(rows: list[Mapping[str, Any]]) -> None:
        # One revoke per host at a time; a host that did not answer keeps
        # its other rows for the next pass.
        async with gate:
            for row in rows:
                try:
                    outcome = await _revoke_one(store, row, report)
                except Exception:
                    logger.warning(
                        "Revoking minted credential %s failed", row["id"], exc_info=True
                    )
                    return
                if outcome == "retried":
                    return

    await asyncio.gather(*(host(rows) for rows in by_host.values()))
    if prune_too:
        report.pruned = await prune(store)
    return report


async def connector_minted_credential_sweeper(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    interval_seconds: float | None = None,
) -> None:
    """Leader-gated loop: :func:`sweep_minted_once` every interval (the
    lease sweep's, by default) while any credential may need a provider
    call, and soon after a revoke request commits (it NOTIFYs
    :data:`REVOKE_CHANNEL`, which this loop LISTENs on only while there is
    something to revoke: an idle installation holds no connection).

    Best effort: a failed pass is logged and the next one runs on time;
    every step is re-derived from durable state, so a pass a leadership
    change cancels is run again by the next leader.
    """
    from orchestrator.services.connector_service_hosting import _listen, _until

    interval = float(interval_seconds or lease_sweep_seconds())
    logger.info("Connector minted-credential sweeper started (every %.0fs)", interval)
    wake = asyncio.Event()
    listener: asyncio.Task[Any] | None = None
    pruned_at = 0.0

    async def stop_listening() -> None:
        nonlocal listener
        if listener is not None:
            listener.cancel()
            try:
                await listener
            except (asyncio.CancelledError, Exception):
                pass
            listener = None

    try:
        while not shutdown_event.is_set():
            wake.clear()
            try:
                busy = await pending(store)
                if busy and listener is None:
                    listener = asyncio.create_task(
                        _listen(store, wake, shutdown_event, channel=REVOKE_CHANNEL)
                    )
                elif not busy:
                    await stop_listening()
                prune_due = time.monotonic() - pruned_at >= PRUNE_SECONDS
                if busy:
                    report = await sweep_minted_once(store, prune_too=prune_due)
                    if report.any():
                        logger.info("connector minted credentials: %s", vars(report))
                elif prune_due:
                    await prune(store)
                if prune_due:
                    pruned_at = time.monotonic()
            except Exception as exc:
                logger.warning("connector minted-credential sweep error: %s", exc)
            await _until((shutdown_event, wake), interval)
    finally:
        await stop_listening()
    logger.info("Connector minted-credential sweeper stopped")


__all__ = [
    "DISABLED_DETAIL",
    "MINTED_KEY",
    "MintFailure",
    "MintedCredential",
    "MintedRuntime",
    "PROVIDER_GITHUB_APP",
    "PROVIDER_KUBERNETES",
    "REVOKE_CHANNEL",
    "SweepReport",
    "configure_minted_credentials",
    "connector_changed",
    "clamped_access",
    "connector_minted_credential_sweeper",
    "connector_read_only",
    "deliver_minted_entries",
    "ensure_minted",
    "forget_connector",
    "fresh",
    "github_app_marker",
    "kubeconfig_marker",
    "minted_lease_upstream",
    "minted_marker",
    "mint_for_test",
    "minted_runtime",
    "minting_enabled",
    "pending",
    "plan_for",
    "prepare_minted_entries",
    "prepare_thread_minted",
    "prune",
    "revoke_connector_credentials",
    "revoke_owner_credentials",
    "revoke_terminal_owner_credentials",
    "row_provider",
    "settle_background_mints",
    "sweep_minted_once",
]
