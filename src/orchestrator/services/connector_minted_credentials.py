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

**Records.** ``connector_minted_credentials`` holds one row per credential:
its owner (the workspace-owning execution, as for C2's leases: a child Job
on its parent's workspace uses the parent's), connector, provider, access
level and a digest of the minting inputs, the token and what its revoke
needs (the bound Secret and the minting credential, or the API base) as
``APP_ENCRYPTION_KEY`` ciphertexts. A row is written *before* the provider
call (``minting``), so a crash mid-mint leaves a record the sweep revokes.
Owners and connectors carry no foreign key: a revoke outlives them.

**Delivery.** One row per owner and connector is ``live``. Every delivery
(a claim, an attach, a dispatch) hands out the live credential while more
than half of its lifetime is left, so a stateless session's turns and a
pod recycle receive the same token; past that, the delivery mints afresh
and the old one becomes ``superseded``: still valid at the provider until
its own expiry (another work item on the same workspace may still hold
it), then revoked by the sweep. A change of access level or of the
connector's minting inputs revokes the old one at once instead.
:func:`prepare_minted_entries` mints before the delivery's transaction;
:func:`deliver_minted_entries` fills the entries inside it, minting inline
(bounded by :data:`INLINE_MINT_SECONDS`) only when the preparation found
nothing. A session whose credential cannot be minted gets the connector
without it and a notice; a job's delivery waits (a provider that did not
answer) or fails (one that refused), as D6's binds do.

**Renewal reaches a running execution only where SRW delivers again**: a
stateless session at every turn's claim, a stateless job at every worker
batch's claim, a pinned session at an attach, a pod recycle or a live
connector update, a job at a re-dispatch after a pause. Nothing pushes a
fresh file into a pinned execution mid-run: a single pinned run or turn
that outlasts ``expiration_seconds`` (Kubernetes) or an hour (GitHub, token
in URL) loses access until its next delivery. Through the git swap driver
the exchange mints again whenever the driver asks, so a GitHub App
connector served by the driver never lapses while its lease lives.

**Revocation** is requested inside the terminal transactions C2's revoke
points already run (End, cancel, delete, completion, a live detach, a
connector delete: ``connector_credential_leases`` calls
:func:`revoke_owner_credentials` and :func:`revoke_connector_credentials`),
and when a connector's config or credentials change
(:func:`connector_changed`). A request is an UPDATE to ``revoking`` that
commits with the decision and NOTIFYs; the leader's
:func:`connector_minted_credential_sweeper` then makes the provider call,
retrying with backoff. The same sweep revokes credentials whose execution
is terminal or gone (a backstop), expired ones (a paused job's lapse: its
bound Secret is deleted so nothing is left), and mints a crash abandoned.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Three ways
to give an agent ephemeral authority", "The lease service", "The git swap
driver", "Today's types as drivers"; slice C5.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
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
#: The most a delivery's transaction waits for a mint its preparation did
#: not make (the git swap check's inline budget is 4 s; a mint is two calls).
INLINE_MINT_SECONDS = 8.0
#: The most a delivery's preparation waits for one connector's mint.
PREPARE_MINT_SECONDS = 15.0
#: A ``minting`` row older than this was abandoned (a crash, a cancel).
MINT_ABANDON_SECONDS = 300
REVOKES_PER_PASS = 20
MAX_REVOKE_ATTEMPTS = 12
REVOKE_RETRY_BASE_SECONDS = 30.0
REVOKE_RETRY_MAX_SECONDS = 3600.0
RETENTION_DAYS = 30
#: Revoke requests NOTIFY this channel at commit; the leader's sweep LISTENs.
REVOKE_CHANNEL = "srw_connector_minted_revoke"
_TERMINAL_JOB_STATUSES = ("completed", "failed", "cancelled")
_ACTIVE = "('minting', 'live', 'superseded')"


class MintFailure(Exception):
    """A credential could not be minted. ``permanent``: the provider (or the
    connector's config) refused it, so trying again changes nothing until
    the connector changes."""

    def __init__(self, message: str, *, permanent: bool) -> None:
        super().__init__(message)
        self.permanent = permanent


@dataclass(frozen=True)
class MintedRuntime:
    """What deliveries outside a preparation mint with: the application's
    store (a mint is recorded on its own connections, never a delivery's
    transaction)."""

    store: Any


_state: dict[str, MintedRuntime | None] = {"runtime": None}


def configure_minted_credentials(runtime: MintedRuntime | None) -> None:
    _state["runtime"] = runtime


def minted_runtime() -> MintedRuntime | None:
    return _state["runtime"]


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
        "file": target,
    }


def github_app_marker(row: Mapping[str, Any]) -> dict[str, Any]:
    return {"provider": PROVIDER_GITHUB_APP, "connector_id": str(row.get("id") or "")}


# =============================================================================
# Plans: what one connector mints with
# =============================================================================


@dataclass(frozen=True)
class _Plan:
    provider: str
    digest: str
    #: What delivery and revoke need, encrypted on the row (secret).
    material: dict[str, Any] = field(repr=False)
    kubernetes: Any = field(default=None, repr=False)
    github: Any = field(default=None, repr=False)


def _digest(value: Mapping[str, Any]) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _secret_digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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
            material,
            kubernetes=(minting, options),
        )
    if provider == PROVIDER_GITHUB_APP:
        from orchestrator.services.connector_drivers.github_app import (
            normalize_private_key,
        )

        try:
            options = parse_github_app(config, row.get("connection_url"))
            key = normalize_private_key(credentials.get("private_key"))
        except (GitHubAppConfigError, ValueError) as exc:
            raise MintFailure(str(exc), permanent=True) from None
        inputs = {
            "api_base": options.api_base,
            "app_id": options.app_id,
            "installation_id": options.installation_id,
            "repository": f"{options.owner}/{options.repository}",
            "key": _secret_digest(key),
        }
        material = {
            "api_base": options.api_base,
            "repository": f"{options.owner}/{options.repository}",
        }
        return _Plan(
            PROVIDER_GITHUB_APP, _digest(inputs), material, github=(options, key)
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


def _material(row: Mapping[str, Any]) -> dict[str, Any]:
    text = _decrypt(row["material_ciphertext"])
    try:
        value = json.loads(text) if text else None
    except ValueError:
        value = None
    return value if isinstance(value, dict) else {}


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
       token_ciphertext, expires_at, minted_at
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
    if not token:
        return None
    return MintedCredential(
        id=str(row["id"]),
        provider=str(row["provider"]),
        token=token,
        expires_at=_aware(row["expires_at"]),
        access=str(row["access"]),
        material=_material(row),
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
    owner: LeaseOwner,
    connector_id: str,
    access: str,
) -> MintedToken:
    if plan.provider == PROVIDER_KUBERNETES:
        from orchestrator.services.connector_drivers.token_request import mint_token

        minting, options = plan.kubernetes
        return await mint_token(
            minting,
            options,
            secret=material["secret"]["name"],
            credential_id=credential_id,
            annotations={
                "srw.io/owner": f"{owner.kind}:{owner.id}",
                "srw.io/connector": connector_id,
            },
        )
    from orchestrator.services.connector_drivers.github_app import (
        mint_installation_token,
    )

    options, key = plan.github
    return await mint_installation_token(options, key, access)


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
    what was minted at the provider. Raises :class:`MintFailure`.
    """
    try:
        connector = UUID(str(connector_id))
    except ValueError:
        raise MintFailure("The entry names no connector", permanent=True) from None
    if row is None:
        row = await store.get_datasource(str(connector))
        if row is None:
            raise MintFailure("The connector no longer exists", permanent=True)
    plan = plan_for(row)
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
        material = dict(plan.material)
        if plan.provider == PROVIDER_KUBERNETES:
            material["secret"] = {"name": secret_name(credential_id), "uid": None}
        await conn.execute(
            """
            INSERT INTO connector_minted_credentials
                (id, owner_kind, owner_id, connector_id, provider, access,
                 config_digest, material_ciphertext)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
            credential_id,
            owner.kind,
            UUID(owner.id),
            connector,
            plan.provider,
            access,
            plan.digest,
            _encrypt(json.dumps(material)),
        )
    try:
        minted = await _mint(
            plan,
            credential_id,
            material,
            owner=owner,
            connector_id=str(connector),
            access=access,
        )
    except ProviderError as exc:
        await _settle_failed_mint(store, credential_id, plan.provider, str(exc))
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
    store: Any, credential_id: UUID, provider: str, message: str
) -> None:
    """A refused mint: a Kubernetes one may have left its Secret (the
    sweep deletes it by name); a GitHub one minted nothing."""
    try:
        async with store.acquire() as conn:
            if provider == PROVIDER_KUBERNETES:
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoking', revoke_requested_at = now(),
                           revoke_reason = 'mint_failed', revoke_next_at = now(),
                           revoke_error = $2
                     WHERE id = $1 AND status = 'minting'
                    """,
                    credential_id,
                    message[:500],
                )
            else:
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoked', revoke_requested_at = now(),
                           revoke_reason = 'mint_failed', revoked_at = now(),
                           revoke_error = $2
                     WHERE id = $1 AND status = 'minting'
                    """,
                    credential_id,
                    message[:500],
                )
    except Exception:
        logger.warning(
            "Recording a failed mint %s failed; the sweep retires it",
            credential_id,
            exc_info=True,
        )


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
    mint won (that one is returned) or the execution ended (or detached the
    connector) meanwhile: then the mint is recorded for its revoke and
    ``None`` is returned."""
    token_ciphertext = _encrypt(minted.token)
    material_ciphertext = _encrypt(json.dumps(material))
    async with store.acquire() as conn:
        async with conn.transaction():
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
                    credential_id,
                    material_ciphertext,
                    token_ciphertext,
                    last_four(minted.token),
                    minted.expires_at,
                )
                await _notify(conn)
                # Committed with the transaction; the caller refuses.
                return None
            winner = (
                _credential(current) if _usable(current, access, plan.digest) else None
            )
            if winner is not None:
                # Another delivery minted one meanwhile: keep it, retire ours.
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoking', revoke_requested_at = now(),
                           revoke_reason = 'mint_raced', revoke_next_at = now(),
                           material_ciphertext = $2, token_ciphertext = $3,
                           token_last_four = $4, expires_at = $5, minted_at = now()
                     WHERE id = $1
                    """,
                    credential_id,
                    material_ciphertext,
                    token_ciphertext,
                    last_four(minted.token),
                    minted.expires_at,
                )
                await _notify(conn)
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
            await conn.execute(
                """
                UPDATE connector_minted_credentials
                   SET status = 'live', material_ciphertext = $2,
                       token_ciphertext = $3, token_last_four = $4,
                       expires_at = $5, minted_at = now()
                 WHERE id = $1
                """,
                credential_id,
                material_ciphertext,
                token_ciphertext,
                last_four(minted.token),
                minted.expires_at,
            )
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
    return MintedCredential(
        id=str(credential_id),
        provider=plan.provider,
        token=minted.token,
        expires_at=_aware(minted.expires_at),
        access=access,
        material=material,
    )


# =============================================================================
# Preparation and delivery
# =============================================================================


def _entry_access(entry: Mapping[str, Any]) -> str:
    spec = driver_spec_for_row(entry) or REPOSITORY_SPEC
    return effective_access(entry, spec) or "ReadOnly"


def _refused_by_swap(entry: Mapping[str, Any]) -> bool:
    block = entry.get("git_swap")
    return isinstance(block, Mapping) and "unavailable" in block


async def _bounded(awaitable: Any, seconds: float) -> Any:
    try:
        return await asyncio.wait_for(awaitable, seconds)
    except (asyncio.TimeoutError, TimeoutError):
        raise MintFailure(
            "minting the credential took too long; it arrives at a later delivery",
            permanent=False,
        ) from None


async def prepare_minted_entries(
    store: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> None:
    """Mint what the delivery of ``entries`` will hand out, before its
    caller opens a transaction (each at most :data:`PREPARE_MINT_SECONDS`).
    Never raises: the delivery applies the outcome."""
    wanted = [
        (marker, entry)
        for entry in entries or ()
        if (marker := minted_marker(entry)) is not None and not _refused_by_swap(entry)
    ]
    if not wanted:
        return
    results = await asyncio.gather(
        *(
            _bounded(
                ensure_minted(
                    store,
                    owner=owner,
                    connector_id=str(marker["connector_id"]),
                    access=_entry_access(entry),
                ),
                PREPARE_MINT_SECONDS,
            )
            for marker, entry in wanted
        ),
        return_exceptions=True,
    )
    for (marker, _entry), result in zip(wanted, results):
        if isinstance(result, BaseException):
            logger.warning(
                "Preparing connector %s's %s credential for %s %s: %s",
                marker["connector_id"],
                marker["provider"],
                owner.kind,
                owner.id,
                result,
            )


_THREAD_TARGETS = f"""
SELECT d.id, COALESCE(pd.read_only, false) AS read_only
  FROM threads AS t
  JOIN datasources AS d
    ON COALESCE(t.metadata->'datasource_ids', '[]'::jsonb) ? d.id::text
  LEFT JOIN project_datasources AS pd
    ON pd.datasource_id = d.id AND pd.project_id = t.project_id
 WHERE t.id = $1
   AND ((d.type = '{KUBECONFIG_SPEC.legacy_type}' AND d.config ? '{TOKEN_REQUEST_KEY}')
        OR (d.type = '{REPOSITORY_SPEC.legacy_type}' AND d.config ? '{GITHUB_APP_KEY}'))
"""


async def prepare_thread_minted(store: Any, thread_id: str) -> None:
    """:func:`prepare_minted_entries` for a session's stored selection (its
    project link's access, as D6's binds read it): for the delivery paths
    that build their payload under the thread's datasource lock. Never
    raises."""
    try:
        owner = LeaseOwner.thread(str(UUID(str(thread_id))))
        async with store.acquire() as conn:
            targets = await conn.fetch(_THREAD_TARGETS, UUID(owner.id))
        if not targets:
            return
        await asyncio.gather(
            *(
                _bounded(
                    ensure_minted(
                        store,
                        owner=owner,
                        connector_id=str(target["id"]),
                        access="ReadOnly" if target["read_only"] else "ReadWrite",
                    ),
                    PREPARE_MINT_SECONDS,
                )
                for target in targets
            ),
            return_exceptions=True,
        )
    except Exception:
        logger.warning(
            "Preparing the minted credentials of thread %s failed",
            thread_id,
            exc_info=True,
        )


async def _deliverable(
    conn: Any, owner: LeaseOwner, connector_id: str, access: str
) -> MintedCredential:
    live = await _live_row(conn, owner, UUID(connector_id))
    if _usable(live, access):
        credential = _credential(live)
        if credential is not None:
            return credential
    runtime = minted_runtime()
    if runtime is None:
        raise MintFailure(
            "this orchestrator does not mint connector credentials", permanent=False
        )
    return await _bounded(
        ensure_minted(
            runtime.store, owner=owner, connector_id=connector_id, access=access
        ),
        INLINE_MINT_SECONDS,
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
        # one-repository token, never the App's key.
        entry["credentials"] = {"auth_method": "token", "token": minted.token}


def _notice(exc: MintFailure) -> str:
    if exc.permanent:
        return f"Not delivered: {exc}"
    return (
        f"Not delivered yet: {exc} (SRW tries again; it arrives at a later "
        "attach or turn)"
    )


async def deliver_minted_entries(
    conn: Any, entries: Sequence[Any] | None, *, owner: LeaseOwner
) -> int:
    """Fill every minting entry of a delivery, in place, inside its
    transaction (``conn``); returns how many were filled.

    A kubeconfig entry gets its file with the delivered kubeconfig; a GitHub
    App repository not served by the git swap driver gets the minted token
    (the installation's token-in-URL fallback; one the installation refuses
    gets nothing). One the driver serves already carries its lease: the
    exchange mints for it. A credential that cannot be minted skips a
    session's connector with a notice (its ``cli_hint``, the workspace
    README's line) and refuses a job's delivery, for a retry
    (:class:`BindTimePending`) or for good (:class:`BindTimeRefused`).
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
        if marker["provider"] == PROVIDER_GITHUB_APP and (
            git_swap_entry(entry) or _refused_by_swap(entry)
        ):
            continue
        try:
            minted = await _deliverable(conn, owner, connector_id, _entry_access(entry))
        except MintFailure as exc:
            label = str(entry.get("name") or connector_id)
            if owner.kind == "thread":
                entry["credentials"] = {}
                entry["cli_hint"] = _notice(exc)
                continue
            if exc.permanent:
                raise BindTimeRefused(f"Connector {label}: {exc}") from None
            raise BindTimePending(f"Connector {label}: {exc}") from None
        _fill(entry, marker, minted)
        filled += 1
    return filled


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


def _uuids(values: Sequence[Any]) -> list[UUID]:
    found = []
    for value in values:
        try:
            found.append(UUID(str(value)))
        except ValueError:
            continue
    return found


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
    """A connector's config or credentials changed: what was minted with the
    old ones is revoked, and each execution's next delivery mints anew."""
    return await revoke_connector_credentials(
        conn, connector_id=connector_id, reason="connector_changed"
    )


_OWNER_ENDED = f"""(
    (m.owner_kind = 'job' AND NOT EXISTS (
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


def _backoff(attempt: int) -> float:
    return min(
        REVOKE_RETRY_BASE_SECONDS * 2 ** max(0, attempt - 1), REVOKE_RETRY_MAX_SECONDS
    )


async def _provider_revoke(row: Mapping[str, Any]) -> None:
    """Revoke one credential at its provider; ``ProviderError`` when it
    could not."""
    material = _material(row)
    if row["provider"] == PROVIDER_GITHUB_APP:
        expires = row["expires_at"]
        token = _decrypt(row["token_ciphertext"])
        if not token or (isinstance(expires, datetime) and _aware(expires) <= _now()):
            return  # nothing minted, or GitHub no longer accepts it
        from orchestrator.services.connector_drivers.github_app import (
            revoke_installation_token,
        )

        await revoke_installation_token(str(material.get("api_base") or ""), token)
        return
    from orchestrator.services.connector_drivers.token_request import (
        delete_bound_secret,
    )

    secret = material.get("secret") if isinstance(material.get("secret"), dict) else {}
    if not secret.get("name") or not material.get("server"):
        raise ProviderError(
            "the record names no bound Secret to delete", transient=False
        )
    minting = MintingKubeconfig(
        server=str(material["server"]),
        ca_pem=material.get("ca"),
        tls_server_name=material.get("tls_server_name"),
        token=str(material.get("token") or ""),
        context_namespace=None,
        cluster_name="",
    )
    await delete_bound_secret(
        minting,
        namespace=str(material.get("namespace") or ""),
        name=str(secret["name"]),
        uid=secret.get("uid"),
    )


async def _revoke_one(store: Any, row: Mapping[str, Any], report: SweepReport) -> None:
    try:
        await _provider_revoke(row)
    except ProviderError as exc:
        attempts = int(row["revoke_attempts"]) + 1
        async with store.acquire() as conn:
            if attempts >= MAX_REVOKE_ATTEMPTS:
                await conn.execute(
                    """
                    UPDATE connector_minted_credentials
                       SET status = 'revoked', revoked_at = now(),
                           revoke_attempts = $2, revoke_error = $3
                     WHERE id = $1 AND status = 'revoking'
                    """,
                    row["id"],
                    attempts,
                    str(exc)[:500],
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
                        error=str(exc)[:200],
                    ),
                )
                report.given_up += 1
                return
            await conn.execute(
                """
                UPDATE connector_minted_credentials
                   SET revoke_attempts = $2, revoke_error = $3,
                       revoke_next_at = now() + make_interval(secs => $4::float8)
                 WHERE id = $1 AND status = 'revoking'
                """,
                row["id"],
                attempts,
                str(exc)[:500],
                _backoff(attempts),
            )
        report.retried += 1
        logger.warning(
            "Revoking minted credential %s (%s) failed, attempt %d: %s",
            row["id"],
            row["provider"],
            attempts,
            exc,
        )
        return
    async with store.acquire() as conn:
        done = await conn.fetchval(
            """
            UPDATE connector_minted_credentials
               SET status = 'revoked', revoked_at = now(), revoke_error = NULL
             WHERE id = $1 AND status = 'revoking'
            RETURNING id
            """,
            row["id"],
        )
        if done is not None and row["revoke_reason"] != "expired":
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
    if done is not None:
        report.revoked += 1


async def sweep_minted_once(store: Any) -> SweepReport:
    """One pass: request the revoke of credentials whose execution ended or
    is gone, of expired ones and of abandoned mints; then revoke what is due
    at the provider (at most :data:`REVOKES_PER_PASS`), and prune revoked
    rows past :data:`RETENTION_DAYS`."""
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
        due = await conn.fetch(
            """
            SELECT id, owner_kind, owner_id, connector_id, provider,
                   material_ciphertext, token_ciphertext, token_last_four,
                   expires_at, revoke_reason, revoke_attempts
              FROM connector_minted_credentials
             WHERE status = 'revoking'
               AND COALESCE(revoke_next_at, now()) <= now()
               AND (minted_at IS NOT NULL
                    OR created_at < now() - make_interval(secs => $1::int))
             ORDER BY revoke_next_at NULLS FIRST, id
             LIMIT $2
            """,
            MINT_ABANDON_SECONDS,
            REVOKES_PER_PASS,
        )
    for row in due:
        try:
            await _revoke_one(store, row, report)
        except Exception:
            logger.warning(
                "Revoking minted credential %s failed", row["id"], exc_info=True
            )
    async with store.acquire() as conn:
        pruned = await conn.fetch(
            """
            DELETE FROM connector_minted_credentials
             WHERE status = 'revoked'
               AND revoked_at < now() - make_interval(days => $1::int)
            RETURNING id
            """,
            RETENTION_DAYS,
        )
    report.pruned = len(pruned)
    return report


async def connector_minted_credential_sweeper(
    shutdown_event: asyncio.Event,
    *,
    store: Any,
    interval_seconds: float | None = None,
) -> None:
    """Leader-gated loop: :func:`sweep_minted_once` every interval (the
    lease sweep's, by default) and soon after a revoke request commits (it
    NOTIFYs :data:`REVOKE_CHANNEL`, which this loop LISTENs on).

    Best effort: a failed pass is logged and the next one runs on time;
    every step is re-derived from durable state, so a pass a leadership
    change cancels is run again by the next leader.
    """
    from orchestrator.services.connector_service_hosting import _listen, _until

    interval = float(interval_seconds or lease_sweep_seconds())
    logger.info("Connector minted-credential sweeper started (every %.0fs)", interval)
    wake = asyncio.Event()
    listener = asyncio.create_task(
        _listen(store, wake, shutdown_event, channel=REVOKE_CHANNEL)
    )
    try:
        while not shutdown_event.is_set():
            wake.clear()
            try:
                report = await sweep_minted_once(store)
                if report.any():
                    logger.info("connector minted credentials: %s", vars(report))
            except Exception as exc:
                logger.warning("connector minted-credential sweep error: %s", exc)
            await _until((shutdown_event, wake), interval)
    finally:
        listener.cancel()
        try:
            await listener
        except (asyncio.CancelledError, Exception):
            pass
    logger.info("Connector minted-credential sweeper stopped")


__all__ = [
    "INLINE_MINT_SECONDS",
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
    "connector_minted_credential_sweeper",
    "deliver_minted_entries",
    "ensure_minted",
    "fresh",
    "github_app_marker",
    "kubeconfig_marker",
    "minted_lease_upstream",
    "minted_marker",
    "minted_runtime",
    "plan_for",
    "prepare_minted_entries",
    "prepare_thread_minted",
    "revoke_connector_credentials",
    "revoke_owner_credentials",
    "revoke_terminal_owner_credentials",
    "row_provider",
    "sweep_minted_once",
]
