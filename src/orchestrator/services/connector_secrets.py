"""A Connector's credentials in its resource secret (slice D3b).

D3a wrote every datasource row through to a manifest Connector resource and
left the secrets on the row.  Now each Connector has one
``connector-<32 hex of its uid>`` row in ``srw_resource_secrets``, in the
Connector's own scope (its creator's Account, or the project of a project's
knowledge base or of an ownerless row), and the resource names its keys in
``spec.credentials`` the way any manifest Connector references a secret.

The keys, flattened per driver credential slot (each driver's
``secret_leaves``):

* ``token``, ``ssh_key``, ``password``, ``username`` (a Neo4j or WebDAV
  login), ``secret``;
* ``env.<NAME>`` for an environment variable (and a stdio MCP server's
  environment), ``header.<Name>`` for a remote MCP server's header,
  ``arg.<n>`` and ``command`` for a stdio MCP server, ``file.<n>`` for a
  credential file's contents;
* ``url``: the row's full connection URL.  The resource keeps only
  ``scheme://host[:port]`` of it; the rest (a Postgres or MongoDB password in
  the userinfo, a token in the path or the query) is only here;
* ``shape``: the rest of the stored credentials object, with each value
  above taken out, and where each one goes back.  It keeps the non-secret
  structure (an auth method, a transport, file targets, mailbox servers) and
  any value no driver names a key for, so the stored object is rebuilt
  exactly, key order included, whatever its shape (``stored_credentials``).

**``shape`` is secret material.**  It holds every value no driver names a
key for: an extra field a repository row stored (a password, a passphrase),
a nested object (``{"tls": {"client_key": ...}}``), a value that is not a
string (a numeric Neo4j password, a non-string MCP token), a file entry's
``content`` where the driver reads ``contents``, an environment variable
whose name cannot be a key name, a legacy row's top-level ``url`` or
``shape`` field.  It is encrypted with the rest and is read by
:func:`stored_credentials` only, to rebuild the row's object for the row's
own driver.  **Nothing may forward ``shape``**: no per-key consumer (a lease
driver, a materializer, a manifest binding of the Connector's slots) may
deliver it, or any key it does not know, to a process; a consumer takes the
slot keys it names and leaves the rest.

A connector with nothing secret (no credentials, no URL) has no secret row
and an empty ``spec.credentials``.  Only string values move into keys; a
value a driver names but that is not a string stays in ``shape``.  Key names
are plaintext in the Connector's revisions, so a stored name becomes part of
a key only when it is an environment name (``connector_drivers.base.
key_name``), and new credentials may not use ``url`` or ``shape`` as
top-level fields.

The secret is written by the datasource write-through, in the transaction
that writes the row and the resource (``manifest_connectors``), from the row
as stored.  The row's update rules therefore decide what it holds: an edit
that sends no credentials keeps them, one that sends some replaces them all,
and a ``credentials`` connector merges its variables (each slot's ``update``
in ``shared.connectors.builtin``).  The row keeps its encrypted copy for
rollback (Release N).  The resource API refuses to write a ``connector-``
secret: only the connector's own write may.  And no other resource may
reference one: only its own Connector uses it, so a reference elsewhere (an
Expert's environment, a generic-hosting file) can neither bypass the
connector policy nor carry a credential the driver never forwards (a lease
driver's upstream secret).

**Who may use it** (decision 11).  A Connector shared with other users
(public, or linked to their project) lends its creator's credentials to
their executions, as a datasource always did.  So the authority for a
Connector's own secret is the connector policy
(``datasource_policy.classify_datasource_selection``), not write access to
the secret's scope: an execution the policy authorized for the connector may
use it.  Delivery uses the ids the policy has just authorized
(:func:`read_connector_credentials`); a manifest resolution asks the policy
for its caller (``ManifestAuthority.connector_secret``).  A Catalog secret is
refused either way, and the scope CHECK of ``srw_resource_secrets`` still
admits no Catalog row.

Design: knowledge-base/knowledge/features/connector_drivers.md, "Datasources
become Connectors" (the mapping's secret rows, Release N, "Carried
questions" -> Credentials) and decision 11.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Collection, Mapping
from copy import deepcopy
from typing import Any
from uuid import UUID

from orchestrator.security.crypto import DecryptionError, decrypt, encrypt

logger = logging.getLogger(__name__)

#: A Connector's resource secret is named after its uid.
SECRET_PREFIX = "connector-"
URL_KEY = "url"
#: Secret material (see the module docstring): never forwarded to a process.
SHAPE_KEY = "shape"
#: Keys no driver slot may use, and no stored credentials object as a
#: top-level field.
RESERVED_KEYS = frozenset({URL_KEY, SHAPE_KEY})
CATALOG_SECRET_DETAIL = (
    "Shared catalog definitions cannot distribute catalog credentials."
)
#: The resource API's refusal to write a Connector's secret.
CONNECTOR_SECRET_DETAIL = (
    "This name belongs to a connector's credentials; change them on the "
    "Connectors page (/api/datasources), which writes the connector and its "
    "secret together."
)
#: A reference to a Connector's secret from anything but that Connector.
FOREIGN_CONNECTOR_SECRET_DETAIL = (
    "A connector's credentials are used only by that connector; select the "
    "connector instead of referencing its secret."
)
_SECRET_NAME = re.compile(r"connector-[0-9a-f]{32}\Z")
_SECRET_SQL_NAME = "'connector-' || replace(r.id::text, '-', '')"

#: A linked Connector's resource and its secret, for delivery.
_READ = f"""
SELECT r.id, r.scope_kind, r.scope_name, r.linked_updated_at,
       r.document->'spec'->'credentials' AS refs,
       s.ciphertext, s.keys
FROM srw_resources r
LEFT JOIN srw_resource_secrets s
  ON s.scope_kind = r.scope_kind AND s.scope_name = r.scope_name
 AND s.name = {_SECRET_SQL_NAME}
WHERE r.id = ANY($1::uuid[]) AND r.kind = 'Connector'
  AND r.linked_id = r.id AND r.deleted_at IS NULL
"""
#: Secrets of Connectors that were retired or moved to another scope by a
#: write that did not drop them (an older orchestrator during a rollout).
#: Only a name that is a linked Connector's uid is looked at.
STALE_SECRETS = """
DELETE FROM srw_resource_secrets s
USING srw_resources r
WHERE s.name ~ '^connector-[0-9a-f]{32}$'
  AND r.id = substr(s.name, 11)::uuid
  AND r.kind = 'Connector' AND r.linked_id IS NOT NULL
  AND (r.deleted_at IS NOT NULL
       OR s.scope_kind <> r.scope_kind OR s.scope_name <> r.scope_name)
"""


# =============================================================================
# The mapping (pure)
# =============================================================================


def connector_secret_name(uid: Any) -> str:
    """``connector-<32 hex>`` of a Connector uid."""
    return SECRET_PREFIX + UUID(str(uid)).hex


def is_connector_secret_name(name: Any) -> bool:
    return isinstance(name, str) and bool(_SECRET_NAME.fullmatch(name))


def _parent(value: Any, path: list[Any] | tuple[Any, ...]) -> Any:
    for step in path:
        value = value[step]
    return value


def secret_values(driver: Any, row: Mapping[str, Any]) -> dict[str, str]:
    """The secret of a row's Connector: its keys and their string values.

    Empty when the row holds no credentials and no URL.
    """
    credentials = row.get("credentials")
    values: dict[str, str] = {}
    if credentials is not None and credentials != {}:
        shape = deepcopy(credentials)
        placed: list[list[Any]] = []
        if isinstance(credentials, Mapping):
            for path, key in driver.secret_leaves(credentials):
                parent = _parent(shape, path[:-1])
                value = parent[path[-1]]
                if key in RESERVED_KEYS or key in values or not isinstance(value, str):
                    continue
                values[key] = value
                parent[path[-1]] = None
                placed.append([list(path), key])
        values[SHAPE_KEY] = json.dumps(
            {"credentials": shape, "secrets": placed}, separators=(",", ":")
        )
    url = row.get("connection_url")
    if isinstance(url, str):
        values[URL_KEY] = url
    return values


def stored_credentials(values: Mapping[str, str]) -> tuple[Any, str | None]:
    """The row's credentials object and connection URL a secret holds; the
    inverse of :func:`secret_values`."""
    credentials: Any = {}
    if SHAPE_KEY in values:
        shape = json.loads(values[SHAPE_KEY])
        credentials = shape["credentials"]
        for path, key in shape["secrets"]:
            _parent(credentials, path[:-1])[path[-1]] = values[key]
    return credentials, values.get(URL_KEY)


def credential_refs(name: str, values: Mapping[str, str]) -> dict[str, Any]:
    """``spec.credentials`` of a Connector: each key of its secret, by name.

    No scope: a reference without one resolves in the Connector's scope.
    """
    return {key: {"secretRef": {"name": name, "key": key}} for key in sorted(values)}


def is_own_connector_secret(ref: Mapping[str, Any], resource: Mapping[str, Any]):
    """Whether ``ref`` names the linked Connector ``resource``'s own secret,
    in the Connector's own scope."""
    if resource.get("kind") != "Connector" or not resource.get("linked_id"):
        return False
    scope = ref.get("scope") or {}
    own = resource["document"]["metadata"]["scope"]
    return (
        ref.get("name") == connector_secret_name(resource["id"])
        and scope.get("kind") == own.get("kind")
        and str(scope.get("name")) == str(own.get("name"))
    )


# =============================================================================
# Persistence (inside the datasource write's transaction)
# =============================================================================


def _decrypted(ciphertext: str) -> dict[str, Any] | None:
    try:
        value = json.loads(decrypt(ciphertext))
    except (DecryptionError, ValueError):
        return None
    return value if isinstance(value, dict) else None


async def write_connector_secret(
    db,
    uid: Any,
    scope: Mapping[str, str],
    *,
    owner_id: Any,
    values: Mapping[str, str],
) -> bool:
    """Make the Connector's secret in ``scope`` hold ``values``; ``True``
    when that changed it.  No values: no secret."""
    name = connector_secret_name(uid)
    current = await db.fetchrow(
        "SELECT ciphertext, owner_id FROM srw_resource_secrets "
        "WHERE scope_kind=$1 AND scope_name=$2 AND name=$3 FOR UPDATE",
        scope["kind"],
        scope["name"],
        name,
    )
    if not values:
        if current is None:
            return False
        return await drop_connector_secret(db, uid, scope)
    owner = UUID(str(owner_id)) if owner_id else None
    if (
        current is not None
        and current["owner_id"] == owner
        and _decrypted(current["ciphertext"]) == dict(values)
    ):
        return False
    await db.execute(
        """INSERT INTO srw_resource_secrets(scope_kind,scope_name,name,owner_id,
               ciphertext,keys)
           VALUES($1,$2,$3,$4,$5,$6)
           ON CONFLICT(scope_kind,scope_name,name) DO UPDATE
           SET ciphertext=EXCLUDED.ciphertext, keys=EXCLUDED.keys,
               owner_id=EXCLUDED.owner_id,
               version=srw_resource_secrets.version+1, updated_at=now()""",
        scope["kind"],
        scope["name"],
        name,
        owner,
        encrypt(json.dumps(dict(values))),
        sorted(values),
    )
    return True


async def drop_connector_secret(db, uid: Any, scope: Mapping[str, str]) -> bool:
    """Delete the Connector's secret in ``scope``; ``True`` if there was one."""
    result = await db.execute(
        "DELETE FROM srw_resource_secrets "
        "WHERE scope_kind=$1 AND scope_name=$2 AND name=$3",
        scope["kind"],
        scope["name"],
        connector_secret_name(uid),
    )
    return result != "DELETE 0"


# =============================================================================
# Readers (Release N: the resource first, the row as the fallback)
# =============================================================================


def _resource_values(record: Mapping[str, Any], row: Mapping[str, Any]):
    """The secret a delivery may read for ``row``, or ``None`` to use the
    row's own credentials."""
    if record["scope_kind"] not in ("Account", "Project"):
        return None
    if record["linked_updated_at"] != row.get("updated_at"):
        # Written from another version of the row (an older orchestrator
        # during a rollout, or a write that committed after this read): the
        # row is what the policy just authorized.
        return None
    refs = record["refs"]
    if isinstance(refs, str):
        refs = json.loads(refs)
    if not isinstance(refs, dict):
        # Written before D3b, or by an orchestrator without it.
        return None
    if not refs:
        return {}
    name = connector_secret_name(record["id"])
    if any(
        not isinstance(ref, dict)
        or (ref.get("secretRef") or {}).get("name") != name
        or (ref.get("secretRef") or {}).get("key") != key
        for key, ref in refs.items()
    ):
        return None
    values = (
        _decrypted(record["ciphertext"]) if record["ciphertext"] is not None else None
    )
    if values is None or not set(refs) <= set(values):
        logger.warning(
            "Connector %s: its resource secret is missing or unreadable; "
            "delivering the row's credentials",
            record["id"],
        )
        return None
    return {key: values[key] for key in refs}


async def read_connector_credentials(
    rows: list[dict[str, Any]], *, authorized: Collection[str], dependencies: Any
) -> None:
    """Replace each row's ``credentials`` and ``connection_url`` with what
    its Connector's secret holds, in place.

    ``authorized`` is the ids the connector policy has authorized for this
    use (an execution's selection, a Test by the connector's owner): the
    authority to read a Connector's secret whatever its scope (decision 11).
    A row outside it, or whose resource is missing, retired, written from
    another version of the row (or a row read without its ``updated_at``)
    or written before D3b, keeps the row's own credentials: the row keeps
    its encrypted copy in Release N.  ``dependencies.store`` is the
    database the rows were read from.
    """
    allowed = {str(value) for value in authorized}
    wanted: dict[UUID, dict[str, Any]] = {}
    for row in rows:
        if str(row.get("id")) not in allowed or row.get("updated_at") is None:
            continue
        try:
            wanted[UUID(str(row["id"]))] = row
        except ValueError:
            continue
    if not wanted:
        return
    for record in await dependencies.store.fetch(_READ, list(wanted)):
        row = wanted[record["id"]]
        values = _resource_values(record, row)
        if values is None:
            continue
        row["credentials"], row["connection_url"] = stored_credentials(values)


# =============================================================================
# The authority for manifest resolutions (decision 11)
# =============================================================================


async def connector_policy_authorizes(
    db, user: Mapping[str, Any], datasource_id: Any, project_ids=()
) -> bool:
    """Whether the connector policy lets ``user``'s work in ``project_ids``
    use the connector: its own, public, linked to every one of those
    projects, or a project's own knowledge base in that project.  An access
    check only, like a session's attach (no workspace tier), and with the
    same administrator override a session's or a job's own selection gets
    (``authorize_thread_datasource_selection``): an administrator's work
    may use any connector, as its delivery does."""
    from orchestrator.services.datasource_policy import (
        DatasourceUnavailableError,
        classify_datasource_selection,
    )

    try:
        verdicts, _revisions = await classify_datasource_selection(
            db,
            dict(user),
            str(user["id"]),
            [str(datasource_id)],
            [str(project) for project in project_ids],
            None,
            allow_admin_explicit_override=True,
        )
    except DatasourceUnavailableError:
        return False
    return [verdict.denied for verdict in verdicts] == [False]


__all__ = [
    "CATALOG_SECRET_DETAIL",
    "CONNECTOR_SECRET_DETAIL",
    "FOREIGN_CONNECTOR_SECRET_DETAIL",
    "SHAPE_KEY",
    "STALE_SECRETS",
    "URL_KEY",
    "connector_policy_authorizes",
    "connector_secret_name",
    "credential_refs",
    "drop_connector_secret",
    "is_connector_secret_name",
    "is_own_connector_secret",
    "read_connector_credentials",
    "secret_values",
    "stored_credentials",
    "write_connector_secret",
]
