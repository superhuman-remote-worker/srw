"""Specs of the drivers SRW ships: the 13 datasource types and the two
generic-hosting delivery drivers.

The datasource drivers are named ``srw.<type>/v1`` and keep the stored
``datasources.type`` as ``legacy_type``.  Their order here is the connector
catalogue's order.  Everything that used to hardcode a list of types derives
from these specs: the catalogue (``shared.runtime.core.datasource_catalog``),
the tool map (``shared.datasource_policy``), the MCP server's type list and
the delivery-form type sets.

The access levels describe today's behaviour.  ``enforced_by`` names what
really holds a level; ``advisory`` marks a level only told to the agent.
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from .contract import AccessLevel, CredentialKind, CredentialSlot, DriverSpec

ALL_BACKENDS = frozenset({"sandbox", "vm", "virtual", "none"})
SHELL_BACKENDS = frozenset({"sandbox", "vm"})
#: Forges the repository and KB drivers accept (``shared.runtime.services.forge``).
FORGES: tuple[str, ...] = ("gitea", "github", "gitlab")

_NO_CONFIG: Mapping[str, Any] = {"type": "object", "maxProperties": 0}
_SECRET: Mapping[str, Any] = {"type": "string", "writeOnly": True}
#: A secret the user pastes or uploads from a file (a key, a kubeconfig).
_SECRET_FILE: Mapping[str, Any] = {
    **_SECRET,
    "x-srw-multiline": True,
    "x-srw-widget": "file",
}
_LOGIN: Mapping[str, Any] = {
    "type": "object",
    "properties": {"username": {"type": "string"}, "password": _SECRET},
}
_ENV_VARS: Mapping[str, Any] = {
    "type": "object",
    "properties": {
        "env_vars": {
            "type": "object",
            "propertyNames": {"pattern": "^[A-Za-z_][A-Za-z0-9_]*$"},
            "additionalProperties": _SECRET,
            "maxProperties": 100,
        }
    },
}
_FILES: Mapping[str, Any] = {
    "type": "object",
    "required": ["files"],
    "properties": {
        "files": {
            "type": "array",
            "minItems": 1,
            "maxItems": 5,
            "items": {
                "type": "object",
                "required": ["contents"],
                "properties": {
                    "name": {"type": "string"},
                    "contents": _SECRET_FILE,
                    "target_path": {"type": "string"},
                    "mode": {"type": "string", "pattern": "^0[0-7]{3}$"},
                    "env_var": {"type": "string"},
                },
            },
        }
    },
}


def _read_write(
    read: tuple[str, ...],
    write: tuple[str, ...],
    *,
    read_only_enforced_by: str,
    read_write_enforced_by: str,
    read_only_advisory: bool = False,
) -> tuple[AccessLevel, ...]:
    return (
        AccessLevel(
            "ReadOnly",
            0,
            read_only_enforced_by,
            tools=read,
            advisory=read_only_advisory,
        ),
        AccessLevel("ReadWrite", 1, read_write_enforced_by, tools=write),
    )


def _declared_only(delivered: str) -> tuple[AccessLevel, ...]:
    """Levels of a driver whose read-only is a note to the agent."""
    return (
        AccessLevel(
            "ReadOnly",
            0,
            f"Told to the agent only; the same {delivered} is delivered either way.",
            advisory=True,
        ),
        AccessLevel("ReadWrite", 1, "The upstream credential decides what is allowed."),
    )


_EMAIL_READ = ("email_list_folders", "email_list", "email_search", "email_read")
_EMAIL_READ_WRITE = _EMAIL_READ + ("email_move", "email_flag")
_EMAIL_DRAFT = _EMAIL_READ_WRITE + ("email_draft",)
_EMAIL_SEND = _EMAIL_DRAFT + ("email_send",)
_EMAIL_TIER_CHECK = "every email tool re-checks the connection's tier before it acts"

# ---------------------------------------------------------------------------
# Generic hosting (manifest connectors, no datasource row)
# ---------------------------------------------------------------------------

_MANIFEST_VALUE: Mapping[str, Any] = {
    "oneOf": [
        {"type": "string"},
        {
            "type": "object",
            "required": ["credential"],
            "additionalProperties": False,
            "properties": {"credential": {"type": "string"}},
        },
    ]
}

ENV_SPEC = DriverSpec(
    name="srw.env/v1",
    title="Environment variables",
    plane="bind_time",
    delivery_forms=("pod_env",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "env": {"type": "object", "additionalProperties": _MANIFEST_VALUE}
        },
    },
    access_levels=(),
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: delivered into the generic-hosting pod.",
    holds_upstream_credentials=True,
)

FILES_SPEC = DriverSpec(
    name="srw.files/v1",
    title="Files",
    plane="bind_time",
    delivery_forms=("pod_file",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "files": {
                "type": "object",
                "propertyNames": {"pattern": "^/run/srw/bindings/"},
                "additionalProperties": _MANIFEST_VALUE,
            }
        },
    },
    access_levels=(),
    supported_backends=ALL_BACKENDS,
    workspace_requirements=(
        "None: delivered into the generic-hosting pod under /run/srw/bindings/."
    ),
    holds_upstream_credentials=True,
)

# ---------------------------------------------------------------------------
# Datasource types, in catalogue order
# ---------------------------------------------------------------------------

GENERIC_SPEC = DriverSpec(
    name="srw.generic/v1",
    legacy_type="generic",
    title="Generic",
    guide_topic="datasources",
    plane="bind_time",
    delivery_forms=("env_file",),
    config_schema=_NO_CONFIG,
    legacy_connection_url="optional",
    credential_slots=(
        CredentialSlot(
            "env_vars", "secret_string", _ENV_VARS, delivery="env", update="replace"
        ),
    ),
    access_levels=_declared_only("environment"),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A POSIX shell: the variables are written under ~/.srw-credentials/ "
        "and sourced for every command."
    ),
    holds_upstream_credentials=True,
)

CREDENTIALS_SPEC = DriverSpec(
    name="srw.credentials/v1",
    legacy_type="credentials",
    title="Credentials",
    guide_topic="datasources",
    plane="bind_time",
    delivery_forms=("env_file",),
    config_schema=_NO_CONFIG,
    legacy_connection_url="optional",
    credential_slots=(
        CredentialSlot(
            "env_vars",
            "secret_string",
            _ENV_VARS,
            required=True,
            delivery="env",
            update="merge",
            names_field="env_var_names",
        ),
    ),
    access_levels=_declared_only("environment"),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A POSIX shell: the variables are written under ~/.srw-credentials/ "
        "and sourced for every command."
    ),
    publishable=False,
    live_detach="refused",
    delete_while_attached=False,
    holds_upstream_credentials=True,
)

REPOSITORY_SPEC = DriverSpec(
    name="srw.repository/v1",
    legacy_type="repository",
    title="Repository",
    guide_topic="datasources",
    plane="bind_time",
    # A token repository is a checkout only; an SSH-key repository's key
    # also goes into the workspace's ssh-agent.
    delivery_forms=("checkout", "ssh_identity"),
    config_schema={
        "type": "object",
        "properties": {
            "forge": {"enum": list(FORGES)},
            "known_hosts": {"type": "string", "x-srw-multiline": True},
        },
    },
    legacy_connection_url="required",
    credential_slots=(
        # The token lands in the clone URL, and so in .git/config, until the
        # git swap driver (C3); the key is loaded into an ssh-agent (C1).
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            delivery="file",
            update="replace",
        ),
        CredentialSlot(
            "ssh_key",
            "ssh_private_key",
            {"type": "object", "properties": {"ssh_key": _SECRET_FILE}},
            delivery="ssh_agent",
            update="replace",
        ),
    ),
    tool_category="repo",
    access_levels=_read_write(
        ("repo_pull", "repo_pr_status"),
        (
            "repo_checkout",
            "repo_commit",
            "repo_push",
            "repo_pull",
            "repo_open_pr",
            "repo_pr_status",
        ),
        read_only_enforced_by=(
            "Only the repo tools are read-only; the workspace shell can still "
            "push with the checkout's credentials."
        ),
        read_only_advisory=True,
        read_write_enforced_by=(
            "The forge token or deploy key decides which pushes succeed."
        ),
    ),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements="git and a POSIX shell: the repository is cloned there.",
    holds_upstream_credentials=True,
)

KB_SPEC = DriverSpec(
    name="srw.kb/v1",
    legacy_type="kb",
    title="OKF Knowledge Base",
    guide_topic="datasources-okf",
    plane="harness",
    delivery_forms=("knowledge_index",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "root_path": {"type": ["string", "null"]},
            "forge": {"enum": list(FORGES)},
        },
    },
    legacy_connection_url="required",
    credential_slots=(
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            update="replace",
        ),
        CredentialSlot(
            "ssh_key",
            "ssh_private_key",
            {"type": "object", "properties": {"ssh_key": _SECRET_FILE}},
            update="replace",
        ),
    ),
    access_levels=(
        AccessLevel(
            "ReadOnly",
            0,
            "SRW indexes the repository itself; the agent receives an index id, "
            "never the URL or the repository credentials.",
        ),
    ),
    default_access="ReadOnly",
    operations=("status", "reindex"),
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: SRW indexes the repository centrally.",
    forced_read_only=True,
    live_attach=False,
    live_detach="next_attach",
    needs_knowledge_profile=True,
    holds_upstream_credentials=True,
)


def _managed(
    type_id: str,
    title: str,
    category: str,
    read: tuple[str, ...],
    write: tuple[str, ...],
    *,
    read_only_enforced_by: str,
    credential_slots: tuple[CredentialSlot, ...] = (),
) -> DriverSpec:
    return DriverSpec(
        name=f"srw.{type_id}/v1",
        legacy_type=type_id,
        title=title,
        guide_topic="datasources",
        plane="harness",
        delivery_forms=("managed_connection",),
        config_schema=_NO_CONFIG,
        legacy_connection_url="optional",
        credential_slots=credential_slots,
        tool_category=category,
        access_levels=_read_write(
            read,
            write,
            read_only_enforced_by=read_only_enforced_by,
            read_write_enforced_by=(f"The {title} login decides which writes succeed."),
        ),
        supported_backends=ALL_BACKENDS,
        workspace_requirements="None: the agent process holds the connection.",
        holds_upstream_credentials=True,
    )


POSTGRESQL_SPEC = _managed(
    "postgresql",
    "PostgreSQL",
    "sql",
    ("sql_query", "sql_schema"),
    ("sql_query", "sql_schema", "sql_execute"),
    read_only_enforced_by=(
        "Only the read tools are bound, and sql_query runs in a READ ONLY "
        "transaction; the login in the URL is the connector's own."
    ),
)
NEO4J_SPEC = _managed(
    "neo4j",
    "Neo4j",
    "graph",
    ("cypher_query", "get_database_schema"),
    ("cypher_query", "cypher_execute", "get_database_schema"),
    read_only_enforced_by=(
        "Only the read tools are bound, and the connection opens only "
        "read-access sessions, in which the Neo4j server refuses every write."
    ),
    credential_slots=(
        CredentialSlot("login", "secret_string", _LOGIN, update="replace"),
    ),
)
MONGODB_SPEC = _managed(
    "mongodb",
    "MongoDB",
    "mongodb",
    ("mongo_query", "mongo_aggregate", "mongo_schema"),
    ("mongo_query", "mongo_aggregate", "mongo_schema", "mongo_insert", "mongo_update"),
    read_only_enforced_by=(
        "Only the find, aggregate and schema tools are bound; an aggregate "
        "pipeline is not checked for $out or $merge."
    ),
)
WEBDAV_SPEC = _managed(
    "webdav",
    "WebDAV",
    "webdav",
    ("webdav_list", "webdav_read", "webdav_info"),
    ("webdav_list", "webdav_read", "webdav_info", "webdav_write", "webdav_delete"),
    read_only_enforced_by=(
        "Only the list, read and info tools are bound; the login is the "
        "connector's own."
    ),
    credential_slots=(
        CredentialSlot("login", "secret_string", _LOGIN, update="replace"),
    ),
)

_SERVER_BLOCK: Mapping[str, Any] = {
    "type": "object",
    "required": ["host"],
    "properties": {
        "host": {"type": "string"},
        "port": {"type": "integer", "minimum": 1, "maximum": 65535},
        "security": {"enum": ["ssl", "starttls"]},
    },
}

EMAIL_SPEC = DriverSpec(
    name="srw.email/v1",
    legacy_type="email",
    title="Email",
    guide_topic="datasources-email",
    plane="harness",
    delivery_forms=("managed_connection",),
    # Stored if given; the mailbox's servers are in its credentials.
    legacy_connection_url="optional",
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "access": {"enum": ["read", "read_write", "draft", "send"]},
            "folders": {"type": "array", "items": {"type": "string", "minLength": 1}},
            "drafts_folder": {"type": "string", "minLength": 1},
            "from_address": {"type": ["string", "null"]},
            "recipient_allowlist": {
                "type": "array",
                "items": {"type": "string", "minLength": 1},
            },
            "unattended_send": {"type": "boolean"},
        },
    },
    credential_slots=(
        CredentialSlot(
            "mailbox",
            "secret_string",
            {
                "type": "object",
                "required": ["username", "password", "imap"],
                "properties": {
                    "backend": {"const": "imap_smtp"},
                    "username": {"type": "string"},
                    "password": _SECRET,
                    "imap": _SERVER_BLOCK,
                },
            },
            required=True,
            update="replace",
        ),
        CredentialSlot(
            "smtp",
            "secret_string",
            {"type": "object", "properties": {"smtp": _SERVER_BLOCK}},
            access_levels=("send",),
            update="replace",
        ),
    ),
    tool_category="email",
    access_levels=(
        AccessLevel(
            "read",
            0,
            f"Only the reading tools are bound, and {_EMAIL_TIER_CHECK}.",
            tools=_EMAIL_READ,
        ),
        AccessLevel(
            "read_write",
            1,
            f"Moving and flagging tools are added; {_EMAIL_TIER_CHECK}.",
            tools=_EMAIL_READ_WRITE,
        ),
        AccessLevel(
            "draft",
            2,
            f"Drafts are filed over IMAP, nothing is sent; {_EMAIL_TIER_CHECK}.",
            tools=_EMAIL_DRAFT,
        ),
        AccessLevel(
            "send",
            3,
            "email_send needs a folder allowlist and pauses for a person's "
            f"approval unless unattended sending is granted; {_EMAIL_TIER_CHECK}.",
            tools=_EMAIL_SEND,
        ),
    ),
    default_access="draft",
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: the agent process holds the mailbox connection.",
    publishable=False,
    max_per_execution=1,
    holds_upstream_credentials=True,
)

MCP_SPEC = DriverSpec(
    name="srw.mcp/v1",
    legacy_type="mcp",
    title="MCP Server",
    guide_topic="datasources",
    plane="harness",
    delivery_forms=("mcp_client",),
    config_schema=_NO_CONFIG,
    legacy_connection_url="optional",
    credential_slots=(
        CredentialSlot(
            "server",
            "secret_string",
            {
                "type": "object",
                "properties": {
                    "transport": {"enum": ["http", "sse", "stdio"]},
                    "auth": {"type": "object", "writeOnly": True},
                    "command": {"type": "string"},
                    "args": {"type": "array", "items": {"type": "string"}},
                    "env": {"type": "object", "writeOnly": True},
                },
            },
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=(
        AccessLevel(
            "ReadWrite",
            1,
            "The MCP server and its credentials are the access boundary; SRW "
            "binds every tool the server lists.",
            tools="*",
        ),
    ),
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements=(
        "None: the agent process is the MCP client; a stdio server runs as "
        "its subprocess."
    ),
    deployment_gate="mcp_datasources",
    holds_upstream_credentials=True,
)


def _credential_file(
    type_id: str, name: str, title: str, kind: CredentialKind, file_word: str
) -> DriverSpec:
    return DriverSpec(
        name=f"srw.{name}/v1",
        legacy_type=type_id,
        title=title,
        guide_topic="datasources",
        plane="bind_time",
        delivery_forms=("credential_file",),
        config_schema=_NO_CONFIG,
        legacy_connection_url="optional",
        credential_slots=(
            CredentialSlot(
                "files",
                kind,
                _FILES,
                required=True,
                delivery="file",
                update="replace",
            ),
        ),
        access_levels=_declared_only(file_word),
        supported_backends=SHELL_BACKENDS,
        workspace_requirements=(
            "A shell workspace: each file is written under the home's private "
            "credential store over a secret channel, with its mode, and linked "
            "at its target path; kubeconfigs are merged for kubectl."
        ),
        holds_upstream_credentials=True,
    )


KUBECONFIG_SPEC = _credential_file(
    "kubeconfig", "kubeconfig", "Kubeconfig", "kubeconfig", "kubeconfig"
)
SSH_KEY_SPEC = DriverSpec(
    name="srw.ssh-key/v1",
    legacy_type="ssh_key",
    title="SSH Key",
    guide_topic="datasources",
    plane="bind_time",
    delivery_forms=("ssh_identity",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "host": {"type": "string"},
            "user": {"type": "string"},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "known_hosts": {"type": "string", "x-srw-multiline": True},
        },
    },
    legacy_connection_url="optional",
    credential_slots=(
        # files[0] is the private key, an optional files[1] its public key.
        CredentialSlot(
            "files",
            "ssh_private_key",
            _FILES,
            required=True,
            delivery="ssh_agent",
            update="replace",
        ),
    ),
    access_levels=_declared_only("key"),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A shell workspace: the key is loaded into a dedicated ssh-agent there "
        "and reached through an opaque alias; it never lands on disk."
    ),
    holds_upstream_credentials=True,
)
GENERIC_FILE_SPEC = _credential_file(
    "generic_file", "generic-file", "Generic file", "file", "file"
)

#: Datasource drivers in catalogue order.
DATASOURCE_SPECS: tuple[DriverSpec, ...] = (
    GENERIC_SPEC,
    CREDENTIALS_SPEC,
    REPOSITORY_SPEC,
    KB_SPEC,
    POSTGRESQL_SPEC,
    NEO4J_SPEC,
    MONGODB_SPEC,
    WEBDAV_SPEC,
    EMAIL_SPEC,
    MCP_SPEC,
    KUBECONFIG_SPEC,
    SSH_KEY_SPEC,
    GENERIC_FILE_SPEC,
)
MANIFEST_SPECS: tuple[DriverSpec, ...] = (ENV_SPEC, FILES_SPEC)
BUILTIN_SPECS: tuple[DriverSpec, ...] = MANIFEST_SPECS + DATASOURCE_SPECS

_BY_TYPE: dict[str, DriverSpec] = {
    spec.legacy_type: spec for spec in DATASOURCE_SPECS if spec.legacy_type
}
LEGACY_TYPE_IDS: tuple[str, ...] = tuple(_BY_TYPE)

#: The datasource tool map's key order, kept from before the specs existed so
#: a job's tool override keeps its key order across the move.
_TOOL_MAP_ORDER: tuple[DriverSpec, ...] = (
    NEO4J_SPEC,
    POSTGRESQL_SPEC,
    MONGODB_SPEC,
    WEBDAV_SPEC,
    REPOSITORY_SPEC,
    EMAIL_SPEC,
    MCP_SPEC,
)


def spec_for_type(legacy_type: str | None) -> DriverSpec | None:
    """The built-in spec serving a stored ``datasources.type``."""
    return _BY_TYPE.get(legacy_type or "")


def spec_for_row(row: Any) -> DriverSpec | None:
    """The built-in spec serving a stored connector row or payload entry.

    A row is anything with ``get`` (a dict, a database record).  Its
    ``type`` is matched exactly, as ``ConnectorDriverRegistry.for_type``
    matches it: every write path stores a driver's own lowercase type.
    ``None`` for an unknown type or a value that is no row.
    """
    get = getattr(row, "get", None)
    if not callable(get):
        return None
    ds_type = get("type")
    return spec_for_type(ds_type) if isinstance(ds_type, str) else None


def delivers_in(row: Any, form: str) -> bool:
    """Whether a connector row's driver delivers in ``form`` (a checkout is
    what a pull request is opened from)."""
    spec = spec_for_row(row)
    return spec is not None and form in spec.delivery_forms


def needs_knowledge_profile(row: Any) -> bool:
    """Whether a connector row's driver needs the system KB embedding profile
    delivered with the work it is attached to."""
    spec = spec_for_row(row)
    return spec is not None and spec.needs_knowledge_profile


def legacy_types_where(predicate: Callable[[DriverSpec], bool]) -> frozenset[str]:
    """Stored types whose spec satisfies ``predicate``, for SQL that filters
    rows by type."""
    return frozenset(
        spec.legacy_type
        for spec in DATASOURCE_SPECS
        if spec.legacy_type and predicate(spec)
    )


def legacy_types_with_config_key(key: str) -> frozenset[str]:
    """Stored types whose config schema declares ``key``."""
    return legacy_types_where(
        lambda spec: key in (spec.config_schema.get("properties") or {})
    )


def legacy_types_with_slot(slot: str) -> frozenset[str]:
    """Stored types whose credentials have the named slot."""
    return frozenset(
        spec.legacy_type
        for spec in DATASOURCE_SPECS
        if spec.legacy_type and any(item.name == slot for item in spec.credential_slots)
    )


def legacy_types_with_form(form: str) -> frozenset[str]:
    """Stored types whose driver delivers in ``form``."""
    return frozenset(
        spec.legacy_type
        for spec in DATASOURCE_SPECS
        if spec.legacy_type and form in spec.delivery_forms
    )


def tool_map_entry(spec: DriverSpec) -> dict[str, Any]:
    """One driver's entry in the datasource tool map.

    Three shapes, from the access levels: ``dynamic`` when a level binds
    runtime-discovered tools, ``read``/``write`` for the ReadOnly/ReadWrite
    pair, and ``tiers`` keyed by level id otherwise.
    """
    levels = sorted(spec.access_levels, key=lambda level: level.rank)
    entry: dict[str, Any] = {"category": spec.tool_category}
    if any(level.tools == "*" for level in levels):
        entry["dynamic"] = True
    elif [level.id for level in levels] == ["ReadOnly", "ReadWrite"]:
        entry["read"] = list(levels[0].tools)
        entry["write"] = list(levels[1].tools)
    else:
        entry["tiers"] = {level.id: list(level.tools) for level in levels}
    return entry


def tool_map() -> dict[str, dict[str, Any]]:
    """``DATASOURCE_TOOL_MAP``: stored type to tool category and tool sets."""
    missing = {spec.name for spec in DATASOURCE_SPECS if spec.tool_category} - {
        spec.name for spec in _TOOL_MAP_ORDER
    }
    if missing:
        raise RuntimeError(f"tool map order lacks {sorted(missing)}")
    return {
        spec.legacy_type: tool_map_entry(spec)
        for spec in _TOOL_MAP_ORDER
        if spec.legacy_type
    }


def tool_categories() -> tuple[str, ...]:
    """The tool categories datasource drivers bind, in tool-map order."""
    return tuple(entry["category"] for entry in tool_map().values())
