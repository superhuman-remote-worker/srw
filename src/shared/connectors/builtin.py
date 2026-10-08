"""Specs of the drivers SRW ships: the 13 datasource types, the two
generic-hosting delivery drivers, the managed MCP servers of its catalogue
(off until an installation names their images) and the development drivers
(the lease probe, the echo service and the MCP test servers, off unless an
installation turns them on).

The datasource drivers are named ``srw.<type>/v1`` and keep the stored
``datasources.type`` as ``legacy_type``.  Their order here is the connector
catalogue's order.  Everything that used to hardcode a list of types derives
from these specs: the catalogue (``shared.runtime.core.datasource_catalog``),
the tool map (``shared.datasource_policy``), the MCP server's type list and
the delivery-form type sets.

The access levels describe today's behaviour.  ``enforced_by`` names what
really holds a level; ``advisory`` marks a level only told to the agent.

So do the credential slots' ``update`` rules, which are the datasource API's:
an edit that sends no credentials keeps every stored one, and an edit that
sends any replaces the whole stored object (``replace``), except that a
``credentials`` connector merges the variables it is sent into the stored
set (``merge``).  No built-in slot keeps a blank field of an edit that sends
others (``keep_if_blank``).  The Connector's resource secret is written from
the stored row, so it follows the same rules (slice D3b).
"""

from __future__ import annotations

from typing import Any, Callable, Mapping

from .contract import (
    AccessLevel,
    CredentialKind,
    CredentialSlot,
    DriverSpec,
    EgressRule,
    ServiceSpec,
)

ALL_BACKENDS = frozenset({"sandbox", "vm", "virtual", "none"})
SHELL_BACKENDS = frozenset({"sandbox", "vm"})
#: Forges the repository and KB drivers accept (``shared.runtime.services.forge``).
FORGES: tuple[str, ...] = ("gitea", "github", "gitlab")

_SECRET: Mapping[str, Any] = {"type": "string", "writeOnly": True}

# ---------------------------------------------------------------------------
# Connector config a datasource row mirrors (slice D3a)
#
# A datasource row's Connector resource carries, besides the driver's own
# config, what the row holds that is not secret: where the service is (the
# row's connection URL cut to ``scheme://host[:port]``; the full URL stays a
# secret), the branch, and the non-secret parts of the credentials. SRW
# derives them from the row, so they are ``readOnly``.
# ---------------------------------------------------------------------------

_MIRROR: Mapping[str, Any] = {"type": "string", "readOnly": True}
#: The service a connector reaches, ``scheme://host[:port]``.
_ENDPOINT = _MIRROR
#: Where each credential file lands; its contents are the secret.
_FILE_TARGETS: Mapping[str, Any] = {
    "type": "array",
    "readOnly": True,
    "items": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "name": {"type": "string"},
            "target_path": {"type": "string"},
            "mode": {"type": "string", "pattern": "^0[0-7]{3}$"},
            "env_var": {"type": "string"},
        },
    },
}
#: A repository's stored auth method (token, ssh, none).
_AUTH_METHOD = _MIRROR


def _config(**properties: Mapping[str, Any]) -> Mapping[str, Any]:
    """A closed config schema: these keys and no others."""
    return {"type": "object", "additionalProperties": False, "properties": properties}


#: A driver with no config of its own; its connector mirrors the endpoint.
_ENDPOINT_CONFIG = _config(endpoint=_ENDPOINT)
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
    config_schema=_ENDPOINT_CONFIG,
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
    config_schema=_ENDPOINT_CONFIG,
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

#: A repository connector that authenticates as a GitHub App installation
#: (C5, ``shared.connectors.github_app``): the App and the installation, and
#: the REST API base of a GitHub Enterprise Server; not secret.
_GITHUB_APP: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["app_id", "installation_id"],
    "properties": {
        "app_id": {"type": "string", "pattern": "^[1-9][0-9]{0,19}$"},
        "installation_id": {"type": "string", "pattern": "^[1-9][0-9]{0,19}$"},
        "api_base": {"type": "string"},
    },
}
#: The App's private key: SRW signs App JWTs with it and mints installation
#: tokens; it never leaves SRW.
_GITHUB_APP_KEY = CredentialSlot(
    "private_key",
    "secret_string",
    {"type": "object", "properties": {"private_key": _SECRET_FILE}},
    update="replace",
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
            # PEM certificates the git swap driver alone trusts the upstream
            # with (a forge behind a private CA); not a secret.
            "upstream_ca": {"type": "string", "x-srw-multiline": True},
            "github_app": _GITHUB_APP,
            "endpoint": _ENDPOINT,
            "default_branch": _MIRROR,
            "auth_method": _AUTH_METHOD,
        },
    },
    legacy_connection_url="required",
    credential_slots=(
        # Where the git swap driver (C3, srw.git-swap/v1) is installed, a
        # token repository on HTTPS is served through it and its workspace
        # holds a lease only; otherwise the token lands in the clone URL
        # (connectors.drivers.gitSwap.fallback). The key is loaded into an
        # ssh-agent (C1).
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
        _GITHUB_APP_KEY,
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
            "For a token or deploy-key connector only the repo tools are "
            "read-only; the workspace shell can still push with the checkout's "
            "credentials. A GitHub App connector's read-only is enforced: its "
            "token is minted with contents: read, so no push succeeds."
        ),
        read_only_advisory=True,
        read_write_enforced_by=(
            "The forge token or deploy key decides which pushes succeed; a "
            "GitHub App connector's token is minted with contents: write for "
            "its one repository."
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
            "endpoint": _ENDPOINT,
            "default_branch": _MIRROR,
            "auth_method": _AUTH_METHOD,
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
        config_schema=_ENDPOINT_CONFIG,
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
            "endpoint": _ENDPOINT,
            "backend": _MIRROR,
            "username": _MIRROR,
            "imap": {**_SERVER_BLOCK, "readOnly": True},
            "smtp": {**_SERVER_BLOCK, "readOnly": True},
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

_MCP_ACCESS = (
    AccessLevel(
        "ReadWrite",
        1,
        "The MCP server and its credentials are the access boundary; SRW "
        "binds every tool the server lists.",
        tools="*",
    ),
)

#: Why a stdio MCP connector is refused and never delivered (connector
#: drivers D5b): the agent pod no longer runs a stdio server as its
#: subprocess. A stdio image runs as a managed MCP server instead, one
#: process per binding behind SRW's front; a remote server connects by URL.
MCP_STDIO_RETIRED = (
    "stdio MCP servers no longer run in the agent pod: run the server's image "
    "as a managed MCP server (SRW hosts it, one process per binding behind its "
    "front) or connect the server by URL (http or sse)"
)

# The stored ``mcp`` type has two drivers, by transport: a stdio server
# (``srw.mcp/v1``, the type's catalogue entry) and a remote http or sse
# server (``srw.mcp-remote/v1``). One control-plane driver implementation
# serves both; a row's Connector resource names the one its transport needs.
# The stdio path is retired (D5b, MCP_STDIO_RETIRED): a stored stdio row is
# kept (it can be read, renamed, unpublished and deleted, or moved to a
# remote transport) but no new one is created or published and none is
# delivered or tested.
MCP_SPEC = DriverSpec(
    name="srw.mcp/v1",
    legacy_type="mcp",
    title="MCP Server",
    guide_topic="datasources",
    plane="harness",
    delivery_forms=("mcp_client",),
    config_schema=_config(transport={"enum": ["stdio"], "readOnly": True}),
    # The stored type's column: a remote row of the type has its URL there,
    # and with stdio retired (D5b) every new row of the type is remote.
    legacy_connection_url="required",
    credential_slots=(
        CredentialSlot(
            "server",
            "secret_string",
            {
                "type": "object",
                "properties": {
                    "transport": {"enum": ["stdio"]},
                    "command": {"type": "string"},
                    "args": {"type": "array", "items": {"type": "string"}},
                    "env": {"type": "object", "writeOnly": True},
                },
            },
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_MCP_ACCESS,
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements=(
        "Retired: a stdio server no longer runs in the agent pod; a stored "
        "stdio connector is never delivered. Run its image as a managed MCP "
        "server, or connect it by URL."
    ),
    deployment_gate="mcp_datasources",
    holds_upstream_credentials=True,
)
MCP_REMOTE_SPEC = DriverSpec(
    name="srw.mcp-remote/v1",
    legacy_type="mcp",
    title="MCP Server (remote)",
    guide_topic="datasources",
    plane="harness",
    delivery_forms=("mcp_client",),
    config_schema=_config(
        endpoint=_ENDPOINT,
        transport={"enum": ["http", "sse"], "readOnly": True},
        auth_type={"enum": ["none", "bearer", "headers"], "readOnly": True},
        header_names={
            "type": "array",
            "items": {"type": "string"},
            "readOnly": True,
        },
    ),
    legacy_connection_url="required",
    credential_slots=(
        CredentialSlot(
            "server",
            "secret_string",
            {
                "type": "object",
                "properties": {
                    "transport": {"enum": ["http", "sse"]},
                    "auth": {"type": "object", "writeOnly": True},
                },
            },
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_MCP_ACCESS,
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: the agent process is the MCP client.",
    deployment_gate="mcp_datasources",
    holds_upstream_credentials=True,
)
#: The driver a remote MCP connector's resource names.
MCP_REMOTE_DRIVER = MCP_REMOTE_SPEC.name


def _credential_file(
    type_id: str,
    name: str,
    title: str,
    kind: CredentialKind,
    file_word: str,
    *,
    config: Mapping[str, Mapping[str, Any]] | None = None,
    access_levels: tuple[AccessLevel, ...] | None = None,
    requirements: str = "",
) -> DriverSpec:
    return DriverSpec(
        name=f"srw.{name}/v1",
        legacy_type=type_id,
        title=title,
        guide_topic="datasources",
        plane="bind_time",
        delivery_forms=("credential_file",),
        config_schema=_config(
            endpoint=_ENDPOINT, files=_FILE_TARGETS, **(config or {})
        ),
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
        access_levels=access_levels or _declared_only(file_word),
        supported_backends=SHELL_BACKENDS,
        workspace_requirements=(
            "A shell workspace: each file is written under the home's private "
            "credential store over a secret channel, with its mode, and linked "
            "at its target path; kubeconfigs are merged for kubectl." + requirements
        ),
        holds_upstream_credentials=True,
    )


#: A kubeconfig connector's optional TokenRequest minting (C5,
#: ``shared.connectors.token_request``): its stored kubeconfig then mints a
#: short-lived token for the target ServiceAccount at each bind and is never
#: delivered; the workspace's kubeconfig holds the minted token only.
_TOKEN_REQUEST: Mapping[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": ["namespace", "service_account"],
    "properties": {
        "namespace": {"type": "string"},
        "service_account": {"type": "string"},
        # No "default": a blank form must send no config (minting off); the
        # parser's lifetime is 3600 s when the config omits it.
        "expiration_seconds": {
            "type": "integer",
            "minimum": 600,
            "maximum": 86400,
            "description": "Token lifetime in seconds (3600 when omitted).",
        },
        "audiences": {"type": "array", "items": {"type": "string"}, "maxItems": 10},
    },
}

KUBECONFIG_SPEC = _credential_file(
    "kubeconfig",
    "kubeconfig",
    "Kubeconfig",
    "kubeconfig",
    "kubeconfig",
    config={"token_request": _TOKEN_REQUEST},
    access_levels=(
        AccessLevel(
            "ReadOnly",
            0,
            "Told to the agent only; the same kubeconfig is delivered either "
            "way. With TokenRequest minting, the cluster's RBAC for the target "
            "ServiceAccount decides: SRW cannot narrow a minted token.",
            advisory=True,
        ),
        AccessLevel(
            "ReadWrite",
            1,
            "The upstream credential decides what is allowed; with "
            "TokenRequest minting, the cluster's RBAC for the target "
            "ServiceAccount.",
        ),
    ),
    requirements=(
        " With TokenRequest minting the workspace's kubeconfig holds only a "
        "short-lived token SRW minted for the target ServiceAccount, bound to "
        "a Secret SRW deletes when the execution ends; the minting credential "
        "stays with SRW."
    ),
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
            "endpoint": _ENDPOINT,
            "files": _FILE_TARGETS,
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

#: A development driver behind a credential lease (slice C2). It stores a
#: fake upstream secret that never leaves SRW: the agent receives only a lease
#: token, and the exchange hands the secret to a driver identity of this
#: connector. It lets a k3d gate drive the lease lifecycle before the first
#: real lease driver (the swap driver of C3) exists. The orchestrator installs
#: it only when ``orchestrator.connectorLeases.probeDriver`` is on, and no
#: catalogue lists it.
LEASE_PROBE_SPEC = DriverSpec(
    name="srw.lease-probe/v1",
    legacy_type="lease_probe",
    title="Lease probe (development)",
    plane="bind_time",
    delivery_forms=("lease_token",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {"upstream": {"type": "string", "maxLength": 512}},
    },
    credential_slots=(
        CredentialSlot(
            "secret",
            "secret_string",
            {"type": "object", "properties": {"secret": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    access_levels=(
        AccessLevel("ReadOnly", 0, "The lease exchange refuses a write operation."),
        AccessLevel("ReadWrite", 1, "The lease exchange allows reads and writes."),
    ),
    default_access="ReadWrite",
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A shell workspace: a lease token is written under "
        "~/.srw-credentials/leases/; the secret itself never is."
    ),
    publishable=False,
    credential_delivery="lease",
)

#: A development service-plane driver (connector drivers D5). Its pod runs
#: SRW's srw-driver-echo image (built by Tilt only): an HTTP service that
#: answers with its request file's non-secret fields, calls the lease exchange
#: with its own driver identity when asked, and probes TCP addresses from
#: inside its pod, so a k3d gate can prove the service plane's hosting,
#: reachability, pinned egress and identity. Its fake upstream secret stays
#: behind a lease, as the probe's does. The orchestrator installs it only when
#: ``connectors.drivers.echo.enabled`` names its image; no catalogue lists it.
ECHO_SERVICE_SPEC = DriverSpec(
    name="srw.echo-service/v1",
    legacy_type="echo_service",
    title="Echo service (development)",
    plane="service",
    delivery_forms=("lease_token",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "required": ["host", "port"],
        "properties": {
            "host": {"type": "string", "minLength": 1, "maxLength": 253},
            "port": {"type": "integer", "minimum": 1, "maximum": 65535},
            "message": {"type": "string", "maxLength": 256},
        },
    },
    credential_slots=(
        CredentialSlot(
            "secret",
            "secret_string",
            {"type": "object", "properties": {"secret": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    access_levels=(
        AccessLevel("ReadOnly", 0, "The lease exchange refuses a write operation."),
        AccessLevel("ReadWrite", 1, "The lease exchange allows reads and writes."),
    ),
    default_access="ReadWrite",
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A shell workspace: a lease token is written under "
        "~/.srw-credentials/leases/; the echo pod is reached on its srw-driver "
        "port from the agent pod and from this binding's workspace."
    ),
    egress=(EgressRule("${config.host}", ("${config.port}",)),),
    publishable=False,
    holds_upstream_credentials=True,
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness", "workspace"),
        resources={"limits": {"cpu": "100m", "memory": "64Mi"}},
        start_seconds=10,
    ),
)

# ---------------------------------------------------------------------------
# Managed MCP servers (connector drivers D5a)
#
# An MCP server image SRW hosts as a service driver: a shared pod per
# connector with SRW's front in front of the server. The agent process is the
# client and holds only a lease token; the front checks it with the lease
# exchange, injects the upstream credential the exchange returns, and hides
# and refuses the tools the binding's access level does not allow (the
# ``mcp`` block's tool classes). An installation turns each one on with its
# image in the chart (``connectors.drivers.managedMcp``); no catalogue lists
# one that is off.
# ---------------------------------------------------------------------------


def _managed_mcp_access(read_only: str, read_write: str) -> tuple[AccessLevel, ...]:
    return (
        AccessLevel("ReadOnly", 0, read_only, tools="*"),
        AccessLevel("ReadWrite", 1, read_write, tools="*"),
    )


_MANAGED_MCP_TOOL_ACCESS: Mapping[str, Any] = {
    "ReadOnly": ["read"],
    "ReadWrite": ["read", "write"],
}
_FRONT_HIDES_WRITE_TOOLS = (
    "SRW's front hides and refuses every tool the driver's spec does not "
    "class as read; a tool the server adds later stays hidden until it does."
)

#: The read tools of the official Gitea MCP server (gitea-mcp 1.8). Exact
#: names, never patterns: a tool the server adds is a write tool until it is
#: classed here. ``attachment_read`` and ``actions_run_read`` read Gitea but
#: write files in the shared pod (their download methods take a
#: caller-chosen ``output_path``), so they are write tools.
GITEA_MCP_READ_TOOLS: tuple[str, ...] = (
    "get_gitea_mcp_server_version",
    "get_me",
    "get_user_orgs",
    "search_users",
    "search_org_teams",
    "search_repos",
    "search_issues",
    "notification_read",
    "label_read",
    "milestone_read",
    "wiki_read",
    "timetracking_read",
    "package_read",
    "project_read",
    "list_issues",
    "issue_read",
    "list_pull_requests",
    "pull_request_read",
    "actions_config_read",
    "list_my_repos",
    "list_org_repos",
    "get_repository_tree",
    "get_file_contents",
    "get_dir_contents",
    "list_branches",
    "get_tag",
    "list_tags",
    "list_commits",
    "get_commit",
    "get_release",
    "get_latest_release",
    "list_releases",
)
_ACCESS_CHOICE: Mapping[str, Any] = {
    "enum": ["ReadOnly", "ReadWrite"],
    "title": "Access",
}

#: The official Gitea MCP server (docker.gitea.com/gitea-mcp-server), a
#: stock image that speaks streamable HTTP and takes the Gitea token on each
#: request, so the front injects it from the lease exchange and the pod's
#: Secret holds none. Its connector names the Gitea instance; the host and
#: port it pins are derived from that URL.
GITEA_MCP_SPEC = DriverSpec(
    name="srw.gitea-mcp/v1",
    legacy_type="gitea_mcp",
    title="Gitea (managed MCP server)",
    guide_topic="datasources",
    plane="service",
    delivery_forms=("mcp_client",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "required": ["url"],
        "properties": {
            "url": {
                "type": "string",
                "title": "Gitea URL",
                "maxLength": 512,
                "pattern": "^https?://[^/?#@\\s]+/?$",
            },
            "host": _MIRROR,
            "port": {"type": "integer", "readOnly": True},
            "access": _ACCESS_CHOICE,
        },
    },
    credential_slots=(
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_managed_mcp_access(
        _FRONT_HIDES_WRITE_TOOLS,
        "The Gitea token decides what the server may do.",
    ),
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements=(
        "None: the agent process is the MCP client; the server runs in its "
        "own pod and the token never reaches the agent or the workspace."
    ),
    egress=(EgressRule("${config.host}", ("${config.port}",)),),
    holds_upstream_credentials=True,
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness",),
        resources={"limits": {"cpu": "500m", "memory": "128Mi"}},
        start_seconds=20,
        mcp={
            "transport": "http",
            "port": 8091,
            "path": "/mcp",
            "protocol": "legacy",
            "tools": {"read": list(GITEA_MCP_READ_TOOLS)},
            "access": _MANAGED_MCP_TOOL_ACCESS,
            "credential": {"header": "Authorization", "scheme": "Bearer"},
            "env": {"MCP_MODE": "http", "GITEA_HOST": "${config.url}"},
            "args": ["-b", "127.0.0.1", "-p", "8091"],
            "max_in_flight_per_binding": 4,
            "tool_pinning": "warn",
        },
    ),
)

#: A development managed MCP server (D5a): SRW's srw-mcp-test image, built
#: by Tilt only. Its read tools report what the front injected (a digest of
#: the bearer it received) and try to leak it back, so a k3d gate can prove
#: injection, scrubbing and the tool filter; its write tools change an
#: in-memory note. Installed only when ``connectors.drivers.mcpTest`` names
#: its image; no catalogue lists it.
MCP_TEST_SPEC = DriverSpec(
    name="srw.mcp-test/v1",
    legacy_type="mcp_test",
    title="MCP test server (development)",
    plane="service",
    delivery_forms=("mcp_client",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        # The server reports the message (whoami): what a gate set reaches
        # the server only through its configuration, never a secret.
        "required": ["message"],
        "properties": {
            "message": {"type": "string", "maxLength": 256},
            "access": _ACCESS_CHOICE,
        },
    },
    credential_slots=(
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_managed_mcp_access(
        _FRONT_HIDES_WRITE_TOOLS,
        "The test server allows every tool.",
    ),
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: the agent process is the MCP client.",
    publishable=False,
    holds_upstream_credentials=True,
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness",),
        resources={"limits": {"cpu": "100m", "memory": "64Mi"}},
        start_seconds=10,
        mcp={
            "transport": "http",
            "port": 8091,
            "path": "/mcp",
            "protocol": "legacy",
            "tools": {
                "read": ["whoami", "notes_list", "notes_read", "leak_credential"]
            },
            "access": _MANAGED_MCP_TOOL_ACCESS,
            "credential": {"header": "Authorization", "scheme": "Bearer"},
            "env": {"MCP_TEST_MESSAGE": "${config.message}"},
            "args": ["-listen", "127.0.0.1:8091"],
            "max_in_flight_per_binding": 4,
            "tool_pinning": "warn",
        },
    ),
)

#: The read tools of the official MCP memory server (Docker's stock
#: mcp/memory image): they read the knowledge graph, every other tool
#: changes it. Exact names: a tool the image adds is a write tool.
MEMORY_MCP_READ_TOOLS: tuple[str, ...] = ("read_graph", "search_nodes", "open_nodes")

#: A development managed MCP server for stdio images (D5b): Docker's stock
#: mcp/memory image, unchanged, behind SRW's stdio bridge and front, one
#: process per binding, each as a user of its own. Its graph is a file in
#: the process's private directory, so it is the binding's alone and lives
#: as long as its process. The server takes no credential; the connector's
#: token is delivered to each binding's process anyway
#: (MCP_STDIO_TEST_TOKEN), so the k3d gate can prove where a stdio server's
#: credential goes. Installed only when ``connectors.drivers.mcpStdioTest``
#: is on; no catalogue lists it.
MCP_STDIO_TEST_SPEC = DriverSpec(
    name="srw.mcp-stdio-test/v1",
    legacy_type="mcp_stdio_test",
    title="MCP stdio test server (development)",
    plane="service",
    delivery_forms=("mcp_client",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "properties": {"access": _ACCESS_CHOICE},
    },
    credential_slots=(
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_managed_mcp_access(
        _FRONT_HIDES_WRITE_TOOLS,
        "The memory server allows every tool.",
    ),
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: the agent process is the MCP client.",
    publishable=False,
    holds_upstream_credentials=True,
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness",),
        # The bridge and up to five Node processes of the server (four
        # bindings and the probe), each held to a 64 MiB heap: one process
        # running out of heap exits alone, while the container's memory
        # limit is one cgroup, whose OOM kill takes every process down.
        resources={"limits": {"cpu": "500m", "memory": "640Mi"}},
        start_seconds=20,
        mcp={
            "transport": "stdio",
            "path": "/mcp",
            "protocol": "legacy",
            "tools": {"read": list(MEMORY_MCP_READ_TOOLS)},
            "access": _MANAGED_MCP_TOOL_ACCESS,
            "credential": {"env": "MCP_STDIO_TEST_TOKEN"},
            "env": {
                # The image's root filesystem is read-only: each binding's
                # graph lives in its process's private directory.
                "MEMORY_FILE_PATH": "${binding.home}/memory.json",
                "NODE_OPTIONS": "--max-old-space-size=64",
            },
            "stdio_mode": "process-per-binding",
            "max_bindings_per_pod": 4,
            "idle_seconds": 600,
            "max_in_flight_per_binding": 4,
            "tool_pinning": "warn",
        },
    ),
)

#: The read tools of SRW's MCP test server in stdio mode: its own, and the
#: probe tools, which report only whether an attempt was refused.
MCP_STDIO_PROBE_READ_TOOLS: tuple[str, ...] = (
    "whoami",
    "notes_list",
    "notes_read",
    "leak_credential",
    "self_status",
    "probe_path",
    "probe_socket",
    "probe_signal",
)

#: A development managed MCP server (D5b): SRW's srw-mcp-test image in
#: stdio mode (``-stdio``, built by Tilt only), behind SRW's stdio bridge
#: and front, one process per binding, each as a user of its own. Its probe
#: tools look around the pod from inside a binding's process, as a
#: compromised server could, so the k3d gate can prove that one binding's
#: process reaches neither another's environment, directory or process nor
#: the bridge. Installed only when ``connectors.drivers.mcpStdioProbe`` is on
#: (it runs the ``mcpTest`` image); no catalogue lists it.
MCP_STDIO_PROBE_SPEC = DriverSpec(
    name="srw.mcp-stdio-probe/v1",
    legacy_type="mcp_stdio_probe",
    title="MCP stdio probe server (development)",
    plane="service",
    delivery_forms=("mcp_client",),
    config_schema={
        "type": "object",
        "additionalProperties": False,
        "required": ["message"],
        "properties": {
            "message": {"type": "string", "maxLength": 256},
            "access": _ACCESS_CHOICE,
        },
    },
    credential_slots=(
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            required=True,
            update="replace",
        ),
    ),
    tool_category="mcp",
    access_levels=_managed_mcp_access(
        _FRONT_HIDES_WRITE_TOOLS,
        "The test server allows every tool.",
    ),
    default_access="ReadWrite",
    supported_backends=ALL_BACKENDS,
    workspace_requirements="None: the agent process is the MCP client.",
    publishable=False,
    holds_upstream_credentials=True,
    credential_delivery="lease",
    service=ServiceSpec(
        port=8080,
        callers=("harness",),
        resources={"limits": {"cpu": "200m", "memory": "128Mi"}},
        start_seconds=10,
        mcp={
            "transport": "stdio",
            "path": "/mcp",
            "protocol": "legacy",
            "tools": {"read": list(MCP_STDIO_PROBE_READ_TOOLS)},
            "access": _MANAGED_MCP_TOOL_ACCESS,
            "credential": {"env": "MCP_TEST_TOKEN"},
            "env": {"MCP_TEST_MESSAGE": "${config.message}"},
            "command": ["/srw-mcp-test", "-stdio", "-credential-env", "MCP_TEST_TOKEN"],
            "stdio_mode": "process-per-binding",
            "max_bindings_per_pod": 4,
            "idle_seconds": 600,
            "max_in_flight_per_binding": 4,
            "tool_pinning": "warn",
        },
    ),
)

#: Managed MCP servers SRW ships in its catalogue (each needs its image in
#: the chart before it is installed).
MANAGED_MCP_SPECS: tuple[DriverSpec, ...] = (GITEA_MCP_SPEC,)

# ---------------------------------------------------------------------------
# The git swap driver (connector drivers C3)
#
# A git smart-HTTP reverse proxy in a shared service pod per connector, not a
# second Git server. It serves the token repositories of the ``repository``
# type (a variant of srw.repository/v1, found by name): the workspace keeps
# the clean upstream URL as its remote, and its git reaches the driver
# through a per-binding ``insteadOf`` with the lease token from a credential
# helper; the driver exchanges the lease for the forge token per request and
# never takes a host from the request. SRW's own image
# (docker/Dockerfile.driver-git-swap); installed only when the chart turns
# it on (``connectors.drivers.gitSwap``) with service-pod hosting.
# ---------------------------------------------------------------------------

GIT_SWAP_SPEC = DriverSpec(
    name="srw.git-swap/v1",
    legacy_type="repository",
    title="Repository (git swap driver)",
    guide_topic="datasources",
    plane="service",
    delivery_forms=("checkout", "lease_token"),
    # A repository row's config, plus what its pod is built from: the clean
    # upstream URL and the host its egress pins (derived from the row's URL).
    config_schema={
        "type": "object",
        "properties": {
            "forge": {"enum": list(FORGES)},
            "endpoint": _ENDPOINT,
            "default_branch": _MIRROR,
            "auth_method": _AUTH_METHOD,
            "upstream_ca": {"type": "string", "x-srw-multiline": True},
            "github_app": _GITHUB_APP,
            "upstream": _MIRROR,
            "host": _MIRROR,
        },
    },
    credential_slots=(
        # Never the workspace: the driver injects it upstream per request,
        # and the agent process keeps it for the forge API (pull requests).
        # A GitHub App connector stores the App's key instead: the exchange
        # hands the driver an installation token SRW mints at the binding's
        # level and mints again before it expires (C5).
        CredentialSlot(
            "token",
            "secret_string",
            {"type": "object", "properties": {"token": _SECRET}},
            update="replace",
        ),
        _GITHUB_APP_KEY,
    ),
    tool_category="repo",
    access_levels=(
        AccessLevel(
            "ReadOnly",
            0,
            "SRW's git swap driver refuses both git-receive-pack routes, by "
            "request path, and the lease exchange refuses it the write "
            "credential; the workspace holds a lease token, never the forge "
            "token. A GitHub App connector's token is minted with contents: "
            "read as well.",
            tools=("repo_pull", "repo_pr_status"),
        ),
        AccessLevel(
            "ReadWrite",
            1,
            "Branch pushes only: the driver refuses ref deletes and any ref "
            "outside refs/heads/ with the reason; fast-forward and protected "
            "branches stay with the upstream's branch protection and the "
            "forge token's own permissions.",
            tools=(
                "repo_checkout",
                "repo_commit",
                "repo_push",
                "repo_pull",
                "repo_open_pr",
                "repo_pr_status",
            ),
        ),
    ),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "git and a POSIX shell, on a container or same-cluster VM workspace "
        "that reaches the driver namespace. The remote stays the "
        "clean HTTPS upstream URL; ~/.gitconfig includes SRW's wiring under "
        "~/.srw-credentials/git/, which every git in the workspace (the "
        "agent's, IDE terminals, ssh-gateway sessions) reads. HTTPS upstreams "
        "on port 443 only; Git LFS is unsupported; submodules only as "
        "relative URLs within the same repository path."
    ),
    egress=(EgressRule("${config.host}", (443,)),),
    holds_upstream_credentials=True,
    credential_delivery="lease",
    harness_credentials=("token",),
    service=ServiceSpec(
        port=8443,
        callers=("workspace",),
        tls=True,
        resources={"limits": {"cpu": "1", "memory": "128Mi"}},
        start_seconds=20,
        # A first clone waits for a cold pod: keep one an hour after its
        # last binding ends (C3 review S1).
        idle_seconds=3600,
    ),
)
#: Service drivers SRW ships as its own images (each off until the chart
#: names its image).
OFFICIAL_SERVICE_SPECS: tuple[DriverSpec, ...] = (GIT_SWAP_SPEC,)

# ---------------------------------------------------------------------------
# Registered image drivers (connector drivers D6)
#
# A connector of a driver someone registered (an image with its own spec) is
# stored with this one type. The registration's spec governs its config, its
# credentials and its bind; what SRW and the agent key on the stored type
# (the wire entry, the workspace backends, live attach and detach) follows
# this spec: a bind-time image returns data in the env_file and
# credential_file forms only (``shared.connectors.registration``), and SRW
# delivers it to the workspace as an environment or credential-file
# connector's. No catalogue lists it: the registrations are the catalogue.
# ---------------------------------------------------------------------------

IMAGE_DRIVER_SPEC = DriverSpec(
    name="srw.image-driver/v1",
    legacy_type="image_driver",
    title="Registered driver",
    plane="bind_time",
    delivery_forms=("env_file", "credential_file"),
    config_schema={"type": "object"},
    access_levels=_declared_only("binding"),
    supported_backends=SHELL_BACKENDS,
    workspace_requirements=(
        "A shell workspace: what the driver's bind returns is written under "
        "~/.srw-credentials/ (variables) and linked at the allowed credential "
        "file locations (files)."
    ),
    holds_upstream_credentials=True,
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
#: Drivers that serve some rows of a stored type whose catalogue entry is
#: another driver's, each listed after that driver.
TYPE_VARIANT_SPECS: tuple[DriverSpec, ...] = (MCP_REMOTE_SPEC,)
BUILTIN_SPECS: tuple[DriverSpec, ...] = MANIFEST_SPECS + tuple(
    variant
    for spec in DATASOURCE_SPECS
    for variant in (
        spec,
        *(v for v in TYPE_VARIANT_SPECS if v.legacy_type == spec.legacy_type),
    )
)
#: Drivers an installation turns on for development only. Their stored types
#: resolve (an agent must read what it is sent) but no catalogue lists them.
DEVELOPMENT_SPECS: tuple[DriverSpec, ...] = (
    LEASE_PROBE_SPEC,
    ECHO_SERVICE_SPEC,
    MCP_TEST_SPEC,
    MCP_STDIO_TEST_SPEC,
    MCP_STDIO_PROBE_SPEC,
)

#: Stored types that carry connectors of registered image drivers (D6).
#: Their type resolves; no catalogue lists them.
HOSTED_SPECS: tuple[DriverSpec, ...] = (IMAGE_DRIVER_SPEC,)

_BY_TYPE: dict[str, DriverSpec] = {
    spec.legacy_type: spec
    for spec in DATASOURCE_SPECS + MANAGED_MCP_SPECS + DEVELOPMENT_SPECS + HOSTED_SPECS
    if spec.legacy_type
}
LEGACY_TYPE_IDS: tuple[str, ...] = tuple(
    spec.legacy_type for spec in DATASOURCE_SPECS if spec.legacy_type
)

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


def mcp_spec_for(credentials: Any) -> DriverSpec:
    """The MCP driver a server's stored credentials need: ``srw.mcp/v1`` for
    a stdio server, ``srw.mcp-remote/v1`` for any other transport (http, the
    default, or sse)."""
    transport = (
        credentials.get("transport") if isinstance(credentials, Mapping) else None
    )
    if str(transport or "http").lower().strip() == "stdio":
        return MCP_SPEC
    return MCP_REMOTE_SPEC


def git_swap_entry(row: Any) -> bool:
    """Whether a payload entry is a token repository bound through the git
    swap driver (C3): its ``git_swap`` block, empty until the lease step
    decides it can be served and fills in the driver's endpoint, and
    without an ``unavailable`` reason (a token repository the installation
    refuses instead) or a ``fallback`` one (a token repository the lease
    step put on the installation's token-in-URL fallback)."""
    get = getattr(row, "get", None)
    if not callable(get) or get("type") != REPOSITORY_SPEC.legacy_type:
        return False
    block = get("git_swap")
    return (
        isinstance(block, Mapping)
        and "unavailable" not in block
        and "fallback" not in block
    )


def driver_spec_for_row(row: Any) -> DriverSpec | None:
    """The driver serving one stored row or payload entry.

    The type's driver (:func:`spec_for_row`), except where the row's own
    fields pick a variant of it: an ``mcp`` row names its driver by
    transport, and a repository entry its bind routed through the git swap
    driver names that.
    """
    spec = spec_for_row(row)
    if spec is MCP_SPEC:
        return mcp_spec_for(row.get("credentials"))
    if spec is REPOSITORY_SPEC and git_swap_entry(row):
        return GIT_SWAP_SPEC
    return spec


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
