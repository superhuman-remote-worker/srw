"""Shared datasource setup logic for both job agents and persistent sessions.

Processes datasource configs received from the orchestrator: connects managed
connectors, injects env vars for CLI access, clones repositories onto the
workspace backend, materializes credential files (kubeconfig, ssh_key,
generic_file), and renders the workspace-facts block in README.md.

Repository datasources are cloned exclusively on the workspace via
``clone_repository_datasources()`` (GitManager + shell-capable backend).
There is no agent-local clone path — the former subprocess ``git clone``
branch wrote credentials and repos onto the agent pod and was removed
(knowledge-base/knowledge/features/no_workspace_agent_mode.md §9.4).
"""

import logging
import os
import re
import shlex
import subprocess
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

from agent.services.knowledge.bindings import native_kb_project_id
from shared.credential_connectors import ENV_CONNECTOR_TYPES, collect_credential_env
from shared.datasource_policy import (
    EMAIL_TIER_ORDER as EMAIL_TIER_ORDER,
    EMAIL_TIER_TOOLS as EMAIL_TIER_TOOLS,
    DATASOURCE_TOOL_MAP as DATASOURCE_TOOL_MAP,
    email_effective_access as email_effective_access,
    datasource_tool_categories as datasource_tool_categories,
    resolve_repo_clone_names as resolve_repo_clone_names,
)

logger = logging.getLogger(__name__)

# Kept in sync with orchestrator/security/credential_files.py. The orchestrator
# stores ``target_path`` already resolved against ``/home/srw``; at materialization
# time we may need to remap to a different home (tests use a tmp dir).
AGENT_HOME = "/home/srw"
CREDENTIAL_FILE_TYPES = frozenset({"kubeconfig", "ssh_key", "generic_file"})


def _slugify(name: str) -> str:
    """Convert a datasource name to a lowercase slug for env vars / filenames."""
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def _ds_slug_hyphen(name: str) -> str:
    """Hyphenated slug for filenames and kubeconfig context prefixes.

    Matches ``slugify_datasource_name`` in
    ``src/orchestrator/security/credential_files.py``.
    """
    s = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return s or "unnamed"


def process_datasources(
    ds_configs: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], List[str]]:
    """Process datasource configs and create connections/env vars.

    ALL managed connectors (postgresql, neo4j, mongodb, webdav) get a real
    tool connection now — read-write ones included. The former CLI-mode
    routing for read-write managed connectors (env vars / pg_service.conf,
    no connection) was a dead path on remote workspace backends: the env
    landed in the agent process while the shell runs on the workspace host,
    leaving read-write datasources with no access at all. See
    knowledge-base/knowledge/issues/datasource_cli_mode_dead_on_remote.md (fix direction 1);
    the inject_* helpers below are kept for a future real CLI-forwarding
    feature but are no longer called from here.

    Repository datasources are NOT handled here — callers filter them out
    and clone via clone_repository_datasources() once the workspace backend
    is ready. Any repository entry that still reaches this function is
    skipped with a warning; there is no agent-local clone path.

    Args:
        ds_configs: List of datasource config dicts from the orchestrator.

    Returns:
        Tuple of (datasources_dict, client_registry, cli_ds_types):
        - datasources_dict: Connection objects keyed by type for ToolContext
        - client_registry: Parent clients (e.g. MongoClient) for cleanup
        - cli_ds_types: Always empty since the CLI-mode retirement; kept in
          the signature so callers' prompt-block plumbing (the future CLI
          feature's seam) stays in place.
    """
    datasources_dict: Dict[str, Any] = {}
    client_registry: Dict[str, Any] = {}
    cli_ds_types: List[str] = []

    mcp_list: List[Dict[str, Any]] = []
    connector_list: List[Dict[str, Any]] = []

    for ds in ds_configs:
        ds_type = ds.get("type")
        if not ds_type:
            continue

        if ds_type in ENV_CONNECTOR_TYPES:
            continue
        elif ds_type == "repository":
            logger.warning(
                "Repository datasource %r ignored by process_datasources(); "
                "repos clone onto the workspace backend via "
                "clone_repository_datasources().",
                ds.get("name", "unnamed"),
            )
        elif ds_type == "kb":
            # External OKF KBs are centrally indexed. The agent receives only
            # datasource id/display/config metadata and never opens a connector
            # or clones the repository.
            logger.debug(
                "OKF Knowledge Base %r handled by KnowledgeStore bindings",
                ds.get("name", "unnamed"),
            )
        elif ds_type == "mcp":
            mcp_list.append(ds)
        else:
            connector_list.append(ds)

    # ENV connectors are delivered by install_workspace_credentials() after
    # workspace initialization. Never put them in the reused agent process.

    # Managed connectors: create tool connections. The registry is
    # TYPE-keyed (last-one-wins), and datasource_tool_categories() binds
    # write tools when ANY datasource of a type is read-write — so process
    # read-only entries first and read-write last, ensuring the connection
    # that wins the type slot matches the granted tool surface (write tools
    # must never end up bound to a read-only-linked connection).
    for ds in sorted(
        connector_list, key=lambda d: not d.get("project_read_only", False)
    ):
        ds_type = ds["type"]
        try:
            conn, client = create_datasource_connection(ds)
            datasources_dict[ds_type] = conn
            if client:
                client_registry[ds_type] = client
            logger.info(
                "Connected to %s datasource: %s (%s)",
                ds_type,
                ds.get("name", "unnamed"),
                "read-only" if ds.get("project_read_only", False) else "read-write",
            )
        except Exception as e:
            logger.warning("Failed to connect to %s datasource: %s", ds_type, e)

    # ToolContext's datasource registry is keyed by type, so one manager must
    # aggregate every attached MCP server. Construction performs validation
    # only; callers connect it asynchronously once their runtime is ready.
    if mcp_list:
        from agent.tools.mcp import MCPManager

        datasources_dict["mcp"] = MCPManager(mcp_list)

    return datasources_dict, client_registry, cli_ds_types


def install_workspace_credentials(
    ds_configs: List[Dict[str, Any]], workspace: Any
) -> None:
    """Deliver attached ENV connectors to the workspace over its own transport."""
    values = collect_credential_env(ds_configs)
    if not values:
        return
    if workspace is None or not workspace.backend.supports_shell:
        raise ValueError("Credential connectors require a sandbox or VM workspace")
    workspace.backend.install_credential_environment(values)


def close_datasource_connections(
    connections: Dict[str, Any],
    clients: Dict[str, Any],
) -> None:
    """Close all datasource connections and parent clients."""
    for ds_type, conn in connections.items():
        try:
            if hasattr(conn, "close"):
                conn.close()
                logger.debug("Closed %s datasource connection", ds_type)
        except Exception as e:
            logger.warning("Error closing %s datasource: %s", ds_type, e)

    for ds_type, client in clients.items():
        try:
            if hasattr(client, "close"):
                client.close()
                logger.debug("Closed %s datasource client", ds_type)
        except Exception as e:
            logger.warning("Error closing %s datasource client: %s", ds_type, e)


# ---------------------------------------------------------------------------
# Credential file materialization (kubeconfig / ssh_key / generic_file)
# ---------------------------------------------------------------------------


def _retarget(path: str, home_dir: str) -> str:
    """Swap ``/home/srw`` for ``home_dir`` so tests can use a tmp directory.

    The orchestrator's validator already resolved ``~`` against
    ``/home/srw``; in production no swap is needed. In tests we pass a
    tmp ``home_dir`` and rewrite the prefix at write time.
    """
    if not path or home_dir == AGENT_HOME:
        return path
    if path == AGENT_HOME:
        return home_dir
    if path.startswith(AGENT_HOME + "/"):
        return home_dir + path[len(AGENT_HOME) :]
    return path


def _mkdir_tracking(path: str, created_dirs: List[str]) -> None:
    """``mkdir -p`` while recording each directory we (not the OS image) created.

    Cleanup uses this list to ``rmdir`` only the directories we made, leaving
    pre-existing ones like ``~/.ssh`` (which may have ``known_hosts``) intact.
    """
    if not path or path == "/" or os.path.isdir(path):
        return
    parent = os.path.dirname(path)
    if parent and parent != path:
        _mkdir_tracking(parent, created_dirs)
    try:
        os.mkdir(path)
        created_dirs.append(path)
    except FileExistsError:
        pass


def _prefix_kubeconfig_yaml(yaml_str: str, prefix: str) -> str:
    """Prefix every cluster/user/context name in a kubeconfig with ``<prefix>-``.

    Multi-cluster jobs merge several kubeconfigs into ``~/.kube/config``;
    without prefixing, two uploads with a context named ``default`` would
    collide. We pre-prefix per-datasource so the agent sees deterministic,
    collision-free context names like ``prod-eu-default``.

    Returns the re-emitted YAML. On a parse failure the original string is
    returned and a warning is logged — the upload may still be usable,
    just with un-prefixed contexts.
    """
    try:
        import yaml
    except ImportError:
        logger.warning("PyYAML not available; skipping kubeconfig prefixing")
        return yaml_str

    try:
        doc = yaml.safe_load(yaml_str)
    except yaml.YAMLError as e:
        logger.warning("Kubeconfig YAML parse failed (%s); writing un-prefixed", e)
        return yaml_str
    if not isinstance(doc, dict):
        return yaml_str

    def _pfx(name: Any) -> Any:
        return f"{prefix}-{name}" if isinstance(name, str) and name else name

    for cluster in doc.get("clusters") or []:
        if isinstance(cluster, dict) and "name" in cluster:
            cluster["name"] = _pfx(cluster["name"])
    for user in doc.get("users") or []:
        if isinstance(user, dict) and "name" in user:
            user["name"] = _pfx(user["name"])
    for ctx in doc.get("contexts") or []:
        if isinstance(ctx, dict):
            if "name" in ctx:
                ctx["name"] = _pfx(ctx["name"])
            inner = ctx.get("context")
            if isinstance(inner, dict):
                if "cluster" in inner:
                    inner["cluster"] = _pfx(inner["cluster"])
                if "user" in inner:
                    inner["user"] = _pfx(inner["user"])
    if doc.get("current-context"):
        doc["current-context"] = _pfx(doc["current-context"])

    return yaml.safe_dump(doc, sort_keys=False)


def _merge_kubeconfigs(
    kubeconfig_paths: List[str],
    home_dir: str,
    manifest: Dict[str, Any],
) -> Optional[str]:
    """Merge per-datasource kubeconfigs into ``~/.kube/config`` using ``kubectl``.

    Returns the merged absolute path on success. On failure (no kubectl,
    bad input) returns ``None`` and the per-datasource files remain
    available individually — the caller falls back to a colon-separated
    ``KUBECONFIG`` so kubectl can still find them.
    """
    if not kubeconfig_paths:
        return None
    merged_path = os.path.join(home_dir, ".kube", "config")
    if os.path.exists(merged_path):
        logger.warning(
            "Refusing to overwrite existing %s; agent will use per-ds KUBECONFIG list",
            merged_path,
        )
        return None
    _mkdir_tracking(os.path.dirname(merged_path), manifest["dirs"])
    env = {**os.environ, "KUBECONFIG": ":".join(kubeconfig_paths)}
    try:
        result = subprocess.run(
            ["kubectl", "config", "view", "--flatten", "--merge"],
            env=env,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )
    except FileNotFoundError:
        logger.warning("kubectl not installed; falling back to KUBECONFIG=<colon-list>")
        return None
    except subprocess.CalledProcessError as e:
        logger.warning("kubectl config view failed: %s", e.stderr.strip())
        return None
    except subprocess.TimeoutExpired:
        logger.warning("kubectl config view timed out")
        return None

    try:
        with open(merged_path, "w") as f:
            f.write(result.stdout)
        os.chmod(merged_path, 0o600)
    except OSError as e:
        logger.warning("Failed to write merged kubeconfig %s: %s", merged_path, e)
        return None
    manifest["files"].append(merged_path)
    return merged_path


def process_credential_files(
    ds_configs: List[Dict[str, Any]],
    home_dir: str = AGENT_HOME,
) -> Dict[str, Any]:
    """Materialize ``credentials.files[]`` for credential-file datasource types.

    For each ``kubeconfig``/``generic_file`` datasource (``ssh_key`` lives in
    a workspace ``ssh-agent`` and is skipped here):
      - ``mkdir -p`` the parent (recording new dirs in the manifest).
      - For ``kubeconfig`` entries, prefix cluster/user/context names with
        the datasource slug so multi-cluster merging is collision-free.
      - Write contents at the resolved absolute path with the requested mode.
      - If ``env_var`` is set, inject ``os.environ[env_var] = abs_path``.
      - Skip (warn) any path that already exists on disk — we don't clobber
        anything we didn't write.

    After all per-datasource files are written, if any kubeconfigs were
    attached, merge them via ``kubectl config view --flatten --merge`` into
    ``~/.kube/config`` and set ``KUBECONFIG`` to that path.

    Args:
        ds_configs: Datasource configs from the orchestrator (already
            validated by ``normalize_credential_files``).
        home_dir: Override the ``/home/srw`` prefix in stored target_paths.
            Production leaves the default; tests pass a tmp directory.

    Returns:
        Manifest dict consumed by :func:`cleanup_credential_files` at job
        teardown::

            {
                "files":    [abs paths written],
                "dirs":     [abs dirs we created],
                "env_vars": [env var names we set],
            }
    """
    manifest: Dict[str, Any] = {
        "files": [],
        "dirs": [],
        "env_vars": [],
    }
    kubeconfig_paths: List[str] = []

    for ds in ds_configs:
        ds_type = ds.get("type")
        # An ssh_key connector's key never reaches the agent pod or the
        # workspace disk: it is loaded into a workspace ssh-agent instead
        # (``shared.runtime.core.workspace_ssh_identity``).
        if ds_type not in CREDENTIAL_FILE_TYPES or ds_type == "ssh_key":
            continue
        creds = ds.get("credentials") or {}
        files = creds.get("files") or []
        ds_name = ds.get("name", "unnamed")
        ds_slug = _ds_slug_hyphen(ds_name)

        for entry in files:
            target_path = entry.get("target_path") or ""
            absolute = _retarget(target_path, home_dir)
            if not absolute:
                logger.warning(
                    "Skipping file entry with empty target_path on '%s'", ds_name
                )
                continue
            if os.path.exists(absolute):
                logger.warning(
                    "Refusing to overwrite existing file at %s (datasource '%s')",
                    absolute,
                    ds_name,
                )
                continue
            contents = entry.get("contents", "")
            if ds_type == "kubeconfig":
                contents = _prefix_kubeconfig_yaml(contents, ds_slug)
            mode_str = entry.get("mode") or "0600"
            try:
                mode_int = int(mode_str, 8)
            except ValueError:
                logger.warning("Bad mode %r on '%s'; using 0600", mode_str, ds_name)
                mode_int = 0o600

            parent = os.path.dirname(absolute)
            if parent:
                _mkdir_tracking(parent, manifest["dirs"])
            try:
                with open(absolute, "w") as f:
                    f.write(contents)
                os.chmod(absolute, mode_int)
            except OSError as e:
                logger.warning(
                    "Failed to write credential file %s for '%s': %s",
                    absolute,
                    ds_name,
                    e,
                )
                continue
            manifest["files"].append(absolute)
            logger.info(
                "Materialized credential file for '%s' at %s (mode %s)",
                ds_name,
                absolute,
                mode_str,
            )

            env_var = entry.get("env_var")
            if env_var:
                os.environ[env_var] = absolute
                manifest["env_vars"].append(env_var)

            if ds_type == "kubeconfig":
                kubeconfig_paths.append(absolute)

    if kubeconfig_paths:
        merged = _merge_kubeconfigs(kubeconfig_paths, home_dir, manifest)
        if merged:
            os.environ["KUBECONFIG"] = merged
        else:
            os.environ["KUBECONFIG"] = ":".join(kubeconfig_paths)
        manifest["env_vars"].append("KUBECONFIG")

    return manifest


def cleanup_credential_files(manifest: Optional[Dict[str, Any]]) -> None:
    """Undo a :func:`process_credential_files` manifest. Best-effort, never raises.

    Removes materialized files, unsets env vars, and ``rmdir``s only the
    directories the materialization step created (pre-existing dirs like
    ``~/.ssh`` are left alone).
    """
    if not manifest:
        return

    for env_var in manifest.get("env_vars", []) or []:
        os.environ.pop(env_var, None)

    for path in manifest.get("files", []) or []:
        try:
            os.unlink(path)
            logger.debug("Removed credential file %s", path)
        except FileNotFoundError:
            pass
        except OSError as e:
            logger.warning("Failed to remove credential file %s: %s", path, e)

    # Deepest first so children come out before their parents.
    for d in sorted(manifest.get("dirs", []) or [], key=lambda p: -len(p)):
        try:
            os.rmdir(d)
        except OSError:
            # Non-empty or already gone; either way, nothing to do.
            pass


# ---------------------------------------------------------------------------
# Named connection generators (Phase 2: multi-source)
# ---------------------------------------------------------------------------


def inject_postgresql_services(datasources: List[Dict[str, Any]]) -> None:
    """Generate ~/.pg_service.conf entries for all PostgreSQL datasources.

    Uses PostgreSQL's native service file for named connection profiles.
    All libpq-based tools (psql, pg_dump, etc.) support PGSERVICE.
    """
    service_file = os.path.expanduser("~/.pg_service.conf")
    os.environ["PGSERVICEFILE"] = service_file

    entries: List[str] = []
    for ds in datasources:
        slug = _slugify(ds["name"])
        url = ds.get("connection_url", "")
        creds = ds.get("credentials") or {}
        parsed = urlparse(url)

        entries.append(f"[{slug}]")
        if parsed.hostname:
            entries.append(f"host={parsed.hostname}")
        if parsed.port:
            entries.append(f"port={parsed.port}")
        if parsed.username:
            entries.append(f"user={parsed.username}")
        password = parsed.password or creds.get("password", "")
        if password:
            entries.append(f"password={password}")
        db_name = parsed.path.lstrip("/").split("?")[0]
        if db_name:
            entries.append(f"dbname={db_name}")
        entries.append("")  # blank line between services

        logger.info("Configured pg_service entry '%s' for: %s", slug, ds.get("name"))

    with open(service_file, "a") as f:
        f.write("\n".join(entries))

    # Backward compat: also set legacy env vars when only one PG datasource
    if len(datasources) == 1:
        _inject_legacy_pg_env(datasources[0])


def inject_mongodb_env_vars(datasources: List[Dict[str, Any]]) -> None:
    """Set per-datasource MONGO_{SLUG}_URI environment variables."""
    for ds in datasources:
        slug = _slugify(ds["name"]).upper()
        url = ds.get("connection_url", "")
        os.environ[f"MONGO_{slug}_URI"] = url
        logger.info("Set MONGO_%s_URI for: %s", slug, ds.get("name"))

    # Backward compat: also set MONGOSH_URI when only one
    if len(datasources) == 1:
        os.environ["MONGOSH_URI"] = datasources[0].get("connection_url", "")


def inject_neo4j_env_vars(datasources: List[Dict[str, Any]]) -> None:
    """Set per-datasource NEO4J_{SLUG}_* environment variables."""
    for ds in datasources:
        slug = _slugify(ds["name"]).upper()
        url = ds.get("connection_url", "")
        creds = ds.get("credentials") or {}
        os.environ[f"NEO4J_{slug}_URI"] = url
        os.environ[f"NEO4J_{slug}_USERNAME"] = creds.get("username", "neo4j")
        os.environ[f"NEO4J_{slug}_PASSWORD"] = creds.get("password", "")
        logger.info("Set NEO4J_%s_* for: %s", slug, ds.get("name"))

    # Backward compat: also set legacy env vars when only one
    if len(datasources) == 1:
        creds = datasources[0].get("credentials") or {}
        os.environ["NEO4J_URI"] = datasources[0].get("connection_url", "")
        os.environ["NEO4J_USERNAME"] = creds.get("username", "neo4j")
        os.environ["NEO4J_PASSWORD"] = creds.get("password", "")


def _inject_legacy_pg_env(ds: Dict[str, Any]) -> None:
    """Set legacy PGHOST/PGPORT/etc. env vars for a single PostgreSQL datasource."""
    url = ds.get("connection_url", "")
    creds = ds.get("credentials") or {}
    parsed = urlparse(url)
    if parsed.hostname:
        os.environ["PGHOST"] = parsed.hostname
    if parsed.port:
        os.environ["PGPORT"] = str(parsed.port)
    if parsed.username:
        os.environ["PGUSER"] = parsed.username
    password = parsed.password or creds.get("password", "")
    if password:
        os.environ["PGPASSWORD"] = password
    db_name = parsed.path.lstrip("/").split("?")[0]
    if db_name:
        os.environ["PGDATABASE"] = db_name


# ---------------------------------------------------------------------------
# Typed connections (read-only tool mode)
# ---------------------------------------------------------------------------


def create_datasource_connection(
    ds: Dict[str, Any],
) -> Tuple[Any, Any]:
    """Create a connection to an external datasource.

    Returns:
        Tuple of (connection, parent_client). parent_client is non-None
        only for MongoDB where the MongoClient must be kept for cleanup.
    """
    ds_type = ds["type"]
    url = ds.get("connection_url") or ""
    creds = ds.get("credentials") or {}

    if ds_type == "neo4j":
        from shared.runtime.database.neo4j_db import Neo4jDB

        db = Neo4jDB(
            uri=url,
            username=creds.get("username", "neo4j"),
            password=creds.get("password", ""),
            # A read-only link: every session reads, so the server refuses
            # writes even though the login could make them.
            read_only=bool(ds.get("project_read_only", False)),
        )
        db.connect()
        return db, None

    elif ds_type == "postgresql":
        import psycopg

        conn = psycopg.connect(url, autocommit=False)
        conn.execute("SELECT 1")
        conn.rollback()
        return conn, None

    elif ds_type == "mongodb":
        from pymongo import MongoClient

        client = MongoClient(url, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        parsed = urlparse(url)
        db_name = parsed.path.lstrip("/").split("?")[0] or "default"
        db = client[db_name]
        return db, client

    elif ds_type == "webdav":
        from webdav3.client import Client

        client = Client(
            {
                "webdav_hostname": url,
                "webdav_login": creds.get("username"),
                "webdav_password": creds.get("password"),
            }
        )
        client.list("/")
        return client, None

    elif ds_type == "email":
        # Lazy import: the email tool module ships with the email tool
        # surface — keep this module importable without it.
        from agent.tools.email.connection import EmailConnection

        config = dict(ds.get("config") or {})
        # Normalize to the effective tier (default 'draft'; a read-only
        # project link clamps to 'read' but credentials stay intact — email
        # needs a live IMAP login at every tier).
        config["access"] = email_effective_access(ds)
        return EmailConnection(creds, config), None

    else:
        raise ValueError(f"Unknown datasource type: {ds_type}")


# ---------------------------------------------------------------------------
# Repository setup
# ---------------------------------------------------------------------------


#: What an ``unavailable`` SSH identity's fixed reason code means. Anything
#: else is shown as the generic reason: the field is never echoed verbatim
#: into a README (or a log), which another user's connector would control.
_SSH_UNAVAILABLE_REASONS = {
    "ssh_endpoint_invalid": "its SSH host, user, port or repository URL is not allowed",
    "ssh_key_invalid": "its SSH key could not be parsed",
    "ssh_key_passphrase": "its SSH key is passphrase-protected",
    "known_hosts_invalid": "its pinned host key is not valid for its host",
    "default_known_hosts_invalid": "the default host keys of this deployment are invalid",
    "ssh_identity_unresolvable": "it has no stable identity",
}


def _ssh_unavailable_reason(identity: Dict[str, Any]) -> str:
    return _SSH_UNAVAILABLE_REASONS.get(
        str(identity.get("unavailable") or ""),
        "it cannot be delivered to this workspace",
    )


def _ssh_clone_target(
    ds: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]]
) -> tuple[Optional[str], str]:
    """``(clone_url, "")`` for a loaded SSH identity, else ``(None, reason)``."""

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict):
        return None, "no workspace SSH identity was delivered for it"
    if identity.get("unavailable"):
        return None, _ssh_unavailable_reason(identity)
    clone_url = str(identity.get("clone_url") or "")
    alias = str(identity.get("alias") or "")
    path = r"[A-Za-z0-9._~-]+(?:/[A-Za-z0-9._~-]+)*"
    # ``ssh://alias/path`` (absolute) or the scp form ``alias:path``, which
    # keeps a relative path relative exactly as the connector URL had it.
    if not re.fullmatch(r"srw-repo-[a-f0-9]{32}", alias) or not re.fullmatch(
        rf"(?:ssh://{alias}/|{alias}:){path}", clone_url
    ):
        return None, "its SSH identity is malformed"
    if ssh_identity_status is not None:
        status = ssh_identity_status.get(str(identity.get("authority_id") or ""))
        if status != "ready":
            return None, f"its key is not loaded ({status or 'not delivered'})"
    return clone_url, ""


#: A pre-agent clone wrote its key to ``~/.ssh/repo_<datasource-name-slug>``.
_LEGACY_KEY_NAME = re.compile(r"repo_[a-z0-9-]+")
_LEGACY_KEY_MARKER = "__srw_legacy_repo_keys__"
# Lists them: regular files only, slug-shaped names only, and only files whose
# first line is a PEM key header that is not a public key, so nothing a user
# named ``repo_*`` that is not such a key is ever touched. Runs in a subshell
# to leave the persistent ``git`` tab where it was; the marker is split in
# the command so an echoed or wrapped command line can never read as the
# answer. (The pattern avoids spelling a private-key header: every command
# stays clear of what the key-residue checks scan for.)
_LIST_LEGACY_KEYS = (
    "( cd ~/.ssh 2>/dev/null || exit 0; _srw_names=''; "
    "for _srw_f in repo_*; do "
    '[ -f "$_srw_f" ] && [ ! -L "$_srw_f" ] || continue; '
    'case "${_srw_f#repo_}" in ""|*[!a-z0-9-]*) continue;; esac; '
    'IFS= read -r _srw_first < "$_srw_f" || continue; '
    'case "$_srw_first" in *"PUBLIC KEY-----") ;; '
    '"-----BEGIN "*" KEY-----") _srw_names="$_srw_names $_srw_f";; esac; '
    "done; "
    f"printf '%s%s\\n' {_LEGACY_KEY_MARKER[:13]} "
    f'{_LEGACY_KEY_MARKER[13:]}"$_srw_names" )'
)


def _legacy_ssh_key_files(backend: Any) -> set[str]:
    """Names of pre-agent ``~/.ssh/repo_*`` key files; empty when unsure."""

    output = str(backend.shell_run(_LIST_LEGACY_KEYS, timeout=10, tab_name="git"))
    answers = [
        line.strip()[len(_LEGACY_KEY_MARKER) :]
        for line in output.splitlines()
        if line.strip().startswith(_LEGACY_KEY_MARKER)
    ]
    if not answers:
        return set()
    return {name for name in answers[-1].split() if _LEGACY_KEY_NAME.fullmatch(name)}


def _retire_legacy_ssh_key_files(
    backend: Any, outcomes: List[Tuple[str, Any]], *, sweep: bool
) -> None:
    """Delete pre-agent key files whose repository works through its alias.

    ``outcomes`` holds one ``(name slug, outcome)`` per SSH repository:
    ``True`` (cloned through its alias), ``False`` (skipped or failed) or the
    reused checkout's ``GitManager``, re-proven with ``ls-remote`` only when a
    key file is actually there. A repository's own ``repo_<slug>`` goes once
    it is proven. With ``sweep``, and every SSH repository proven, every such
    file goes, including those a renamed or detached connector left behind.
    """

    try:
        found = _legacy_ssh_key_files(backend)
        if not found:
            return
        can_sweep = sweep and all(outcome is not False for _, outcome in outcomes)
        delete: set[str] = set()
        for ds_name, outcome in outcomes:
            own = f"repo_{ds_name}"
            if outcome is False or (own not in found and not can_sweep):
                continue
            if outcome is True or outcome.remote_reachable():
                if own in found:
                    delete.add(own)
            else:
                can_sweep = False
        if can_sweep:
            delete |= found
        if delete:
            backend.shell_run(
                "( cd ~/.ssh && rm -f -- "
                + " ".join(shlex.quote(name) for name in sorted(delete))
                + " )",
                timeout=10,
                tab_name="git",
            )
            logger.info("Deleted %d pre-agent SSH key file(s)", len(delete))
    except Exception as exc:  # cleanup only: never fails a clone
        logger.warning(
            "Could not retire pre-agent SSH key files: %s", type(exc).__name__
        )


def clone_repository_datasources(
    repo_datasources: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    ssh_identity_status: Optional[Dict[str, str]] = None,
    sweep_legacy_keys: bool = True,
) -> None:
    """Clone repository datasources onto the workspace backend.

    Every operation runs on the workspace and the clone itself is
    ``GitManager.clone(backend=...)`` (git on the workspace over SSH).

    An SSH-key repository is cloned from ``ssh://srw-repo-<slug>/<path>`` (or
    ``srw-repo-<slug>:<path>`` for a relative scp-style path), an opaque alias whose key a dedicated workspace ``ssh-agent`` holds
    (``shared.runtime.core.workspace_ssh_identity``); no key file is written
    and no ``Host`` block is appended. ``ssh_identity_status`` is the
    materializer's ``{authority_id: status}``: a connector whose identity did
    not load is skipped with a warning, never cloned without its key. A
    reused checkout gets its origin reset to the alias. A pre-agent
    ``~/.ssh/repo_<slug>`` key file is deleted only once its repository is
    proven to work through the alias; ``sweep_legacy_keys`` (attach, which
    sees every repository) also deletes the files of renamed or detached
    connectors once every SSH repository is proven.

    There is deliberately NO agent-local fallback: without a shell-capable
    backend the datasources are skipped with an error. Repository
    datasources require a full workspace — lite tiers reject them at
    dispatch (knowledge-base/knowledge/features/no_workspace_agent_mode.md §4/§7).

    Args:
        repo_datasources: Datasource config dicts of type "repository".
        workspace_manager: WorkspaceManager whose backend hosts the clones;
            successful clones are registered in its ``source_repos``.
        ssh_identity_status: Which SSH identities the workspace agent holds;
            ``None`` when the caller did not materialize any.
        sweep_legacy_keys: Whether ``repo_datasources`` is the full set, so
            key files no listed repository owns may go too.
    """
    if not isinstance(ssh_identity_status, dict):
        ssh_identity_status = None
    if not repo_datasources:
        return

    backend = getattr(workspace_manager, "backend", None)
    if backend is None or not getattr(backend, "supports_shell", False):
        logger.error(
            "Repository datasources require a workspace backend with shell "
            "support; skipping %d repository datasource(s), no local clone "
            "fallback: %s",
            len(repo_datasources),
            ", ".join(ds.get("name", "unnamed") for ds in repo_datasources),
        )
        return

    from agent.managers.git_manager import GitManager

    # The workspace root is itself a durable Git repository. Without this
    # exclusion, its next checkpoint records each nested checkout as a
    # contentless gitlink; a fallback restore then recreates only an empty
    # directory. Keeping the clone root ignored means every attach can either
    # reuse the PVC copy below or re-clone it from the connector.
    try:
        if backend.exists(".gitignore"):
            content = backend.read_file(".gitignore")
            ignored = any(
                line.strip() == "repos/" for line in str(content).splitlines()
            )
            if not ignored:
                separator = "" if str(content).endswith("\n") else "\n"
                backend.append_file(
                    ".gitignore",
                    f"{separator}\n# Cloned repository datasources\nrepos/\n",
                )
        else:
            backend.write_file(
                ".gitignore", "# Cloned repository datasources\nrepos/\n"
            )
    except Exception as exc:
        logger.warning(
            "Could not exclude repository datasource clones from workspace "
            "versioning: %s",
            exc,
        )

    clone_names = resolve_repo_clone_names(repo_datasources)
    ssh_outcomes: List[List[Any]] = []
    for ds, repo_name in zip(repo_datasources, clone_names):
        # ds_name is the safe form of the user-supplied datasource label.
        ds_name = (
            re.sub(r"[^a-z0-9]+", "-", ds.get("name", "repo").lower()).strip("-")
            or "repo"
        )
        try:
            repo_url = ds.get("connection_url", "")
            branch = ds.get("default_branch")
            creds = ds.get("credentials") or {}

            # Determine auth method: explicit field, or infer from the
            # delivered SSH identity / credentials keys.
            auth_method = creds.get("auth_method")
            if not auth_method:
                if ds.get("ssh_identity") is not None or creds.get("ssh_key"):
                    auth_method = "ssh"
                elif creds.get("token"):
                    auth_method = "token"

            ssh_clone_url: Optional[str] = None
            ssh_outcome: Optional[List[Any]] = None
            if auth_method == "ssh":
                ssh_clone_url, reason = _ssh_clone_target(ds, ssh_identity_status)
                # A key file a pre-agent clone wrote goes only once this
                # repository is proven through its alias (after the loop).
                ssh_outcome = [ds_name, False]
                ssh_outcomes.append(ssh_outcome)
                if ssh_clone_url is None:
                    logger.warning(
                        "Skipping SSH repository datasource %r: %s",
                        ds_name,
                        reason,
                    )
                    continue
                repo_url = ssh_clone_url

            elif (auth_method == "token" or not auth_method) and creds.get("token"):
                parsed = urlparse(repo_url)
                repo_url = parsed._replace(
                    netloc=f"oauth2:{creds['token']}@{parsed.hostname}"
                    + (f":{parsed.port}" if parsed.port else "")
                ).geturl()

            target = workspace_manager.path / "repos" / repo_name
            remote_cwd = f"repos/{repo_name}"
            reused = backend.exists(f"{remote_cwd}/.git")
            if reused:
                # A session workspace may outlive its agent pod (PVC hot tier)
                # or be restored from the thread repository. Re-register the
                # managed checkout instead of attempting a second clone into
                # the existing directory, which git correctly refuses.
                git_mgr = GitManager(
                    target,
                    backend=backend,
                    remote_cwd=remote_cwd,
                )
                if ssh_clone_url is not None:
                    if git_mgr.add_remote("origin", ssh_clone_url):
                        # Proven lazily: ls-remote only if a key file is left.
                        ssh_outcome[1] = git_mgr
                    else:
                        # A pre-agent checkout points at the real host and
                        # its key file; leaving it would fail every fetch.
                        logger.warning(
                            "Could not point reused repos/%s at its SSH identity",
                            repo_name,
                        )
                logger.info(
                    "Reusing repository datasource %r from repos/%s",
                    ds_name,
                    repo_name,
                )
            else:
                git_mgr = GitManager.clone(
                    repo_url,
                    target,
                    backend=backend,
                    remote_cwd=remote_cwd,
                )
                if ssh_outcome is not None and git_mgr:
                    ssh_outcome[1] = True
            if git_mgr:
                branch_ready = True
                if branch and (not reused or ds.get("require_default_branch")):
                    branch_ready = git_mgr.checkout_branch(branch)
                elif branch:
                    # Reused checkout without require_default_branch: the
                    # worker may have moved HEAD (e.g. onto a job branch)
                    # before this re-attach; re-running checkout here silently
                    # reverted that on every resume (job 12a0e92c). Only a
                    # review session pins the branch on reuse — its entire
                    # point is that this exact delivery is checked out
                    # (orchestrator/services/job_delivery.py sets
                    # require_default_branch).
                    logger.debug(
                        "Reused repos/%s keeps its checked-out branch %r "
                        "(pinned default %r not re-applied on re-attach)",
                        repo_name,
                        git_mgr.current_branch(),
                        branch,
                    )
                if ds.get("require_default_branch") and not branch_ready:
                    logger.error(
                        "Repository datasource %r could not check out required "
                        "branch %r; refusing to register the review source",
                        ds_name,
                        branch,
                    )
                    continue
                workspace_manager.source_repos[repo_name] = git_mgr
                try:
                    from shared.runtime.services.forge import (
                        parse_owner_repo,
                        resolve_api_base,
                    )

                    forge = str((ds.get("config") or {}).get("forge") or "").lower()
                    raw_url = ds.get("connection_url", "")
                    owner, repo_slug = parse_owner_repo(raw_url)
                    repo_meta = {
                        "forge": forge,
                        "api_base": resolve_api_base(raw_url, forge),
                        "owner": owner,
                        "repo": repo_slug,
                        "token": (creds or {}).get("token", ""),
                        # The agent payload carries the project link flag as
                        # `project_read_only`; `read_only` is the publisher's
                        # declared flag on public datasources. Either one
                        # forbids writes, and reading only the latter made
                        # every read-only repository record read_only=False.
                        "read_only": bool(
                            ds.get("project_read_only") or ds.get("read_only")
                        ),
                        "default_branch": branch,
                    }
                    # Current orchestrators deliberately omit the raw DB
                    # ``id`` and send the resolved, server-owned identity as
                    # ``datasource_id``.  The ``id`` fallback is retained only
                    # for older in-process/internal callers that passed a
                    # resolved row directly to this shared clone helper.
                    datasource_id = str(
                        ds.get("datasource_id") or ds.get("id") or ""
                    ).strip()
                    if datasource_id:
                        repo_meta["datasource_id"] = datasource_id
                    workspace_manager.source_repo_meta[repo_name] = repo_meta
                except Exception as e:
                    # A metadata failure must not fail the clone; the repo is
                    # still usable through the shell and the read-only git tools.
                    logger.warning(
                        "Could not record forge metadata for repos/%s: %s",
                        repo_name,
                        e,
                    )
                logger.info(
                    "Cloned repository datasource %r into repos/%s",
                    ds_name,
                    repo_name,
                )
            else:
                logger.warning(
                    "Failed to clone repository datasource %r (target repos/%s)",
                    ds_name,
                    repo_name,
                )
        except Exception as e:
            logger.warning(
                "Failed to clone repository datasource %s: %s",
                ds.get("name", "unnamed"),
                e,
            )

    if ssh_outcomes:
        _retire_legacy_ssh_key_files(
            backend,
            [(name, outcome) for name, outcome in ssh_outcomes],
            sweep=sweep_legacy_keys,
        )


# ---------------------------------------------------------------------------
# Workspace facts (README.md)
# ---------------------------------------------------------------------------


def _declared_ro_note(ds: Dict[str, Any]) -> str:
    """Advisory suffix for publisher-declared read-only datasources.

    Declarative only (knowledge-base/knowledge/features/public_datasources.md): credentials are
    the enforcement boundary; this just tells the agent the intent. Distinct
    from ``project_read_only`` (per-link connector mode), which switches the
    tool surface.
    """
    return " (declared read-only — treat as no-write)" if ds.get("read_only") else ""


WORKSPACE_FACTS_START = "<!-- srw:workspace-facts:start -->"
WORKSPACE_FACTS_END = "<!-- srw:workspace-facts:end -->"
# Gitea auto-inits per-job repos with a stub README of the form
# "# job-xxxxxxxx\n\nWorkspace for job-xxxxxxxx"; anything else without our
# markers is a real README (a human's, on a shared project repo) that is only
# ever appended to.
# Gitea auto-init READMEs for per-job repos: the legacy form
# "# job-xxxxxxxx\n\nWorkspace for job-xxxxxxxx" and the current
# `_repository_intent_description` form "SRW managed repository; creation-intent=<uuid>"
# (orchestrator/services/gitea.py). Both are stubs to replace, never a human README.
_GITEA_STUB_README_MARKERS = (
    "Workspace for job-",
    "SRW managed repository; creation-intent=",
)
MATERIALS_LIST_CAP = 30
# Bounds on the documents/ walk so a pathological upload cannot turn the
# facts block into thousands of SFTP round trips.
_MATERIALS_MAX_DEPTH = 8
_MATERIALS_MAX_ENTRIES = 2000


def _repo_meta(workspace_manager: Any, clone_name: str) -> Dict[str, Any]:
    """Forge metadata recorded by clone_repository_datasources(), or {}."""
    meta = getattr(workspace_manager, "source_repo_meta", None)
    if not isinstance(meta, dict):
        return {}
    entry = meta.get(clone_name)
    return entry if isinstance(entry, dict) else {}


def _ssh_identity_unloaded(
    identity: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]]
) -> bool:
    """True when the materializer ran and this identity's key is not held."""

    if not isinstance(ssh_identity_status, dict):
        return False
    authority = str(identity.get("authority_id") or "")
    return ssh_identity_status.get(authority) != "ready"


_SSH_KEY_NOT_LOADED = "its key could not be loaded into this workspace's ssh-agent"


def _ssh_identity_note(
    ds: Dict[str, Any], ssh_identity_status: Optional[Dict[str, str]] = None
) -> str:
    """Suffix for an SSH-key repository line: how git reaches it."""

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict):
        return ""
    if identity.get("unavailable"):
        return f"; not available: {_ssh_unavailable_reason(identity)}"
    if _ssh_identity_unloaded(identity, ssh_identity_status):
        return f"; not available: {_SSH_KEY_NOT_LOADED}"
    return (
        f"; git uses SSH alias `{identity.get('alias')}`, whose key an "
        "ssh-agent holds (never on disk)"
    )


def _ssh_key_host(ds: Dict[str, Any]) -> Optional[str]:
    """The host a delivered ``ssh_key`` connector declares, if any."""

    identity = ds.get("ssh_identity")
    if (
        ds.get("type") != "ssh_key"
        or not isinstance(identity, dict)
        or not identity.get("alias")
        or identity.get("unavailable")
        or not identity.get("host")
    ):
        return None
    return str(identity["host"]).lower()


def _ssh_key_usage(
    ds: Dict[str, Any],
    ssh_identity_status: Optional[Dict[str, str]] = None,
    shared_hosts: frozenset[str] = frozenset(),
) -> str:
    """How the agent uses an ``ssh_key`` connector held by an ssh-agent.

    ``shared_hosts`` are hosts more than one delivered ``ssh_key`` connector
    declares. OpenSSH takes the first matching ``Host`` block, so a plain
    ``ssh <host>`` offers only one of their keys; each is named by its alias.
    """

    identity = ds.get("ssh_identity")
    if not isinstance(identity, dict) or not identity.get("alias"):
        return "not available: its key was not delivered to this workspace."
    if identity.get("unavailable"):
        return f"not available: {_ssh_unavailable_reason(identity)}."
    if _ssh_identity_unloaded(identity, ssh_identity_status):
        return f"not available: {_SSH_KEY_NOT_LOADED}."
    fingerprint = (
        f" Key `{identity.get('fingerprint')}`." if identity.get("fingerprint") else ""
    )
    held = "The key is held by an ssh-agent and never written to disk."
    host = identity.get("host")
    if host and str(host).lower() in shared_hosts:
        return (
            f"use the alias: `ssh {identity['alias']}` reaches `{host}` with this "
            f"key. Another ssh_key connector here also declares `{host}`, so a "
            f"plain `ssh {host}` may offer the other key. {held}{fingerprint}"
        )
    if host:
        user = identity.get("user")
        port = identity.get("port")
        target = f"{user}@{host}" if user else str(host)
        port_clause = f" (port {port})" if port and port != 22 else ""
        return (
            f"`ssh {target}`{port_clause} uses it, as does the alias "
            f"`{identity['alias']}`. {held}{fingerprint}"
        )
    slug = str(identity["alias"]).removeprefix("srw-repo-")
    return (
        "use it with `ssh -o IdentityAgent=~/.ssh/srw-managed/sockets/"
        f"{slug}.sock <user>@<host>`. {held}{fingerprint}"
    )


def _render_connector_lines(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> List[str]:
    """Per-type connector lines with each connector's named access method."""
    lines: List[str] = []

    # Group by category for readable output
    repos = [ds for ds in ds_configs if ds.get("type") == "repository"]
    # A project's own KB datasource is a management surface, not an external
    # source: it is bound as the writable native KB. Listing it here would
    # advertise the agent's own knowledge base as "read-only".
    knowledge_bases = [
        ds
        for ds in ds_configs
        if ds.get("type") == "kb" and not native_kb_project_id(ds)
    ]
    databases = [
        ds for ds in ds_configs if ds.get("type") in ("postgresql", "neo4j", "mongodb")
    ]
    mcps = [ds for ds in ds_configs if ds.get("type") == "mcp"]
    creds = [ds for ds in ds_configs if ds.get("type") in CREDENTIAL_FILE_TYPES]
    others = [
        ds
        for ds in ds_configs
        if ds.get("type")
        not in (
            "repository",
            "kb",
            "postgresql",
            "neo4j",
            "mongodb",
            "mcp",
            "kubeconfig",
            "ssh_key",
            "generic_file",
        )
    ]

    if repos:
        lines.append("### Repositories")
        # Same name resolution as clone_repository_datasources() — the list
        # must point at the directories the clones actually land in.
        for ds, clone_name in zip(repos, resolve_repo_clone_names(repos)):
            meta = _repo_meta(workspace_manager, clone_name)
            default_branch = str(
                meta.get("default_branch") or ds.get("default_branch") or ""
            ).strip()
            branch_clause = (
                f"; base branch `{default_branch}`" if default_branch else ""
            )
            read_only = bool(
                meta.get("read_only")
                if "read_only" in meta
                else (ds.get("project_read_only") or ds.get("read_only"))
            )
            access = (
                "read-only — only repo_pull/repo_pr_status"
                if read_only
                else "writable — pull requests opened with repo_open_pr are "
                "recorded for this job"
            )
            lines.append(
                f"- **{ds.get('name')}** — repository cloned at "
                f'`./repos/{clone_name}/` (use `repo="{clone_name}"` with the '
                f"repo_* tools){branch_clause}; {access}{_declared_ro_note(ds)}"
                + _ssh_identity_note(ds, ssh_identity_status)
            )
        lines.append("")

    if knowledge_bases:
        lines.append("### OKF Knowledge Bases")
        for ds in knowledge_bases:
            name = ds.get("name", "Unnamed")
            root = str((ds.get("config") or {}).get("root_path") or "")
            root_hint = f", root `{root}`" if root else ""
            lines.append(
                f"- **{name}** (centrally indexed, read-only{root_hint}) — "
                "use `kb_search`, `kb_grep`, `kb_list`, and `kb_read`"
            )
        lines.append("")

    if databases:
        lines.append("### Databases")
        for ds in databases:
            ds_type = ds.get("type", "unknown")
            name = ds.get("name", "Unnamed")
            is_ro = ds.get("project_read_only", False)

            if is_ro:
                lines.append(f"- **{name}** ({ds_type}, read-only) — query tools")
            else:
                lines.append(
                    f"- **{name}** ({ds_type}, read-write) — query + write tools"
                    + _declared_ro_note(ds)
                )
        lines.append("")

    if mcps:
        lines.append("### MCP Servers")
        for ds in mcps:
            name = ds.get("name", "Unnamed")
            transport = str(
                (ds.get("credentials") or {}).get("transport") or "http"
            ).lower()
            status = ds.get("_mcp_status") or "not connected yet"
            tools = ds.get("_mcp_tools") or []
            if status == "connected":
                shown = ", ".join(f"`{tool_name}`" for tool_name in tools[:40])
                if not shown:
                    shown = "_no tools advertised_"
                more = f" (+{len(tools) - 40} more)" if len(tools) > 40 else ""
                lines.append(
                    f"- **{name}** (mcp, {len(tools)} tools) — "
                    f"{transport}; {shown}{more}"
                )
            else:
                lines.append(f"- **{name}** (mcp, {transport}) — {status}")
        lines.append("")

    if creds:
        lines.append("### Credential Files")
        declared = [_ssh_key_host(ds) for ds in creds]
        shared_hosts = frozenset(
            host for host in declared if host and declared.count(host) > 1
        )
        for ds in creds:
            ds_type = ds.get("type", "unknown")
            name = ds.get("name", "Unnamed")
            slug = _ds_slug_hyphen(name)
            ds_creds = ds.get("credentials") or {}
            files = ds_creds.get("files") or []
            if ds_type == "kubeconfig":
                lines.append(
                    f"- **{name}** (kubeconfig) — merged into `~/.kube/config`; "
                    f"contexts prefixed `{slug}-*`. Try `kubectl config get-contexts`."
                )
            elif ds_type == "ssh_key":
                lines.append(
                    f"- **{name}** (ssh_key) — "
                    + _ssh_key_usage(ds, ssh_identity_status, shared_hosts)
                )
            else:  # generic_file
                paths = (
                    ", ".join(f"`{f.get('target_path')}`" for f in files) or "<none>"
                )
                lines.append(f"- **{name}** (file) — {paths}")
        lines.append("")

    if others:
        lines.append("### Other")
        for ds in others:
            ds_type = ds.get("type", "unknown")
            name = ds.get("name", "Unnamed")
            is_ro = ds.get("project_read_only", False)

            if ds_type in ENV_CONNECTOR_TYPES:
                cli = ds.get("cli_hint") or "CLI via env vars"
                lines.append(f"- **{name}** ({ds_type}) — {cli}{_declared_ro_note(ds)}")
                variables = (ds.get("credentials") or {}).get("env_vars", {})
                if variables:
                    lines.append(
                        "  Environment: " + ", ".join(f"`{key}`" for key in variables)
                    )
                    lines.append(
                        "  Read values with os.environ in workspace scripts. For login forms, "
                        'use browser_type(ref=..., env_var="VARIABLE_NAME"). '
                        "Avoid printing credentials or writing literal values into scripts."
                    )
            elif ds_type == "webdav":
                access = "read-only tools" if is_ro else "read-write tools"
                lines.append(f"- **{name}** (webdav, {access}){_declared_ro_note(ds)}")
            elif ds_type == "email":
                config = ds.get("config") or {}
                account = (
                    (ds.get("credentials") or {}).get("username")
                    or config.get("from_address")
                    or "unknown account"
                )
                folders = config.get("folders") or []
                scope = (
                    ", ".join(f"`{f}`" for f in folders)
                    if folders
                    else "entire mailbox"
                )
                lines.append(
                    f"- **{name}** (email, {account}, tier "
                    f"`{email_effective_access(ds)}`) — folders: {scope}"
                    f"{_declared_ro_note(ds)}"
                )
            else:
                lines.append(f"- **{name}** ({ds_type}){_declared_ro_note(ds)}")
        lines.append("")

    if not lines:
        # A live remove-all still needs the section rewritten — an agent
        # re-reading the file must not act on connection names that no
        # longer resolve.
        lines.append("_No connectors attached._")
        lines.append("")

    return lines


def _list_materials(workspace_manager: Any) -> List[str]:
    """Workspace-relative paths of the files under ``documents/``, sorted.

    Walks through the workspace backend (the workspace is remote), bounded in
    depth and entry count. Dotfiles are skipped. Any listing failure yields
    an empty list — the facts block is advisory and never fatal.
    """
    list_files = getattr(workspace_manager, "list_files", None)
    if not callable(list_files):
        return []
    files: List[str] = []
    pending: List[Tuple[str, int]] = [("documents", 0)]
    seen = 0
    try:
        while pending and seen < _MATERIALS_MAX_ENTRIES:
            directory, depth = pending.pop()
            entries = list_files(directory)
            if not isinstance(entries, (list, tuple)):
                return []
            for entry in entries:
                seen += 1
                if seen > _MATERIALS_MAX_ENTRIES:
                    break
                rel = str(entry)
                base = rel.rstrip("/").rsplit("/", 1)[-1]
                if not base or base.startswith("."):
                    continue
                if rel.endswith("/"):
                    if depth < _MATERIALS_MAX_DEPTH:
                        pending.append((rel.rstrip("/"), depth + 1))
                    continue
                files.append(rel)
    except Exception as e:
        logger.warning("Could not list documents/ for the workspace facts: %s", e)
        return []
    return sorted(files)


def _has_dir(workspace_manager: Any, relative_path: str) -> bool:
    exists = getattr(workspace_manager, "exists", None)
    if not callable(exists):
        return False
    try:
        return bool(exists(relative_path))
    except Exception:
        return False


def render_workspace_facts(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    project_name: Optional[str] = None,
    expert: Optional[str] = None,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> str:
    """Render the marker-delimited workspace-facts block for README.md.

    FACTS ONLY: what this workspace is, which connectors are attached and
    where they live, what input materials were provided, and the layout.
    Never the job id, description, kickoff, or todos — those stay in the
    virtual ``task_brief.md``. Subjobs share their parent's workspace and
    connector set, so the block is safe on shared workspaces.
    """
    lines: List[str] = [WORKSPACE_FACTS_START]

    facts: List[str] = []
    if isinstance(project_name, str) and project_name.strip():
        facts.append(f"- **Project**: {project_name.strip()}")
    if isinstance(expert, str) and expert.strip():
        facts.append(f"- **Expert**: {expert.strip()}")
    if facts:
        lines += ["## Workspace", "", *facts, ""]

    lines += ["## Connectors", ""]
    lines += _render_connector_lines(
        list(ds_configs or []), workspace_manager, ssh_identity_status
    )

    lines += ["## Materials", ""]
    materials = _list_materials(workspace_manager)
    if materials:
        lines += [f"- `{path}`" for path in materials[:MATERIALS_LIST_CAP]]
        if len(materials) > MATERIALS_LIST_CAP:
            lines.append(f"… and {len(materials) - MATERIALS_LIST_CAP} more")
    else:
        lines.append("_No input documents._")
    lines.append("")

    lines += ["## Layout", "", "- `output/` — deliverables"]
    if _has_dir(workspace_manager, "notes"):
        lines.append("- `notes/` — working notes")
    lines += [
        "- `tools/` — tool documentation (virtual)",
        "- `skills/` — skills available via use_skill",
        "- `archive/` — completed phases",
        WORKSPACE_FACTS_END,
    ]
    return "\n".join(lines)


def merge_workspace_facts(existing: Optional[str], block: str) -> str:
    """Merge the facts block into an existing README, or create one.

    1. README absent/empty → ``# Workspace`` + block.
    2. README with markers → replace exactly the marked span; everything
       else stays byte-identical.
    3. README without markers that is the Gitea stub → replaced as in (1).
    4. Any other README (a human's, on a shared project repo) → the block
       is appended; the existing text is never modified.
    """
    fresh = f"# Workspace\n\n{block}\n"
    if existing is None or not existing.strip():
        return fresh
    start = existing.find(WORKSPACE_FACTS_START)
    end = existing.find(WORKSPACE_FACTS_END)
    if start != -1 and end != -1 and end > start:
        return existing[:start] + block + existing[end + len(WORKSPACE_FACTS_END) :]
    if start != -1:
        # Start marker without an end marker: the block ran to EOF.
        return existing[:start] + block + "\n"
    stripped = existing.strip()
    if (
        stripped.startswith("# job-")
        and len(stripped) < 300
        and any(marker in stripped for marker in _GITEA_STUB_README_MARKERS)
    ):
        return fresh
    return existing.rstrip("\n") + "\n\n" + block + "\n"


def inject_workspace_facts(
    ds_configs: List[Dict[str, Any]],
    workspace_manager: Any,
    *,
    project_name: Optional[str] = None,
    expert: Optional[str] = None,
    ssh_identity_status: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Write the workspace-facts block into the workspace's README.md.

    One on-disk file orients both the agent and a human opening the job
    repo: the system prompts point the agent at README.md for connector
    names and clone paths. Regenerated at every agent init and on every
    live attach/detach, so the block always reflects the current connector
    set (including the explicit "no connectors" state after a remove-all).

    ``ssh_identity_status`` is the SSH identity materializer's
    ``{authority_id: status}``: an SSH connector whose key did not load is
    listed as not available rather than advertised.

    Non-fatal: a failure logs a warning. Returns the README content that was
    written, or None when nothing was written.
    """
    try:
        try:
            existing: Optional[str] = workspace_manager.read_file("README.md")
        except (FileNotFoundError, ValueError, OSError):
            existing = None
        if not isinstance(existing, str):
            existing = None
        block = render_workspace_facts(
            ds_configs,
            workspace_manager,
            project_name=project_name,
            expert=expert,
            ssh_identity_status=ssh_identity_status,
        )
        content = merge_workspace_facts(existing, block)
        workspace_manager.write_file("README.md", content)
        logger.info(
            "Wrote workspace facts (%d connectors) into README.md",
            len(ds_configs or []),
        )
        return content
    except Exception as e:
        logger.warning("Failed to write workspace facts into README.md: %s", e)
        return None


# NOTE: the former _format_rw_cli_entry (PGSERVICE/cypher-shell/mongosh usage
# lines for read-write connectors) was removed with the CLI-mode retirement —
# it advertised commands that cannot work on remote workspace backends. A
# future real CLI-forwarding feature reintroduces it properly
# (knowledge-base/knowledge/issues/datasource_cli_mode_dead_on_remote.md, direction 2).
