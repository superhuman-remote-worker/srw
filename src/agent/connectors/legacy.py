"""Today's agent payload, read as binding descriptors.

The ``datasources`` payload is still an untyped list of entries, and agent
images roll independently of the orchestrator, so D1 keeps it byte-identical
(a ``binding`` key on the wire waits for D5). :func:`binding_from_legacy_entry`
is the ONE agent-side place that maps a stored ``type`` to delivery forms:
everything after it routes by form.

Building a descriptor never raises and never validates. Validation stays
where it was, at delivery, with the same messages (an environment name is
checked when it is installed, not when a README is rendered).

Three things stay on the wire entry rather than in the descriptor, because
the components that read them take today's entry:

* an SSH key travels apart from its entry, in ``workspace_ssh_identities``
  (slice C1). The ``ssh_identity`` materializer loads those payloads, so no
  ``ssh_identity`` entry is built here, and an entry's non-secret
  ``ssh_identity`` reference (alias, authority, clone URL, or an
  ``unavailable`` reason) is read from the entry;
* the clone keeps C1's rules on the entry (``agent.connectors.checkout``);
* ``MCPManager`` parses and annotates MCP entries, and the knowledge binding
  builder reads KB entries.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from agent.connectors.base import Delivery
from agent.connectors.slots import connection_slot
from shared.connectors.binding import BindingDescriptor, BindingEntry
from shared.connectors.builtin import (
    GIT_SWAP_SPEC,
    driver_spec_for_row,
    git_swap_entry,
    spec_for_type,
)
from shared.connectors.contract import DriverSpec, effective_access
from shared.native_kb import native_kb_project_id


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _credentials(entry: Mapping[str, Any]) -> Mapping[str, Any]:
    return _mapping(entry.get("credentials"))


def _optional_str(value: Any) -> str | None:
    return str(value) if value else None


def env_vars_unreadable(entry: Mapping[str, Any]) -> bool:
    """Whether the entry carries ``env_vars`` that are not a name/value object.

    Such an entry yields no ``env_file`` entries; the environment
    materializer refuses it with the message it always gave.
    """
    credentials = _credentials(entry)
    return "env_vars" in credentials and not isinstance(
        credentials["env_vars"], Mapping
    )


def _env_entries(entry: Mapping[str, Any], spec: DriverSpec) -> list[BindingEntry]:
    variables = _credentials(entry).get("env_vars")
    if not isinstance(variables, Mapping):
        return []
    return [
        BindingEntry(
            recipient="workspace",
            form="env_file",
            value={"name": name, "value": value},
            collision="error",
            refresh="on_backend_swap",
        )
        for name, value in variables.items()
    ]


def _file_mode(raw: Any) -> int | None:
    try:
        return int(str(raw or "0600"), 8)
    except ValueError:
        return None


def unreadable_file_modes(entry: Mapping[str, Any]) -> list[Any]:
    """Per ``credential_file`` entry, in order, the raw mode it could not read.

    ``None`` where the mode was read (or absent). A file entry without a
    readable mode gets 0600; the materializer warns about these.
    """
    return [
        None if _file_mode(item.get("mode")) is not None else item.get("mode")
        for item in _credentials(entry).get("files") or []
        if isinstance(item, Mapping)
    ]


def _file_entries(entry: Mapping[str, Any], spec: DriverSpec) -> list[BindingEntry]:
    slot = next((slot for slot in spec.credential_slots if slot.name == "files"), None)
    kubeconfig = slot is not None and slot.kind == "kubeconfig"
    entries: list[BindingEntry] = []
    for item in _credentials(entry).get("files") or []:
        if not isinstance(item, Mapping):
            continue
        value: dict[str, Any] = {
            "path": str(item.get("target_path") or ""),
            "content": str(item.get("contents") or ""),
            "env_var": _optional_str(item.get("env_var")),
            # Several kubeconfigs merge into ~/.kube/config, each with its
            # names prefixed by the connector's slug.
            "transform": "kubeconfig_prefix" if kubeconfig else None,
            "merge_group": "kubeconfig" if kubeconfig else None,
        }
        mode = _file_mode(item.get("mode"))
        if mode is not None:
            value["mode"] = mode
        entries.append(
            BindingEntry(
                recipient="workspace",
                form="credential_file",
                value=value,
                collision="skip_existing",
                refresh="on_backend_swap",
                retire="remove",
            )
        )
    return entries


def checkout_auth(entry: Mapping[str, Any]) -> str:
    """How the clone authenticates: ``swap``, ``ssh_agent``, ``token_in_url``
    or ``none``.

    A ``git_swap`` block means the git swap driver (C3): the workspace's git
    reaches it with a lease, or the installation refused the repository and
    the block says why. Otherwise an explicit ``auth_method`` wins; a
    delivered SSH identity or a stored key means SSH, and a token means a
    token in the clone URL.
    """
    if isinstance(entry.get("git_swap"), Mapping):
        return "swap"
    credentials = _credentials(entry)
    method = credentials.get("auth_method")
    if not method:
        if entry.get("ssh_identity") is not None or credentials.get("ssh_key"):
            method = "ssh"
        elif credentials.get("token"):
            method = "token"
    if method == "ssh":
        return "ssh_agent"
    if method == "token" and credentials.get("token"):
        return "token_in_url"
    return "none"


def _checkout_entries(entry: Mapping[str, Any], spec: DriverSpec) -> list[BindingEntry]:
    # No ``secret``: the clone reads the token from the entry, and a second
    # in-memory copy would have no reader. The rest is what a D5 wire
    # binding must carry for the clone and the repo tools.
    return [
        BindingEntry(
            recipient="workspace",
            form="checkout",
            value={
                "url": str(entry.get("connection_url") or ""),
                "name_hint": str(entry.get("name") or "repo"),
                "auth": checkout_auth(entry),
                "default_branch": _optional_str(entry.get("default_branch")),
                "require_default_branch": entry.get("require_default_branch") is True,
                "forge": _optional_str(_mapping(entry.get("config")).get("forge")),
                "datasource_id": _optional_str(
                    entry.get("datasource_id") or entry.get("id")
                ),
                # The payload carries the project link flag as
                # project_read_only; read_only is the publisher's declaration.
                "read_only": bool(
                    entry.get("project_read_only") or entry.get("read_only")
                ),
            },
            collision="suffix",
            # A detached repository keeps its clone; only its registration goes.
            retire="drop_registration",
        )
    ]


def _connection_entries(
    entry: Mapping[str, Any], spec: DriverSpec
) -> list[BindingEntry]:
    url = entry.get("connection_url")
    return [
        BindingEntry(
            recipient="harness",
            form="managed_connection",
            value={
                # The harness slot the built-in tools read.
                "kind": connection_slot(spec) or "",
                "url": str(url) if url is not None else None,
                "credentials": dict(_credentials(entry)),
                "config": dict(_mapping(entry.get("config"))),
                "read_only": bool(entry.get("project_read_only", False)),
            },
            collision="last_wins",
            retire="close",
        )
    ]


def _mcp_entries(entry: Mapping[str, Any], spec: DriverSpec) -> list[BindingEntry]:
    credentials = _credentials(entry)
    url = entry.get("connection_url")
    value: dict[str, Any] = {
        "transport": str(credentials.get("transport") or "http").lower(),
        "url": str(url) if url else None,
    }
    if isinstance(credentials.get("command"), str):
        value["command"] = credentials["command"]
    args = credentials.get("args")
    if isinstance(args, list) and all(isinstance(arg, str) for arg in args):
        value["args"] = list(args)
    return [
        BindingEntry(
            recipient="harness",
            form="mcp_client",
            value=value,
            collision="last_wins",
            retire="close",
        )
    ]


def _knowledge_entries(
    entry: Mapping[str, Any], spec: DriverSpec
) -> list[BindingEntry]:
    return [
        BindingEntry(
            recipient="harness",
            form="knowledge_index",
            value={
                "datasource_id": str(
                    entry.get("datasource_id") or entry.get("id") or ""
                ),
                "config": dict(_mapping(entry.get("config"))),
                "native_project_id": native_kb_project_id(dict(entry)),
            },
        )
    ]


def _lease_entries(entry: Mapping[str, Any], spec: DriverSpec) -> list[BindingEntry]:
    # The orchestrator puts the lease where an inline driver's secret would
    # be (``credentials``), so every path that strips credentials from a
    # payload (checkpoints, audit) strips the token too. An entry it could
    # not lease carries none and delivers nothing.
    lease = _mapping(_credentials(entry).get("lease"))
    value = {
        key: lease.get(source)
        for key, source in (
            ("lease_id", "id"),
            ("connector_id", "connector_id"),
            ("token", "token"),
        )
    }
    if not all(isinstance(item, str) and item for item in value.values()):
        return []
    return [
        BindingEntry(
            recipient="workspace",
            form="lease_token",
            value=value,
            collision="last_wins",
            refresh="on_backend_swap",
            retire="remove",
        )
    ]


_ENTRY_BUILDERS = {
    "env_file": _env_entries,
    "lease_token": _lease_entries,
    "credential_file": _file_entries,
    "checkout": _checkout_entries,
    "managed_connection": _connection_entries,
    "mcp_client": _mcp_entries,
    "knowledge_index": _knowledge_entries,
}


def routing_spec(entry: Mapping[str, Any]) -> DriverSpec | None:
    """The spec whose delivery forms an entry routes by: its type's driver,
    or the git swap driver for a repository bound through it (which also
    delivers a lease token, before the checkout)."""
    if git_swap_entry(entry):
        return GIT_SWAP_SPEC
    return spec_for_type(entry.get("type"))


def binding_from_legacy_entry(entry: Mapping[str, Any]) -> BindingDescriptor | None:
    """The binding descriptor one wire entry stands for.

    ``None`` when no installed driver serves the entry's type. Never raises.
    """
    spec = routing_spec(entry)
    if spec is None:
        return None
    entries = tuple(
        item
        for form in spec.delivery_forms
        for item in _ENTRY_BUILDERS.get(form, lambda _entry, _spec: [])(entry, spec)
    )
    return BindingDescriptor(
        # The row's own driver: a remote MCP server's is srw.mcp-remote/v1.
        driver=(driver_spec_for_row(entry) or spec).name,
        name=str(entry.get("name") or "unnamed"),
        entries=entries,
        access=effective_access(entry, spec),
        connector_id=_optional_str(entry.get("datasource_id")),
        description=_optional_str(entry.get("description")),
        cli_hint=_optional_str(entry.get("cli_hint")),
    )


def deliveries_from_payload(entries: Iterable[Any] | None) -> list[Delivery]:
    """One :class:`Delivery` per dict entry of a payload, in payload order.

    Entries keep their identity (see :class:`Delivery`); an entry no driver
    serves keeps a ``None`` descriptor so the README can still list it.
    """
    deliveries: list[Delivery] = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        deliveries.append(
            Delivery(
                index=len(deliveries),
                entry=entry,
                binding=binding_from_legacy_entry(entry),
                spec=routing_spec(entry),
            )
        )
    return deliveries
