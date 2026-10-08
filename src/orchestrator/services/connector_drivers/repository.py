"""``srw.repository/v1``: a Git repository the agent checks out.

A token repository on HTTPS is bound through the git swap driver
(``srw.git-swap/v1``, C3) where it is installed: the workspace holds a lease
token, never the forge token. Otherwise the installation's fallback applies
(``connectors.drivers.gitSwap.fallback``): the token in the clone URL, as
before, or no delivery. An SSH-key repository's key goes into the workspace's
ssh-agent and the clone runs through its ``srw-repo-<32hex>`` alias (C1): the
payload entry keeps only the non-secret ``ssh_identity``. ``config`` carries
the forge (inferred for github.com and gitlab.com, declared otherwise) and,
for an SSH-key repository, pinned ``known_hosts``.

Test asks the forge API what a token may do, or reaches the SSH endpoint and
reports the host key the connector form offers to pin.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

from fastapi import HTTPException

from orchestrator.services.connector_drivers import knowledge_note
from orchestrator.services.connector_drivers.git_swap import (
    token_auth,
    route_token_repository,
)
from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    NormalizedConnector,
    SecretLeaf,
    ValidationContext,
    auth_method_config,
    payload_entry,
    top_level_leaves,
)
from orchestrator.services.connector_drivers.workspace_ssh import WorkspaceSshDriver
from orchestrator.services.datasource_config import (
    normalize_repository_config,
    stored_json_object,
)
from orchestrator.services.workspace_ssh_connector import (
    probe_workspace_ssh_connector,
    repository_uses_ssh_key,
)
from shared.connectors.builtin import REPOSITORY_SPEC


_URL_REQUIRED = "Repository connectors require a repository URL"


class RepositoryDriver(WorkspaceSshDriver):
    def __init__(self) -> None:
        super().__init__(REPOSITORY_SPEC)

    def holds_ssh_key(self, row: Mapping[str, Any]) -> bool:
        return repository_uses_ssh_key(stored_json_object(row.get("credentials")))

    def knowledge_note(self, row: Mapping[str, Any]) -> str:
        return knowledge_note.repository_note(row)

    def retrieval_messages(self, row: Mapping[str, Any]) -> list[str]:
        return knowledge_note.repository_phrases(row)

    def credential_config(self, credentials: Mapping[str, Any]) -> dict[str, Any]:
        """The auth method; the token and the key stay secret."""
        return auth_method_config(credentials)

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        return top_level_leaves(credentials, ("token", "ssh_key"))

    def unread_pins_dropped(
        self, config: dict[str, Any], credentials: Mapping[str, Any]
    ) -> dict[str, Any]:
        # A switch to token auth leaves a stored pin unread, not invalid.
        if repository_uses_ssh_key(credentials):
            return config
        return {key: value for key, value in config.items() if key != "known_hosts"}

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            config = normalize_repository_config(draft.config, draft.connection_url)
            credentials = self.stored_credentials(draft, existing)
            config = self.validate_endpoint(
                connection_url=draft.connection_url,
                config=config,
                credentials=credentials,
            )
            # The clone and Test both need it; a declared forge used to make a
            # URL-less connector valid.
            if not (draft.connection_url or "").strip():
                raise HTTPException(status_code=400, detail=_URL_REQUIRED)
            return NormalizedConnector(draft.connection_url, config, credentials)

        credentials = self.stored_credentials(draft, existing)
        config = draft.config
        if config is not None:
            config = normalize_repository_config(
                config, draft.connection_url or existing.get("connection_url")
            )
        config = self.validate_effective_endpoint(
            draft, existing, config=config, credentials=credentials
        )
        # An explicit null leaves the stored URL alone; a blank one would
        # clear it.
        if draft.connection_url is not None and not draft.connection_url.strip():
            raise HTTPException(status_code=400, detail=_URL_REQUIRED)
        return NormalizedConnector(draft.connection_url, config, credentials)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        result = await probe_repository(dict(row), row["connection_url"], credentials)
        if not token_auth(row, credentials):
            return result
        # Where the git swap driver is installed, Test says how the token is
        # delivered now (through the driver, or the fallback and why) and
        # probes the upstream's TLS without a credential (C3).
        from orchestrator.services import connector_git_swap_delivery as swaps

        report = await swaps.delivery_report(row)
        if report is None:
            return result
        result = dict(result)
        result["message"] = f"{result.get('message') or ''}; {swaps.describe(report)}"
        result["details"] = {**(result.get("details") or {}), "delivery": report}
        return result

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        # An SSH key never rides ``datasources``: that list becomes job
        # metadata and graph state. An SSH-key repository's entry keeps the
        # non-secret alias the key is reached through instead, and a token
        # repository's clone never reads a key, so a stray one is dropped too.
        ssh_identity = self.ssh_identity_descriptor(
            row, default_known_hosts=ctx.default_known_hosts
        )
        if isinstance(credentials, Mapping):
            credentials = {
                key: value for key, value in credentials.items() if key != "ssh_key"
            }
        fields: dict[str, Any] = {}
        # Repository identity is server-owned runtime authority.  Keep the
        # raw database ``id`` out of the payload, but carry its exact value
        # under the dedicated internal key consumed by the clone/tool
        # binding.  ``resolved_ds`` comes from the authorization query;
        # callers and models never select this field.
        datasource_id = row.get("id")
        if datasource_id is not None:
            fields["datasource_id"] = str(datasource_id)
        # The clone reads config["forge"] to resolve the forge API base;
        # without it every repository records forge="" and repo_open_pr
        # can never be used. _datasource_row_to_dict already parsed the
        # JSONB, so this is a real dict. No secrets live in config —
        # credentials travel in `creds`.
        fields["config"] = row.get("config") or {}
        entry = payload_entry(
            row,
            credentials=credentials,
            read_only=row.get("project_read_only", False),
            fields=fields,
        )
        if row.get("require_default_branch") is True:
            entry["require_default_branch"] = True
        if ssh_identity is not None:
            entry["ssh_identity"] = ssh_identity
        # A token repository goes through the git swap driver where it is
        # installed and serves the URL; otherwise the installation's fallback.
        route_token_repository(
            entry, git_swap=ctx.git_swap, fallback=ctx.git_swap_fallback
        )
        return entry


async def probe_repository(
    ds: dict[str, Any], url: str | None, creds: dict[str, Any]
) -> dict[str, Any]:
    """Probe a repository connector without exposing its credential.

    Reports the principal the agent will act as, its permission on the
    repository, the token class (GitHub only), and the repository's default
    branch, with warnings for the two configurations that silently defeat
    the guardrails: an administrator token (bypasses branch rules) and a
    connector that is not read-only but cannot push. SSH-key connectors have
    no API to ask: Test reaches their SSH endpoint and reports its host key
    (``workspace_ssh_connector.probe_workspace_ssh_connector``); the clone at
    job start proves the key.
    """
    from shared.runtime.services.forge import (  # noqa: PLC0415
        ForgeError,
        ForgeRepo,
        parse_owner_repo,
        probe_repository_access,
        resolve_api_base,
    )

    token = str(creds.get("token") or "")
    auth_method = str(creds.get("auth_method") or "").lower()
    if not auth_method:
        auth_method = "ssh" if creds.get("ssh_key") else ("token" if token else "")
    if auth_method == "ssh":
        # No forge API takes a deploy key; reach the SSH endpoint and report
        # the host key the connector form offers to pin.
        probed = await probe_workspace_ssh_connector({**ds, "credentials": creds})
        if probed is not None:
            return probed
    if auth_method != "token" or not token:
        return {
            "status": "ok",
            "message": (
                "No API probe for SSH-key repository connectors; "
                "the clone at job start is the test"
            ),
        }

    config = stored_json_object(ds.get("config"))
    try:
        forge = normalize_repository_config(config, url)["forge"]
        owner, repo = parse_owner_repo(url or "")
        target = ForgeRepo(
            forge=forge,
            api_base=resolve_api_base(url or "", forge),
            owner=owner,
            repo=repo,
            token=token,
        )
    except HTTPException as exc:
        return {"status": "error", "message": str(exc.detail)}
    except ForgeError as exc:
        return {"status": "error", "message": str(exc)}

    try:
        facts = await asyncio.wait_for(probe_repository_access(target), timeout=15)
    except asyncio.TimeoutError:
        return {"status": "error", "message": "Repository probe timed out after 15s"}
    except ForgeError as exc:
        return {"status": "error", "message": str(exc)}

    warnings = list(facts.get("warnings") or [])
    if not facts["can_write"] and not ds.get("read_only"):
        warnings.append(
            f"{facts['principal'] or 'the token'} cannot push to {owner}/{repo} "
            "but the connector is not marked read-only"
        )
    configured_branch = str(ds.get("default_branch") or "")
    repo_default = facts.get("default_branch")
    branch_note = ""
    if repo_default:
        branch_note = f"; repository default branch {repo_default}"
        if configured_branch and configured_branch != repo_default:
            branch_note += f" (connector targets {configured_branch})"

    token_label = (
        f"{facts['token_class']} token"
        if facts["token_class"] != "unknown"
        else "token"
    )
    access = "write" if facts["can_write"] else "read-only"
    message = (
        f"Authenticated as {facts['principal'] or 'unknown principal'} "
        f"({token_label}); {access} access to {owner}/{repo}{branch_note}"
    )
    if warnings:
        message += " — WARNING: " + "; ".join(warnings)
    return {
        "status": "ok",
        "message": message,
        "details": {**facts, "warnings": warnings},
    }
