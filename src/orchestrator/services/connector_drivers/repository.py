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
reports the host key the connector form offers to pin. The address is the
connector's own, so either probe is held to the provider calls' rules
(``provider_http``): resolved once and checked against the project tier (a
private address only where the tier allows one, or on a host the operator
lists in ``connectors.providerMinting.privateHosts``), dialled at the checked
address, under one deadline, without redirects or proxies, and refused or
failed with a fixed reason; the raw detail goes to the server log only.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from typing import Any
from urllib.parse import urlsplit

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
from shared.connectors.github_app import (
    AUTH_METHOD as GITHUB_APP_AUTH,
    CONFIG_KEY as GITHUB_APP_CONFIG,
    GitHubAppConfigError,
    parse_github_app,
    uses_github_app,
)


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
        """The auth method; the token, the key and a GitHub App's private key
        stay secret."""
        return auth_method_config(credentials)

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        return top_level_leaves(credentials, ("token", "ssh_key", "private_key"))

    def github_app_checked(
        self,
        *,
        connection_url: str | None,
        config: Mapping[str, Any] | None,
        credentials: Mapping[str, Any] | None,
        check_key: bool,
        existing: Mapping[str, Any] | None = None,
    ) -> tuple[dict[str, Any], dict[str, Any] | None]:
        """A GitHub App connector's config and credentials, normalized (C5):
        a GitHub repository URL, ``github_app: {app_id, installation_id,
        api_base?}`` on the repository's own API host and the App's
        unencrypted RSA private key, and nothing else secret. HTTP 400
        otherwise; a ``github_app`` config on a connector that does not
        authenticate as an App is refused too. An edit that keeps the stored
        key (``check_key`` off) may not move the API base or the
        repository's host: the key would sign requests elsewhere."""
        from orchestrator.services.connector_drivers.github_app import (
            normalize_private_key,
        )
        from orchestrator.services.connector_minted_credentials import (
            DISABLED_DETAIL,
            minting_enabled,
        )

        out = dict(config or {})
        if uses_github_app(credentials) and not minting_enabled():
            raise HTTPException(status_code=403, detail=DISABLED_DETAIL)
        if not uses_github_app(credentials):
            if out.get(GITHUB_APP_CONFIG) is not None:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "github_app config needs credentials with auth_method "
                        "github_app"
                    ),
                )
            return out, None
        if out.get("forge") != "github":
            raise HTTPException(
                status_code=400,
                detail="A GitHub App connector's forge is github",
            )
        try:
            options = parse_github_app(out, connection_url)
        except GitHubAppConfigError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        configured = out[GITHUB_APP_CONFIG].get("api_base")
        out[GITHUB_APP_CONFIG] = options.as_config(
            configured_api_base=options.api_base if configured else None
        )
        if not check_key:
            self._same_targets(existing, options, connection_url)
            return out, None
        extra = sorted(
            key
            for key in (credentials or {})
            if key not in ("auth_method", "private_key")
        )
        if extra:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A GitHub App connector stores the App's private key only, "
                    f"not {', '.join(extra)}"
                ),
            )
        try:
            key = normalize_private_key((credentials or {}).get("private_key"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        return out, {"auth_method": GITHUB_APP_AUTH, "private_key": key}

    @staticmethod
    def _same_targets(
        existing: Mapping[str, Any] | None, options: Any, connection_url: Any
    ) -> None:
        """The stored key keeps signing for what it was saved for: the same
        App, installation, API base, repository host and repository
        (``owner/name``, as GitHub compares them, case-insensitively)."""
        if existing is None:
            return
        try:
            stored = parse_github_app(
                stored_json_object(existing.get("config")),
                existing.get("connection_url"),
            )
        except GitHubAppConfigError:
            return  # no stored App: check_key would be on

        def target(app: Any, url: Any) -> tuple[str, ...]:
            return (
                str(app.app_id),
                str(app.installation_id),
                app.api_base,
                (urlsplit(str(url or "")).hostname or "").lower(),
                f"{app.owner}/{app.repository}".lower(),
            )

        if target(stored, existing.get("connection_url")) != target(
            options, connection_url
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Pointing a GitHub App connector at another App, "
                    "installation, API base or repository needs the App's "
                    "private key again"
                ),
            )

    @staticmethod
    def static_keys_checked(credentials: Mapping[str, Any] | None) -> None:
        """A token or SSH key repository stores no ``username``: the forge
        token is presented as ``oauth2``, and only a credential SRW minted
        (a GitHub App's installation token) names another (C5)."""
        if not isinstance(credentials, Mapping) or uses_github_app(credentials):
            return
        named = sorted({"username", "minted"} & set(credentials))
        if named:
            raise HTTPException(
                status_code=400,
                detail=(
                    "A repository connector's credentials carry no "
                    f"{' or '.join(named)}: a token is presented as oauth2"
                ),
            )

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
            self.static_keys_checked(credentials)
            config = self.validate_endpoint(
                connection_url=draft.connection_url,
                config=config,
                credentials=credentials,
            )
            # The clone and Test both need it; a declared forge used to make a
            # URL-less connector valid.
            if not (draft.connection_url or "").strip():
                raise HTTPException(status_code=400, detail=_URL_REQUIRED)
            config, app_credentials = self.github_app_checked(
                connection_url=draft.connection_url,
                config=config,
                credentials=credentials,
                check_key=True,
            )
            return NormalizedConnector(
                draft.connection_url, config, app_credentials or credentials
            )

        credentials = self.stored_credentials(draft, existing)
        self.static_keys_checked(credentials)
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
        if (
            config is not None
            or credentials is not None
            or "connection_url" in draft.supplied
        ):
            # A GitHub App connector's App, key and repository are checked
            # as the edit leaves them (C5).
            effective, app_credentials = self.github_app_checked(
                connection_url=draft.connection_url or existing.get("connection_url"),
                config=(
                    config
                    if config is not None
                    else stored_json_object(existing.get("config"))
                ),
                credentials=(
                    credentials
                    if credentials is not None
                    else stored_json_object(existing.get("credentials"))
                ),
                check_key=credentials is not None,
                existing=existing,
            )
            if config is not None:
                config = effective
            if app_credentials is not None:
                credentials = app_credentials
        return NormalizedConnector(draft.connection_url, config, credentials)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        if uses_github_app(credentials):
            result = await probe_github_app(
                dict(row), credentials, requester=getattr(ctx, "requester", None)
            )
            if result.get("status") != "ok":
                return result
        else:
            from orchestrator.services.connector_drivers.provider_http import (
                tier_allows_private,
            )

            result = await probe_repository(
                dict(row),
                row["connection_url"],
                credentials,
                allow_private=await tier_allows_private(
                    getattr(ctx, "store", None), row.get("id")
                ),
            )
            if not token_auth(row, credentials):
                return result
        # Where the git swap driver is installed, Test says how the token is
        # delivered now (through the driver, or the fallback and why) and
        # probes the upstream's TLS without a credential (C3).
        from orchestrator.services import connector_git_swap_delivery as swaps

        report = await swaps.delivery_report(row, token=credentials.get("token"))
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
        github_app = uses_github_app(credentials)
        if isinstance(credentials, Mapping):
            # Nor does a GitHub App's private key: SRW mints the execution's
            # installation token with it at delivery (C5).
            credentials = {
                key: value
                for key, value in credentials.items()
                if key not in ("ssh_key", "private_key")
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
        read_only = row.get("project_read_only", False)
        if github_app:
            from orchestrator.services.connector_minted_credentials import (
                connector_read_only,
            )

            # A GitHub App connector's read-only is enforced by the token SRW
            # mints (contents: read), so its own rule (a public one's
            # included) is the entry's: the lease, the repo tools and the
            # minted token all bind at the same level (C5).
            read_only = bool(read_only) or connector_read_only(row)
        entry = payload_entry(
            row,
            credentials=credentials,
            read_only=read_only,
            fields=fields,
        )
        if row.get("require_default_branch") is True:
            entry["require_default_branch"] = True
        if ssh_identity is not None:
            entry["ssh_identity"] = ssh_identity
        if github_app:
            from orchestrator.services.connector_minted_credentials import (
                MINTED_KEY,
                github_app_marker,
            )

            entry[MINTED_KEY] = github_app_marker(row)
        # A token repository goes through the git swap driver where it is
        # installed and serves the URL; otherwise the installation's fallback.
        route_token_repository(
            entry, git_swap=ctx.git_swap, fallback=ctx.git_swap_fallback
        )
        return entry


#: One Test's forge probe, both reads included: resolution, connections and
#: answers.
PROBE_DEADLINE_SECONDS = 15.0


def _guarded_fetch(
    forge: str, *, ca_pem: str | None, allow_private: bool
) -> Callable[[str, dict[str, str]], Awaitable[Any]]:
    """The probe's GET through ``provider_http``: a checked, pinned address,
    a capped answer, no redirect or proxy, and a fixed text when no answer
    came."""
    from orchestrator.services.connector_drivers.provider_http import (
        ProviderError,
        provider_request,
    )
    from shared.runtime.services.forge import ForgeError, ProbeAnswer

    async def fetch(url: str, headers: dict[str, str]) -> ProbeAnswer:
        try:
            answer = await provider_request(
                "GET",
                url,
                who=forge,
                headers=headers,
                ca_pem=ca_pem,
                allow_private=allow_private,
                deadline=PROBE_DEADLINE_SECONDS,
            )
        except ProviderError as exc:
            raise ForgeError(str(exc)) from None
        return ProbeAnswer(
            status=answer.status, headers=answer.headers, body=answer.body
        )

    return fetch


async def probe_repository(
    ds: dict[str, Any],
    url: str | None,
    creds: dict[str, Any],
    *,
    allow_private: bool = False,
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

    The forge's address is checked before anything is sent to it
    (``allow_private``: the connector's project tier allows private
    addresses), and both reads run under one deadline
    (:data:`PROBE_DEADLINE_SECONDS`).
    """
    from orchestrator.services.connector_git_swap_delivery import upstream_ca_of
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
        probed = await probe_workspace_ssh_connector(
            {**ds, "credentials": creds}, allow_private=allow_private
        )
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

    fetch = _guarded_fetch(
        forge, ca_pem=upstream_ca_of(config), allow_private=allow_private
    )
    try:
        async with asyncio.timeout(PROBE_DEADLINE_SECONDS):
            facts = await probe_repository_access(target, fetch=fetch)
    except TimeoutError:
        return {
            "status": "error",
            "message": (
                f"Repository probe timed out after {PROBE_DEADLINE_SECONDS:.0f}s"
            ),
        }
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


async def probe_github_app(
    ds: dict[str, Any],
    credentials: Mapping[str, Any],
    *,
    requester: str | None = None,
) -> dict[str, Any]:
    """Test a GitHub App connector (C5): mint a read token for its one
    repository, read the repository with it, and revoke it again. The mint
    is recorded, so a revoke that fails is retried by the sweep. Never
    discloses the key or the token."""
    from orchestrator.services.connector_drivers.github_app import (
        normalize_private_key,
        repository_facts,
    )
    from orchestrator.services.connector_drivers.provider_http import ProviderError
    from orchestrator.services.connector_git_swap_delivery import upstream_ca_of
    from orchestrator.services.connector_minted_credentials import (
        MintFailure,
        mint_for_test,
    )

    config = stored_json_object(ds.get("config"))
    try:
        options = parse_github_app(config, ds.get("connection_url"))
        normalize_private_key(credentials.get("private_key"))
    except (GitHubAppConfigError, ValueError) as exc:
        return {"status": "error", "message": str(exc)}
    try:
        minted, revoke = await mint_for_test(
            {**ds, "type": "repository", "config": config, "credentials": credentials},
            requester=requester,
        )
    except MintFailure as exc:
        return {"status": "error", "message": str(exc)}
    try:
        facts = await repository_facts(
            options,
            minted.token,
            ca_pem=upstream_ca_of(config),
            allow_private=bool(minted.material.get("allow_private")),
        )
    except ProviderError as exc:
        return {"status": "error", "message": str(exc)}
    finally:
        await revoke()  # a failure is the sweep's
    branch = facts.get("default_branch")
    return {
        "status": "ok",
        "message": (
            f"GitHub App {options.app_id} (installation {options.installation_id}) "
            f"minted a one-hour token for {facts['repository']} with contents: "
            "read, read it"
            + (f" (default branch {branch})" if branch else "")
            + " and revoked the token; each execution gets its own, with "
            "contents: read or write by its access level"
        ),
        "details": {**facts, "api_base": options.api_base, "auth": "github_app"},
    }
