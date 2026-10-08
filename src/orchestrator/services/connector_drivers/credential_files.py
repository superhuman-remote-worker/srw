"""``srw.generic-file/v1`` and ``srw.kubeconfig/v1``: credential files.

The connector stores ``credentials.files[]``; validation applies the type's
defaults and path rules (``orchestrator.security.credential_files``) on every
write.  The agent writes the files into the workspace home of each job or
session it is attached to, so there is no connection to probe from here.
Test reports what a connector saved before the credential-file allowlist
would no longer deliver (``shared.connectors.file_targets``).

A kubeconfig connector may instead mint (C5, :class:`KubeconfigDriver`): its
``token_request`` config names a ServiceAccount, its stored kubeconfig is
the minting credential, and Test mints a token and revokes it.
"""

from __future__ import annotations

import secrets
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from fastapi import HTTPException

from orchestrator.security.credential_files import (
    CredentialFileValidationError,
    normalize_credential_files,
)
from orchestrator.services.connector_drivers.base import (
    BindContext,
    CheckContext,
    ConnectorDraft,
    DatasourceDriver,
    NormalizedConnector,
    SecretLeaf,
    ValidationContext,
    payload_entry,
)
from orchestrator.services.connector_drivers.token_request import (
    parse_minting_kubeconfig,
)
from orchestrator.services.datasource_config import stored_json_object
from shared.connectors.builtin import GENERIC_FILE_SPEC, KUBECONFIG_SPEC
from shared.connectors.token_request import (
    CONFIG_KEY as TOKEN_REQUEST_KEY,
    TokenRequestConfigError,
    parse_token_request,
    token_request_options,
)
from shared.connectors.envelope import (
    DriverError,
    DriverOutcome,
    api_check_result,
    unsupported_check,
)
from shared.connectors.file_targets import (
    allowed_targets_text,
    mode_problem,
    target_problem,
)


def undeliverable_files(credentials: Mapping[str, Any] | None) -> list[str]:
    """Why each stored file would not be delivered as saved (empty: none).

    A row saved before the allowlist keeps its target; the agent skips it
    and an execute bit is dropped. Paths and modes only, never contents.
    """
    files = (
        (credentials or {}).get("files") if isinstance(credentials, Mapping) else None
    )
    problems: list[str] = []
    for item in files if isinstance(files, list) else []:
        if not isinstance(item, Mapping):
            continue
        path = str(item.get("target_path") or "")
        _relative, refused = target_problem(path)
        if refused is not None:
            problems.append(f"{path or '(no target)'} is {refused}")
        try:
            mode = int(str(item.get("mode") or "0600"), 8)
        except ValueError:
            continue
        if mode_problem(mode) is not None:
            problems.append(f"{path} has mode {mode:04o}: {mode_problem(mode)}")
    return problems


def new_directory_token() -> str:
    """A new connector's default-directory token (no id exists yet)."""
    return secrets.token_hex(4)


def directory_token(existing: Mapping[str, Any] | None) -> str:
    """The token a connector's default directory carries: its id's first
    eight hex digits once it has one, a fresh token while it is created."""
    if existing is not None and existing.get("id"):
        return str(existing["id"]).replace("-", "")[:8]
    return new_directory_token()


class CredentialFileDriver(DatasourceDriver):
    def _normalize_files(
        self,
        name: str,
        credentials: dict[str, Any] | None,
        existing: Mapping[str, Any] | None,
    ) -> dict[str, Any] | None:
        try:
            return normalize_credential_files(
                self.type_id,
                name,
                credentials,
                directory_token=directory_token(existing),
            )
        except CredentialFileValidationError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            config = self.no_config(draft, existing)
            credentials = self._normalize_files(
                draft.name or "", self.stored_credentials(draft, existing), None
            )
            return NormalizedConnector(draft.connection_url, config, credentials)
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            # A rename moves the default target paths with the new name.
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials, existing
            )
        return NormalizedConnector(
            draft.connection_url, self.no_config(draft, existing), credentials
        )

    def credential_config(self, credentials: Mapping[str, Any]) -> dict[str, Any]:
        return self.credential_file_targets(credentials)

    def secret_leaves(self, credentials: Mapping[str, Any]) -> list[SecretLeaf]:
        return self.credential_file_leaves(credentials)

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        # Nothing to connect to: the file is the credential. A row saved
        # before the allowlist says what the workspace will not receive.
        problems = undeliverable_files(credentials)
        if problems:
            return api_check_result(
                DriverOutcome(
                    error=DriverError(
                        "config",
                        "Not delivered as saved: "
                        + "; ".join(problems)
                        + f". Credential files go under {allowed_targets_text()}; "
                        "save the connector with a new target.",
                    )
                )
            )
        return unsupported_check(
            f"{self.spec.title} connectors have no connection test; the "
            "file's path and size are checked when it is saved"
        )


#: The one config a kubeconfig connector may hold.
TOKEN_REQUEST_ONLY_DETAIL = (
    "A kubeconfig connector's config holds token_request only (TokenRequest minting)"
)


class KubeconfigDriver(CredentialFileDriver):
    """``srw.kubeconfig/v1``, with optional TokenRequest minting (C5).

    Without a ``token_request`` config the stored kubeconfig is delivered as
    it is (D1d), whatever its user holds (an ``exec`` plugin included,
    as before). With one, the stored kubeconfig is the minting credential:
    it must hold a bearer token SRW can use from its own process
    (``shared.connectors.token_request``), it is never delivered, and each
    execution's workspace receives a kubeconfig with a token SRW minted for
    the target ServiceAccount (``connector_minted_credentials``).
    """

    def __init__(self) -> None:
        super().__init__(KUBECONFIG_SPEC)

    @staticmethod
    def _config(
        draft: ConnectorDraft, existing: Mapping[str, Any] | None
    ) -> dict[str, Any] | None:
        """The config to store (``None`` leaves an update's alone)."""
        if draft.config is None:
            return {} if existing is None else None
        unknown = set(draft.config) - {TOKEN_REQUEST_KEY}
        if unknown:
            raise HTTPException(status_code=400, detail=TOKEN_REQUEST_ONLY_DETAIL)
        if draft.config.get(TOKEN_REQUEST_KEY) is None:
            return {}
        try:
            options = parse_token_request(draft.config[TOKEN_REQUEST_KEY])
        except TokenRequestConfigError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        return {TOKEN_REQUEST_KEY: options.as_config()}

    @staticmethod
    def _check_minting(config: Any, credentials: Any) -> None:
        """A minting connector's kubeconfig must be one SRW can mint with."""
        if not isinstance(config, Mapping) or config.get(TOKEN_REQUEST_KEY) is None:
            return
        from orchestrator.services.connector_drivers.provider_http import (
            ProviderError,
            tls_context,
        )

        files = credentials.get("files") if isinstance(credentials, Mapping) else None
        first = files[0] if isinstance(files, list) and files else None
        try:
            minting = parse_minting_kubeconfig(
                first.get("contents") if isinstance(first, Mapping) else None
            )
            tls_context(minting.ca_pem)
        except TokenRequestConfigError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from None
        except ProviderError:
            raise HTTPException(
                status_code=400,
                detail=(
                    "the minting kubeconfig's certificate-authority-data does not "
                    "load as CA certificates"
                ),
            ) from None

    @staticmethod
    def _require_enabled(config: Any) -> None:
        from orchestrator.services.connector_minted_credentials import (
            DISABLED_DETAIL,
            minting_enabled,
        )

        if (
            isinstance(config, Mapping)
            and config.get(TOKEN_REQUEST_KEY) is not None
            and not minting_enabled()
        ):
            raise HTTPException(status_code=403, detail=DISABLED_DETAIL)

    @staticmethod
    def _stored_options(existing: Mapping[str, Any]) -> Any:
        try:
            return token_request_options(stored_json_object(existing.get("config")))
        except TokenRequestConfigError:
            return None

    async def validate(
        self,
        draft: ConnectorDraft,
        *,
        existing: Mapping[str, Any] | None,
        ctx: ValidationContext,
    ) -> NormalizedConnector:
        if existing is None:
            config = self._config(draft, None)
            self._require_enabled(config)
            credentials = self._normalize_files(
                draft.name or "", self.stored_credentials(draft, existing), None
            )
            self._check_minting(config, credentials)
            return NormalizedConnector(draft.connection_url, config, credentials)
        credentials = self.stored_credentials(draft, existing)
        if credentials is not None:
            credentials = self._normalize_files(
                draft.name or existing.get("name", ""), credentials, existing
            )
        config = self._config(draft, existing)
        if config is not None or credentials is not None:
            effective = (
                config
                if config is not None
                else stored_json_object(existing.get("config"))
            )
            self._require_enabled(effective)
            if credentials is None:
                self._same_use(self._stored_options(existing), effective)
            self._check_minting(
                effective,
                credentials
                if credentials is not None
                else stored_json_object(existing.get("credentials")),
            )
        return NormalizedConnector(draft.connection_url, config, credentials)

    @staticmethod
    def _same_use(stored: Any, effective: Mapping[str, Any]) -> None:
        """An edit that keeps the stored kubeconfig may not change what it is
        used for: minting turned on or off (a minting credential would be
        delivered as it is, or a delivered one start minting), another
        target ServiceAccount, or other audiences (tokens for other
        services). Those need the kubeconfig sent again; a lifetime edit
        does not."""
        options = token_request_options(effective)
        if stored is None and options is None:
            return
        if stored is None or options is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    "Turning TokenRequest minting on or off changes what the "
                    "stored kubeconfig is used for: send the kubeconfig again"
                ),
            )
        if (stored.namespace, stored.service_account) != (
            options.namespace,
            options.service_account,
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Changing the target ServiceAccount points the stored minting "
                    "kubeconfig elsewhere: send the kubeconfig again"
                ),
            )
        if sorted(stored.audiences) != sorted(options.audiences):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Changing the audiences mints the stored minting kubeconfig's "
                    "tokens for other services: send the kubeconfig again"
                ),
            )

    def bind(
        self, row: Mapping[str, Any], credentials: Any, *, ctx: BindContext
    ) -> dict[str, Any] | None:
        from orchestrator.services.connector_minted_credentials import (
            MINTED_KEY,
            kubeconfig_marker,
        )

        if stored_json_object(row.get("config")).get(TOKEN_REQUEST_KEY) is None:
            return super().bind(row, credentials, ctx=ctx)
        # The minting kubeconfig never rides the payload: the delivery puts
        # in the kubeconfig with the execution's minted token.
        entry = payload_entry(
            row, credentials={}, read_only=row.get("project_read_only", False)
        )
        entry[MINTED_KEY] = kubeconfig_marker(row, credentials)
        return entry

    async def check(
        self, row: Mapping[str, Any], credentials: dict[str, Any], *, ctx: CheckContext
    ) -> dict[str, Any]:
        config = stored_json_object(row.get("config"))
        if config.get(TOKEN_REQUEST_KEY) is None:
            return await super().check(row, credentials, ctx=ctx)
        return await probe_token_request(
            row, config, credentials, requester=getattr(ctx, "requester", None)
        )


async def probe_token_request(
    row: Mapping[str, Any],
    config: Mapping[str, Any],
    credentials: Mapping[str, Any],
    *,
    requester: str | None = None,
) -> dict[str, Any]:
    """Test a minting kubeconfig connector: mint a token for the target
    ServiceAccount, bound to a Secret, and delete the Secret again. The mint
    is recorded (``connector_minted_credentials``), so a delete that fails
    is retried by the sweep. Never discloses the minting credential or the
    token."""
    from orchestrator.services.connector_minted_credentials import (
        MintFailure,
        mint_for_test,
    )

    try:
        options = parse_token_request(config.get(TOKEN_REQUEST_KEY))
        files = credentials.get("files")
        first = files[0] if isinstance(files, list) and files else {}
        minting = parse_minting_kubeconfig(
            first.get("contents") if isinstance(first, Mapping) else None
        )
    except TokenRequestConfigError as exc:
        return {"status": "error", "message": str(exc)}
    try:
        minted, revoke = await mint_for_test(
            {
                **row,
                "type": KUBECONFIG_SPEC.legacy_type,
                "config": config,
                "credentials": credentials,
            },
            requester=requester,
        )
    except MintFailure as exc:
        return {"status": "error", "message": str(exc)}
    revoked = await revoke()
    lifetime = int((minted.expires_at - datetime.now(timezone.utc)).total_seconds())
    message = (
        f"Minted a token for ServiceAccount {options.namespace}/"
        f"{options.service_account} through TokenRequest (it would live "
        f"about {max(lifetime, 0)} s)"
        + (
            " and revoked it by deleting its bound Secret"
            if revoked
            else "; deleting its bound Secret FAILED (SRW keeps trying): grant "
            "the minting credential delete on secrets in that namespace"
        )
        + ". The workspace receives a kubeconfig with such a token only; the "
        "target ServiceAccount's RBAC decides what it may do, at either access "
        "level"
    )
    return {
        "status": "ok" if revoked else "error",
        "message": message,
        "details": {
            "server": minting.server,
            "service_account": f"{options.namespace}/{options.service_account}",
            "expiration_seconds": options.expiration_seconds,
            "lifetime_seconds": lifetime,
        },
    }


def drivers() -> tuple[CredentialFileDriver, ...]:
    return (
        KubeconfigDriver(),
        CredentialFileDriver(GENERIC_FILE_SPEC),
    )
