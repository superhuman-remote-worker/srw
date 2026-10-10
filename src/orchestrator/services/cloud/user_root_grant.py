"""A user's own account in the main cloud, granted to SRW by that user.

Main cloud slice 4 (the user-root connector). A provider that can mount a
user's whole account declares ``cloud_folder · user_root · read_write`` in
its support matrix and implements :class:`SupportsUserRootGrant`: the user
grants SRW a credential for their own account through the provider's own
consent flow (Nextcloud: Login Flow v2, "Connect your cloud"), SRW checks
whose account it is, proves it can read the account's files, and later
revokes it with the credential itself.

The credential is the account's own: it cannot be restricted to one folder
or to reading, which is why the matrix offers only ``read_write`` for a
user root. It is never printed (every value type here keeps it out of its
``repr``) and is stored only as the connector's encrypted credentials.

Checked on the adapter with ``isinstance``, so no caller names a provider.

Design: knowledge-base/knowledge/features/main_cloud_as_connectors.md
(slice 4) and nextcloud_user_home_app_token_grant.md (Lifecycle, Security).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, Optional, Protocol, runtime_checkable

from orchestrator.services.cloud.handles import ProjectFolderHandle

#: How a deployment lets its users grant their account: through the
#: provider's consent flow (the user clicks Connect), or not at all.
#: ``silent`` (minted by SRW without a click) is reserved: it is not built,
#: and a deployment that asks for it gets ``off``.
UserRootGrantMode = Literal["login_flow", "off"]
GRANT_MODES: tuple[str, ...] = ("login_flow", "off")

#: What a probe of the account's files with its credential found: it reads
#: them; the provider refused the credential (revoked, or the account was
#: disabled); the account has no files to read; or the provider could not
#: be asked.
VerifyOutcome = Literal["ok", "rejected", "missing", "unreachable"]
VERIFY_OUTCOMES: tuple[str, ...] = ("ok", "rejected", "missing", "unreachable")


@dataclass(frozen=True, slots=True)
class LoginFlowStart:
    """A consent flow the user completes in their browser.

    ``login_url`` is the page the user opens (the provider's public URL);
    ``poll_token`` is how SRW asks whether they finished, never shown to
    anyone.
    """

    login_url: str
    poll_token: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class LoginFlowGrant:
    """What a finished consent flow hands SRW: the login name the account
    authenticates with, and the credential, once."""

    login_name: str
    credential: str = field(repr=False)


@dataclass(frozen=True, slots=True)
class CloudAccount:
    """The provider's own record of an account, read as its administrator.

    ``administrator`` is whether the account administers the provider or
    any of its groups (a group admin manages other people's accounts);
    ``service_account`` whether it is one of SRW's own (the agent service
    account, a per-folder or protected-mode principal, a member of an SRW
    service group). SRW never mounts either kind's whole root as a user's.
    """

    account_id: str
    email: Optional[str]
    enabled: bool
    administrator: bool
    service_account: bool = False


@runtime_checkable
class SupportsUserRootGrant(Protocol):
    """Optional capability: a user's own account, granted to SRW by the user.

    Every credential argument is the account's own, from
    :meth:`poll_login_flow`; none is ever logged, raised in an error or
    returned in a ``repr``.
    """

    backend_id: str

    @property
    def user_root_grant_mode(self) -> UserRootGrantMode:
        """How this deployment lets users grant their account."""
        ...

    @property
    def user_root_client_name(self) -> str:
        """The name the grant carries in the provider's device list."""
        ...

    async def start_login_flow(self) -> LoginFlowStart:
        """Begin a consent flow. ``CloudBackendError`` when the provider
        cannot start one."""
        ...

    async def poll_login_flow(self, poll_token: str) -> Optional[LoginFlowGrant]:
        """The grant once the user finished, ``None`` until then.
        ``CloudBackendError`` when the provider cannot be asked."""
        ...

    async def user_root_account_id(
        self, login_name: str, credential: str
    ) -> Optional[str]:
        """The account a credential authenticates as, asked with the
        credential itself; ``None`` when the provider refuses it."""
        ...

    async def user_root_account(self, account_id: str) -> Optional[CloudAccount]:
        """The provider's record of ``account_id``, ``None`` when it has
        none. ``CloudBackendError`` when it cannot be asked."""
        ...

    def user_root_handle(self, account_id: str) -> ProjectFolderHandle:
        """The folder handle of ``account_id``'s whole root."""
        ...

    async def verify_user_root(
        self, account_id: str, login_name: str, credential: str
    ) -> VerifyOutcome:
        """Probe ``account_id``'s root with its credential. Never raises."""
        ...

    async def revoke_user_root(self, login_name: str, credential: str) -> bool:
        """Revoke the credential with the credential itself; True when the
        provider no longer accepts it (revoked now or already). Never
        raises."""
        ...

    async def rotate_user_root(self, login_name: str, credential: str) -> str:
        """A new credential in place of ``credential``, which stops working.
        ``CloudBackendError`` on failure. Nothing calls it on a schedule: a
        rotation breaks every live mount of the old credential."""
        ...


__all__ = [
    "GRANT_MODES",
    "VERIFY_OUTCOMES",
    "CloudAccount",
    "LoginFlowGrant",
    "LoginFlowStart",
    "SupportsUserRootGrant",
    "UserRootGrantMode",
    "VerifyOutcome",
]
