"""Where main-cloud adapters keep their service accounts' app passwords.

A Nextcloud service account that signs in with its login password pays a full
Argon2 login on every request; with an app password Nextcloud only looks the
token up (bundled Nextcloud performance, WP1b). The app password is minted
once per installation and stored, encrypted, in
``main_cloud_service_credentials``, so every orchestrator replica and every
restart uses the same one.

The adapters are built before the database connects and outside any request,
so the store is process-wide: the application sets it when it is built, as
``connector_credential_leases.configure_lease_window`` is set. Without one
(a test, an operator script) the adapters keep their login passwords.
"""

from __future__ import annotations

from typing import Any, Optional, Protocol


class ServiceCredentialStore(Protocol):
    """The two ``PostgresDB`` methods the adapters use."""

    async def get_main_cloud_service_credential(
        self, backend_instance_id: str, login_name: str
    ) -> Optional[dict[str, Any]]: ...

    async def put_main_cloud_service_credential(
        self,
        backend_instance_id: str,
        login_name: str,
        *,
        ciphertext: str,
        token_name: str,
        expected_revision: int | None,
    ) -> bool: ...


_state: dict[str, Optional[ServiceCredentialStore]] = {"store": None}


def configure_service_credential_store(
    store: Optional[ServiceCredentialStore],
) -> None:
    _state["store"] = store


def service_credential_store() -> Optional[ServiceCredentialStore]:
    return _state["store"]


__all__ = [
    "ServiceCredentialStore",
    "configure_service_credential_store",
    "service_credential_store",
]
