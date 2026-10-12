"""OCI manifest resolution for operator-approved VM preparation registries.

The resolver lives in :mod:`shared.oci_registry` (connector driver hosting
resolves driver images with it too). Preparation keeps its contract: only the
operator-approved registry hosts, and token endpoints the operator names.
"""

from shared.oci_registry import (
    ACCEPT,
    RegistryResolutionError,
    RegistryResolver as _SharedRegistryResolver,
)


class RegistryResolver(_SharedRegistryResolver):
    def __init__(self, *, hosts, insecure_hosts=(), token_hosts=(), transport=None):
        super().__init__(
            hosts=hosts,
            insecure_hosts=insecure_hosts,
            token_hosts=token_hosts,
            transport=transport,
            refusal="Image registry is not enabled for preparation.",
        )


__all__ = ["ACCEPT", "RegistryResolutionError", "RegistryResolver"]
