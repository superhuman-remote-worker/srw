"""Shared explicit, default-off inventory policy for both service processes."""

from dataclasses import dataclass
import hashlib
import json
import os
import re

from shared.vm_inventory_transport import decode_document
from shared.vm_lifecycle_auth import configured_secret
from shared.vm_resource_inventory import InventoryError


_LIMITS = {
    "publishIntervalSeconds",
    "staleAfterSeconds",
    "maxItems",
    "maxBytes",
    "requestTimeoutSeconds",
    "collectionTimeoutSeconds",
    "publicationTimeoutSeconds",
    "historyLimit",
}
_POLICY_FIELDS = {
    "observerEnabled",
    "shadowEnabled",
    "enforcementEnabled",
    "clusterWidePodReadAcknowledged",
    "stableClusterId",
    "inventory",
    "hostCost",
    "nodeHeadroom",
    "fairness",
}


@dataclass(frozen=True)
class InventorySettings:
    cluster_id: str
    namespace: str
    policy_digest: str
    label_keys: tuple[str, ...]
    max_items: int
    max_bytes: int
    stale_after_seconds: int
    history_limit: int
    publish_interval_seconds: int
    request_timeout_seconds: int
    collection_timeout_seconds: int
    publication_timeout_seconds: int

    @classmethod
    def from_environment(cls, source=None):
        env = os.environ if source is None else source
        raw = env.get("VM_RESOURCE_ADMISSION_CONFIG", "")
        if not raw:
            return None
        try:
            value = decode_document(raw.encode("utf-8"), max_bytes=16384)
            settings = cls.from_document(value)
            if settings is not None and configured_secret(env) is None:
                raise ValueError
            return settings
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise InventoryError("invalid_inventory_configuration") from None

    @classmethod
    def from_document(cls, value):
        """Pure observer-policy identity; runtime startup separately requires HMAC.

        This does not authorize observation or enable unfinished admission modes.
        Empty resource-value maps remain valid for observation alone.
        """
        try:
            if not isinstance(value, dict) or set(value) != {
                "mode",
                "namespace",
                "policy",
            }:
                raise ValueError
            policy = value["policy"]
            if not isinstance(policy, dict) or set(policy) != _POLICY_FIELDS:
                raise ValueError
            for field in (
                "observerEnabled",
                "shadowEnabled",
                "enforcementEnabled",
                "clusterWidePodReadAcknowledged",
            ):
                if type(policy[field]) is not bool:
                    raise ValueError
            # Later stages must implement their complete rollout gates before
            # permitting these modes. This slice observes only.
            if policy["shadowEnabled"] or policy["enforcementEnabled"]:
                raise ValueError
            if not policy["observerEnabled"]:
                return None
            if (
                value["mode"] != "same-cluster"
                or not policy["clusterWidePodReadAcknowledged"]
            ):
                raise ValueError
            cluster, namespace = policy["stableClusterId"], value["namespace"]
            for identifier, limit in ((cluster, 253), (namespace, 63)):
                if (
                    not isinstance(identifier, str)
                    or len(identifier) > limit
                    or re.fullmatch(r"[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?", identifier)
                    is None
                ):
                    raise ValueError
            inv = policy["inventory"]
            if not isinstance(inv, dict) or set(inv) != _LIMITS | {"nodeLabelKeys"}:
                raise ValueError
            for key in _LIMITS:
                if type(inv[key]) is not int or not 1 <= inv[key] < 2**63:
                    raise ValueError
            labels = inv["nodeLabelKeys"]
            if not isinstance(labels, list) or not 1 <= len(labels) <= 128:
                raise ValueError
            if any(
                not isinstance(key, str)
                or not key
                or len(key) > 253
                or key != key.strip()
                for key in labels
            ):
                raise ValueError
            if (
                len(set(labels)) != len(labels)
                or "kubernetes.io/hostname" not in labels
            ):
                raise ValueError
            for section in ("hostCost", "nodeHeadroom", "fairness"):
                if not isinstance(policy[section], dict):
                    raise ValueError
            encoded = json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=False,
                allow_nan=False,
            ).encode()
            return cls(
                cluster_id=cluster,
                namespace=namespace,
                policy_digest="sha256:" + hashlib.sha256(encoded).hexdigest(),
                label_keys=tuple(sorted(labels)),
                max_items=inv["maxItems"],
                max_bytes=inv["maxBytes"],
                stale_after_seconds=inv["staleAfterSeconds"],
                history_limit=inv["historyLimit"],
                publish_interval_seconds=inv["publishIntervalSeconds"],
                request_timeout_seconds=inv["requestTimeoutSeconds"],
                collection_timeout_seconds=inv["collectionTimeoutSeconds"],
                publication_timeout_seconds=inv["publicationTimeoutSeconds"],
            )
        except (ValueError, TypeError, KeyError, UnicodeError):
            raise InventoryError("invalid_inventory_configuration") from None
