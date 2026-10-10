"""Admission-time authority for immutable job deliverable contracts."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from shared.connectors.builtin import delivers_in
from shared.connectors.contract import bound_read_only
from shared.datasource_policy import resolve_repo_clone_names
from shared.runtime.services.forge import ForgeError, parse_owner_repo
from shared.deliverable_contract import (
    DeliverableContractError,
    cloned_repo_alias,
    cloned_repo_deliverables,
    normalize_repository_identity,
    parse_required_deliverables,
    pr_repositories,
)

BLOCKED_UNDELIVERED_OUTCOME = "blocked_undelivered"


class DeliveryContractConflict(ValueError):
    """A stable refusal before any job or ticket claim is inserted."""

    def __init__(self, code: str, message: str, **fields: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.fields = fields


@dataclass(frozen=True, slots=True)
class DeliveryContractPlan:
    """Server-normalized immutable contract ready for transactional insert."""

    deliverables: tuple[str, ...]
    pr_repositories: tuple[str, ...]
    pr_bindings: tuple[dict[str, Any], ...]
    digest: str

    def as_database_record(self) -> dict[str, Any]:
        return {
            "deliverables": list(self.deliverables),
            "pr_repositories": list(self.pr_repositories),
            "pr_bindings": [dict(binding) for binding in self.pr_bindings],
            "digest": self.digest,
        }


def _repository_rows(datasources: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The connectors a pull request can come from: their driver checks out."""
    return [dict(row) for row in datasources if delivers_in(row, "checkout")]


def _repository_identity(datasource: Mapping[str, Any]) -> str | None:
    try:
        owner, repository = parse_owner_repo(
            str(datasource.get("connection_url") or "")
        )
    except ForgeError:
        return None
    return normalize_repository_identity(f"{owner}/{repository}")


def _writable(datasource: Mapping[str, Any]) -> bool:
    """Not bound read-only: neither its project link nor its creator's tag
    (a public connector with no mode set included) is read-only."""
    return not bound_read_only(datasource)


def _binding(datasource: Mapping[str, Any], identity: str) -> dict[str, Any]:
    config = datasource.get("config")
    if isinstance(config, str):
        try:
            config = json.loads(config)
        except (TypeError, ValueError, json.JSONDecodeError):
            config = {}
    config = config if isinstance(config, Mapping) else {}
    return {
        "repository": identity,
        "datasource_id": str(datasource.get("id") or ""),
        "forge": str(config.get("forge") or "").strip().lower(),
        "policy_revision": int(datasource.get("policy_revision") or 0),
    }


def _digest(deliverables: Sequence[str], bindings: Sequence[Mapping[str, Any]]) -> str:
    payload = {
        "deliverables": list(deliverables),
        "pr_bindings": [dict(binding) for binding in bindings],
        "version": 1,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def prepare_delivery_contract(
    requested: Any,
    *,
    datasources: Sequence[Mapping[str, Any]],
) -> DeliveryContractPlan:
    """Normalize and bind a caller contract to exact attached repositories.

    Cloned-repository file paths are intentionally refused.  The exception
    carries the exact PR repository identity the caller must use instead, so
    an Officer ticket attempt can durably record that non-downgradable
    requirement before returning the refusal.
    """

    try:
        deliverables = parse_required_deliverables(requested, strict=True)
    except DeliverableContractError as exc:
        raise DeliveryContractConflict(
            "invalid_deliverable_contract", str(exc)
        ) from exc

    repositories = _repository_rows(datasources)
    clone_names = resolve_repo_clone_names(repositories)
    alias_map: dict[str, list[tuple[dict[str, Any], str]]] = {}
    identity_map: dict[str, list[dict[str, Any]]] = {}
    for datasource, alias in zip(repositories, clone_names):
        identity = _repository_identity(datasource)
        if identity is None:
            continue
        alias_map.setdefault(alias.casefold(), []).append((datasource, identity))
        identity_map.setdefault(identity, []).append(datasource)

    # Classification consumes the same canonical manifest that is persisted.
    # Presentation-equivalent ``./repos`` and ``/repos`` spellings therefore
    # cannot bypass the generation-scoped Officer anti-downgrade receipt.
    offenders = cloned_repo_deliverables(deliverables)
    if offenders:
        required: list[str] = []
        for path in offenders:
            alias = cloned_repo_alias(path)
            matches = alias_map.get(str(alias or "").casefold(), [])
            if len(matches) != 1 or not _writable(matches[0][0]):
                raise DeliveryContractConflict(
                    "external_repository_contract_unresolvable",
                    "A cloned-repository deliverable does not identify one "
                    "writable attached repository; no job or claim was created.",
                )
            identity = matches[0][1]
            if identity not in required:
                required.append(identity)
        raise DeliveryContractConflict(
            "external_repository_requires_pr",
            "Files inside an attached repository are delivered as a pull "
            "request, not as a knowledge note or an unversioned repos/ path. "
            "Retry with the exact pr:owner/repository contract shown.",
            required_pr_deliverables=[f"pr:{identity}" for identity in required],
            required_pr_repositories=required,
        )

    declared_prs = pr_repositories(deliverables)
    if len(declared_prs) > 1:
        # ``context.pull_request`` is deliberately singular. Refuse an
        # ambiguous promise instead of proving only the first one.
        raise DeliveryContractConflict(
            "multiple_pr_deliverables_unsupported",
            "One job may declare exactly one pull-request deliverable.",
        )

    bindings: list[dict[str, Any]] = []
    for identity in declared_prs:
        matches = identity_map.get(identity, [])
        if len(matches) != 1:
            raise DeliveryContractConflict(
                "pr_deliverable_not_attached",
                "The PR deliverable must name exactly one repository attached "
                "to this job; no job or claim was created.",
                repository=identity,
            )
        datasource = matches[0]
        if not _writable(datasource):
            raise DeliveryContractConflict(
                "pr_deliverable_read_only",
                "The PR deliverable repository is attached read-only and cannot "
                "receive this job's pull request.",
                repository=identity,
            )
        bindings.append(_binding(datasource, identity))

    return DeliveryContractPlan(
        deliverables=tuple(deliverables),
        pr_repositories=tuple(declared_prs),
        pr_bindings=tuple(bindings),
        digest=_digest(deliverables, bindings),
    )


def plan_from_database_record(record: Mapping[str, Any]) -> DeliveryContractPlan:
    """Rehydrate a contract row without accepting a mutable job-context copy."""

    deliverables = parse_required_deliverables(
        list(record.get("normalized_deliverables") or []), strict=True
    )
    repositories = [
        value
        for raw in list(record.get("pr_repositories") or [])
        if (value := normalize_repository_identity(raw)) is not None
    ]
    bindings_raw = record.get("pr_bindings")
    bindings_raw = bindings_raw if isinstance(bindings_raw, list) else []
    bindings = tuple(dict(item) for item in bindings_raw if isinstance(item, Mapping))
    return DeliveryContractPlan(
        deliverables=tuple(deliverables),
        pr_repositories=tuple(repositories),
        pr_bindings=bindings,
        digest=str(record.get("contract_digest") or _digest(deliverables, bindings)),
    )


__all__ = [
    "BLOCKED_UNDELIVERED_OUTCOME",
    "DeliveryContractConflict",
    "DeliveryContractPlan",
    "plan_from_database_record",
    "prepare_delivery_contract",
]
