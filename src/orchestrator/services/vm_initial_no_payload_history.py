"""Validate that attach aborts predate issuance of a Session's first VM."""

import json
from collections.abc import Mapping, Sequence
from uuid import UUID


MAX_INITIAL_NO_PAYLOAD_ABORTS = 4096
_RECEIPT_FIELDS = frozenset(
    {
        "version",
        "runtime_generation",
        "successor_generation",
        "agent_id",
        "runtime_attach_token",
        "agent_pod_uid",
        "release_kind",
        "quiescence_protocol",
        "workspace_generation",
        "workspace_runtime_incarnation",
    }
)


def _uuid(value: object) -> UUID | None:
    try:
        parsed = UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None
    return parsed if parsed.int else None


def initial_vm_no_payload_history_allows(
    *,
    thread_id: UUID,
    current_generation: UUID,
    receipt: object,
    outcomes: Sequence[Mapping[str, object]],
) -> bool:
    """Accept only a complete, unique pre-delivery chain ending at locked G.

    The caller fetches up to MAX + 1 rows under the locked Thread. Any other
    history classes remain independent blockers in initial-create admission.
    """
    if len(outcomes) > MAX_INITIAL_NO_PAYLOAD_ABORTS:
        return False
    if not outcomes:
        return receipt is None
    if isinstance(receipt, str):
        try:
            receipt = json.loads(receipt)
        except (TypeError, ValueError):
            return False
    if not isinstance(receipt, Mapping) or set(receipt) != _RECEIPT_FIELDS:
        return False
    if type(receipt["version"]) is not int or receipt["version"] != 1:
        return False

    sources: dict[UUID, tuple[UUID, Mapping[str, object]]] = {}
    successors: set[UUID] = set()
    for row in outcomes:
        if not isinstance(row, Mapping):
            return False
        try:
            source = _uuid(row["runtime_generation"])
            successor = _uuid(row["successor_generation"])
            actor = _uuid(row["agent_id"])
            token = _uuid(row["runtime_attach_token"])
            owner = _uuid(row["thread_id"])
            pod = row["agent_pod_uid"]
            if (
                owner != thread_id
                or source is None
                or successor is None
                or actor is None
                or token is None
                or not isinstance(pod, str)
                or not pod.strip()
                or row["release_kind"] != "server_pre_delivery"
                or row["quiescence_protocol"] != "pre_delivery_no_payload_v1"
                or row["workspace_generation"] is not None
                or row["workspace_runtime_incarnation"] is not None
                or source == successor
                or source in sources
                or successor in successors
            ):
                return False
        except (KeyError, TypeError):
            return False
        sources[source] = (successor, row)
        successors.add(successor)

    roots = sources.keys() - successors
    if len(roots) != 1 or current_generation in sources:
        return False
    node = next(iter(roots))
    visited: set[UUID] = set()
    terminal: Mapping[str, object] | None = None
    while node in sources:
        if node in visited:
            return False
        visited.add(node)
        node, terminal = sources[node]
    if node != current_generation or len(visited) != len(outcomes) or terminal is None:
        return False

    expected_receipt = {
        "version": 1,
        "runtime_generation": str(terminal["runtime_generation"]),
        "successor_generation": str(terminal["successor_generation"]),
        "agent_id": str(terminal["agent_id"]),
        "runtime_attach_token": str(terminal["runtime_attach_token"]),
        "agent_pod_uid": terminal["agent_pod_uid"],
        "release_kind": terminal["release_kind"],
        "quiescence_protocol": terminal["quiescence_protocol"],
        "workspace_generation": None,
        "workspace_runtime_incarnation": None,
    }
    return receipt == expected_receipt
